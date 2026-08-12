package failureinject

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const (
	CredentialFDEnvironment = "TEMPORAL_FAILURE_INJECT_CREDENTIAL_FD"
	authorizationHeader     = "X-Failure-Inject-Authorization"
	nonceHeader             = "X-Failure-Inject-Nonce"
	credentialSize          = 32
	nonceSize               = 24
)

var ErrUnauthorizedBarrier = errors.New("unauthorized barrier request")

// Credential is an opaque, per-run bearer credential. It is intentionally not
// serializable so it cannot enter a portable evidence document by accident.
type Credential struct {
	value []byte
}

func NewCredential() (Credential, error) {
	value := make([]byte, credentialSize)
	if _, err := rand.Read(value); err != nil {
		return Credential{}, fmt.Errorf("generate barrier credential: %w", err)
	}
	return Credential{value: value}, nil
}

func readCredential(reader io.Reader) (Credential, error) {
	if reader == nil {
		return Credential{}, fmt.Errorf("%w: credential reader is required", ErrInvalidBarrier)
	}
	value := make([]byte, credentialSize)
	if _, err := io.ReadFull(reader, value); err != nil {
		return Credential{}, fmt.Errorf("%w: read credential: %v", ErrInvalidBarrier, err)
	}
	var trailing [1]byte
	if count, err := reader.Read(trailing[:]); err != io.EOF || count != 0 {
		return Credential{}, fmt.Errorf("%w: credential has trailing data", ErrInvalidBarrier)
	}
	return Credential{value: value}, nil
}

func (c Credential) Write(writer io.Writer) error {
	if !c.valid() || writer == nil {
		return fmt.Errorf("%w: credential and writer are required", ErrInvalidBarrier)
	}
	if _, err := writer.Write(c.value); err != nil {
		return fmt.Errorf("write barrier credential: %w", err)
	}
	return nil
}

func (c Credential) MarshalJSON() ([]byte, error) {
	return nil, errors.New("barrier credentials cannot be serialized")
}

func (c Credential) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("[REDACTED]"))
}

func (c Credential) valid() bool {
	return len(c.value) == credentialSize
}

func (c Credential) IsSet() bool {
	return c.valid()
}

func ReadCredentialFromEnvironment() (Credential, error) {
	value := os.Getenv(CredentialFDEnvironment)
	if value == "" {
		return Credential{}, nil
	}
	fd, err := strconv.Atoi(value)
	if err != nil || fd < 3 {
		return Credential{}, fmt.Errorf("%w: credential file descriptor", ErrInvalidBarrier)
	}
	file := os.NewFile(uintptr(fd), "failure-injection-credential")
	if file == nil {
		return Credential{}, fmt.Errorf("%w: open credential file descriptor", ErrInvalidBarrier)
	}
	credential, readErr := readCredential(file)
	return credential, errors.Join(readErr, file.Close())
}

type Expectation struct {
	Point      string
	SessionID  string
	Generation uint64
	ActorID    string
}

func (e Expectation) valid() bool {
	return e.Point != "" && e.SessionID != "" && e.Generation > 0 && e.ActorID != ""
}

type authentication struct {
	credential     []byte
	expected       map[Expectation]struct{}
	usedNonces     map[string]struct{}
	usedArrivalIDs map[string]struct{}
}

func NewAuthenticatedCoordinator(credential Credential, expected ...Expectation) (*Coordinator, error) {
	if !credential.valid() || len(expected) == 0 {
		return nil, fmt.Errorf("%w: credential and expectations are required", ErrInvalidBarrier)
	}
	auth := &authentication{
		credential: append([]byte(nil), credential.value...),
		expected:   make(map[Expectation]struct{}, len(expected)),
		usedNonces: make(map[string]struct{}), usedArrivalIDs: make(map[string]struct{}),
	}
	for _, expectation := range expected {
		if !expectation.valid() {
			return nil, fmt.Errorf("%w: complete point, session, generation, and actor expectation required", ErrInvalidBarrier)
		}
		if _, duplicate := auth.expected[expectation]; duplicate {
			return nil, fmt.Errorf("%w: duplicate barrier expectation", ErrInvalidBarrier)
		}
		auth.expected[expectation] = struct{}{}
	}
	return &Coordinator{points: make(map[string]*pointState), authentication: auth}, nil
}

func NewAuthenticatedClient(baseURL string, credential Credential) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"), http: http.DefaultClient,
		credential: credential,
	}
}

func authenticatedRequest(ctx context.Context, baseURL string, credential Credential, arrival Arrival) (*http.Request, error) {
	if !credential.valid() {
		return nil, fmt.Errorf("%w: credential is required", ErrInvalidBarrier)
	}
	body, err := json.Marshal(arrival)
	if err != nil {
		return nil, fmt.Errorf("encode barrier arrival: %w", err)
	}
	nonceBytes := make([]byte, nonceSize)
	if _, err := rand.Read(nonceBytes); err != nil {
		return nil, fmt.Errorf("generate barrier nonce: %w", err)
	}
	nonce := base64.RawURLEncoding.EncodeToString(nonceBytes)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(baseURL, "/")+"/v1/arrivals", strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("create barrier request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(nonceHeader, nonce)
	request.Header.Set(authorizationHeader,
		hex.EncodeToString(authenticationCode(credential.value, request.Method, request.URL.Path, nonce, body)))
	return request, nil
}

func authenticationCode(credential []byte, method, path, nonce string, body []byte) []byte {
	mac := hmac.New(sha256.New, credential)
	_, _ = mac.Write([]byte(method))
	_, _ = mac.Write([]byte{'\n'})
	_, _ = mac.Write([]byte(path))
	_, _ = mac.Write([]byte{'\n'})
	_, _ = mac.Write([]byte(nonce))
	_, _ = mac.Write([]byte{'\n'})
	_, _ = mac.Write(body)
	return mac.Sum(nil)
}

func (a *authentication) verify(request *http.Request, body []byte) (string, error) {
	nonces := request.Header.Values(nonceHeader)
	authorizations := request.Header.Values(authorizationHeader)
	if len(nonces) != 1 || len(authorizations) != 1 {
		return "", ErrUnauthorizedBarrier
	}
	nonce := nonces[0]
	decodedNonce, nonceErr := base64.RawURLEncoding.DecodeString(nonce)
	provided, authErr := hex.DecodeString(authorizations[0])
	expected := authenticationCode(a.credential, request.Method, request.URL.Path, nonce, body)
	if nonceErr != nil || len(decodedNonce) != nonceSize || base64.RawURLEncoding.EncodeToString(decodedNonce) != nonce ||
		authErr != nil || len(provided) != sha256.Size || !hmac.Equal(provided, expected) {
		return "", ErrUnauthorizedBarrier
	}
	return nonce, nil
}
