package failureinject

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Client struct {
	baseURL    string
	http       *http.Client
	credential Credential
}

func NewClient(baseURL string) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: http.DefaultClient}
}

func NewClientWithHTTP(baseURL string, httpClient *http.Client) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: httpClient}
}

func (c *Client) Arrive(ctx context.Context, arrival Arrival) error {
	if c.baseURL == "" || c.http == nil {
		return fmt.Errorf("%w: barrier URL and HTTP client are required", ErrInvalidBarrier)
	}
	if err := validateArrival(arrival); err != nil {
		return err
	}
	request, err := c.request(ctx, arrival)
	if err != nil {
		return err
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("send barrier arrival: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusNoContent {
		message, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		if readErr != nil {
			return fmt.Errorf("barrier response status %d; read error: %w", response.StatusCode, readErr)
		}
		return fmt.Errorf("barrier response status %d: %s", response.StatusCode, strings.TrimSpace(string(message)))
	}
	return nil
}

func (c *Client) request(ctx context.Context, arrival Arrival) (*http.Request, error) {
	if c.credential.valid() {
		return authenticatedRequest(ctx, c.baseURL, c.credential, arrival)
	}
	body, err := json.Marshal(arrival)
	if err != nil {
		return nil, fmt.Errorf("encode barrier arrival: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/arrivals", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create barrier request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	return request, nil
}
