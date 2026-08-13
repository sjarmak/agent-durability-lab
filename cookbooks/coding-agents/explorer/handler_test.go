package explorer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/cookbooks/coding-agents/presentation"
)

func TestHandlerServesCatalogUIAndOpaqueEvidenceRoutes(t *testing.T) {
	repository, err := OpenRepository(repositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	handler, err := NewHandler(repository)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		path        string
		contentType string
		contains    string
	}{
		{"/", "text/html", "Recovery evidence explorer"},
		{"/assets/styles.css", "text/css", "--surface-paper"},
		{"/assets/app.js", "text/javascript", "textContent"},
		{"/api/catalog", "application/json", presentation.SchemaVersion},
		{"/api/episodes/" + unsafeEpisodeID + "/artifacts/history", "application/json", "events"},
		{"/api/episodes/" + unsafeEpisodeID + "/artifacts/raw-0", "application/json", "logical_session_id"},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
			if !strings.HasPrefix(response.Header().Get("Content-Type"), test.contentType) {
				t.Fatalf("content type = %q", response.Header().Get("Content-Type"))
			}
			if !strings.Contains(response.Body.String(), test.contains) {
				t.Fatalf("body does not contain %q", test.contains)
			}
			assertSecurityHeaders(t, response)
		})
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/catalog", nil))
	var catalog presentation.Catalog
	if err := json.Unmarshal(response.Body.Bytes(), &catalog); err != nil {
		t.Fatal(err)
	}
	if err := presentation.Validate(catalog); err != nil {
		t.Fatal(err)
	}
}

func TestHandlerFailsClosedOnMethodsPathsAndOversizedTargets(t *testing.T) {
	repository, err := OpenRepository(repositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	handler, err := NewHandler(repository)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		method string
		path   string
		status int
	}{
		{http.MethodPost, "/", http.StatusMethodNotAllowed},
		{http.MethodPost, "/api/catalog", http.StatusMethodNotAllowed},
		{http.MethodPost, "/api/episodes/" + unsafeEpisodeID + "/artifacts/history", http.StatusMethodNotAllowed},
		{http.MethodPost, "/missing", http.StatusNotFound},
		{http.MethodGet, "/api/catalog?attacker=true", http.StatusNotFound},
		{http.MethodGet, "/api/episodes/missing/artifacts/history", http.StatusNotFound},
		{http.MethodGet, "/api/episodes/" + unsafeEpisodeID + "/artifacts/../../effect-request", http.StatusNotFound},
		{http.MethodGet, "/api/episodes/" + unsafeEpisodeID + "/artifacts/raw-99", http.StatusNotFound},
		{http.MethodGet, "/missing", http.StatusNotFound},
	}
	for _, test := range tests {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
		if response.Code != test.status {
			t.Fatalf("%s %s = %d, want %d", test.method, test.path, response.Code, test.status)
		}
		assertSecurityHeaders(t, response)
	}
}

func TestHandlerRequiresAnOpenRepository(t *testing.T) {
	if _, err := NewHandler(nil); err == nil {
		t.Fatal("nil repository was accepted")
	}
	repository, err := OpenRepository(repositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewHandler(repository); err == nil {
		t.Fatal("closed repository was accepted")
	}
}

func TestListenAddressMustBeLoopback(t *testing.T) {
	for _, address := range []string{"127.0.0.1:8080", "[::1]:8080"} {
		if err := ValidateListenAddress(address); err != nil {
			t.Fatalf("%s: %v", address, err)
		}
	}
	for _, address := range []string{"", ":8080", "0.0.0.0:8080", "localhost:8080", "10.0.0.2:8080", "127.0.0.1:0"} {
		if err := ValidateListenAddress(address); err == nil {
			t.Fatalf("unsafe address %q accepted", address)
		}
	}
}

func assertSecurityHeaders(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	for header, expected := range map[string]string{
		"Cache-Control":           "no-store",
		"X-Content-Type-Options":  "nosniff",
		"Referrer-Policy":         "no-referrer",
		"Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
	} {
		if actual := response.Header().Get(header); actual != expected {
			t.Fatalf("%s = %q, want %q", header, actual, expected)
		}
	}
}
