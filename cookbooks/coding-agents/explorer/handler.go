package explorer

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path"
	"strconv"
	"strings"
)

const contentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"

func NewHandler(repository *Repository) (http.Handler, error) {
	if repository == nil {
		return nil, errors.New("explorer repository is required")
	}
	if _, err := repository.Catalog(); err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		setSecurityHeaders(writer.Header())
		if request.URL.RawQuery != "" {
			http.NotFound(writer, request)
			return
		}
		switch request.URL.Path {
		case "/":
			serveStatic(writer, request, "text/html; charset=utf-8", indexHTML)
		case "/assets/styles.css":
			serveStatic(writer, request, "text/css; charset=utf-8", stylesCSS)
		case "/assets/app.js":
			serveStatic(writer, request, "text/javascript; charset=utf-8", appJS)
		case "/api/catalog":
			serveCatalog(writer, request, repository)
		default:
			serveArtifact(writer, request, repository)
		}
	}), nil
}

func serveStatic(writer http.ResponseWriter, request *http.Request, contentType string, data []byte) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Content-Type", contentType)
	writer.Header().Set("Content-Length", strconv.Itoa(len(data)))
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(data)
}

func serveCatalog(writer http.ResponseWriter, request *http.Request, repository *Repository) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	catalog, err := repository.Catalog()
	if err != nil {
		http.Error(writer, "catalog unavailable", http.StatusServiceUnavailable)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(true)
	if err := encoder.Encode(catalog); err != nil {
		return
	}
}

func serveArtifact(writer http.ResponseWriter, request *http.Request, repository *Repository) {
	if request.Method != http.MethodGet {
		if strings.HasPrefix(request.URL.Path, "/api/episodes/") {
			writer.Header().Set("Allow", http.MethodGet)
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		http.NotFound(writer, request)
		return
	}
	parts := strings.Split(strings.TrimPrefix(request.URL.Path, "/"), "/")
	if len(parts) != 5 || parts[0] != "api" || parts[1] != "episodes" ||
		parts[3] != "artifacts" || !opaqueToken(parts[2]) || !opaqueToken(parts[4]) {
		http.NotFound(writer, request)
		return
	}
	artifact, err := repository.ReadEpisodeArtifact(request.Context(), parts[2], parts[4])
	if errors.Is(err, ErrArtifactNotFound) {
		http.NotFound(writer, request)
		return
	}
	if err != nil {
		http.Error(writer, "verified evidence unavailable", http.StatusConflict)
		return
	}
	writer.Header().Set("Content-Type", artifact.ContentType+"; charset=utf-8")
	writer.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", path.Base(artifact.Filename)))
	writer.Header().Set("Content-Length", strconv.Itoa(len(artifact.Data)))
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(artifact.Data)
}

func setSecurityHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set("Content-Security-Policy", contentSecurityPolicy)
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("X-Frame-Options", "DENY")
}

func opaqueToken(value string) bool {
	if value == "" || len(value) > 160 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func ValidateListenAddress(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return errors.New("listen address must contain a literal loopback IP and port")
	}
	ip := net.ParseIP(host)
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 || ip == nil || !ip.IsLoopback() {
		return errors.New("explorer may listen only on a literal loopback IP and nonzero port")
	}
	return nil
}
