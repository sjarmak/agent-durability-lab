package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestParseConfigRequiresLoopbackAndNoPositionals(t *testing.T) {
	valid, err := parseConfig([]string{"--listen", "[::1]:9090", "--repository", "/repo"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if valid.listenAddress != "[::1]:9090" || valid.repository != "/repo" {
		t.Fatalf("config = %+v", valid)
	}
	for _, arguments := range [][]string{
		{"--listen", "0.0.0.0:8080"},
		{"--listen", "localhost:8080"},
		{"--repository", ""},
		{"extra"},
		{"--unknown"},
	} {
		if _, err := parseConfig(arguments, &bytes.Buffer{}); err == nil {
			t.Fatalf("arguments %q accepted", arguments)
		}
	}
}

func TestRunServesCatalogAndShutsDown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan int, 1)
	var output, errorOutput bytes.Buffer
	repositoryRoot := testRepositoryRoot(t)
	go func() {
		result <- run(ctx, []string{"--listen", address, "--repository", repositoryRoot}, &output, &errorOutput)
	}()

	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/api/catalog")
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("catalog status = %d", response.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not start: %v; stderr=%s", requestErr, errorOutput.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case code := <-result:
		if code != 0 {
			t.Fatalf("run exit = %d; stderr=%s", code, errorOutput.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
	if !bytes.Contains(output.Bytes(), []byte("Recovery evidence explorer")) {
		t.Fatalf("stdout = %q", output.String())
	}
}

func TestRunReportsConfigurationAndRepositoryFailures(t *testing.T) {
	for _, arguments := range [][]string{
		{"--listen", "0.0.0.0:8080"},
		{"--repository", filepath.Join(t.TempDir(), "missing")},
	} {
		var output, errorOutput bytes.Buffer
		if code := run(context.Background(), arguments, &output, &errorOutput); code == 0 {
			t.Fatalf("run %q succeeded", arguments)
		}
		if errorOutput.Len() == 0 {
			t.Fatalf("run %q had no diagnostic", arguments)
		}
	}
}

func testRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate command tests")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../../../.."))
}
