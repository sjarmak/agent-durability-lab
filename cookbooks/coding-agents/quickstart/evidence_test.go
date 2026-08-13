package quickstart

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/cookbooks/coding-agents/presentation"
)

func TestCatalogArtifactsMatchSealedRepositoryEvidence(t *testing.T) {
	t.Parallel()
	catalog, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	root := repositoryRoot(t)
	seenDigests := make(map[string]string)
	seenMembers := make(map[string]map[string]bool)
	for _, artifact := range catalogArtifacts(catalog) {
		path := filepath.Join(root, filepath.FromSlash(artifact.Path))
		if prior, ok := seenDigests[path]; ok {
			if prior != artifact.SHA256 {
				t.Fatalf("artifact %s has conflicting digests", artifact.Path)
			}
		} else {
			seenDigests[path] = artifact.SHA256
			assertDigest(t, path, artifact.SHA256)
		}
		if artifact.ArchiveMember == "" {
			continue
		}
		members, ok := seenMembers[path]
		if !ok {
			members = tarMembers(t, path)
			seenMembers[path] = members
		}
		if !members[artifact.ArchiveMember] {
			t.Fatalf("archive %s lacks member %s", artifact.Path, artifact.ArchiveMember)
		}
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate quickstart test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", "..", ".."))
}

func catalogArtifacts(catalog presentation.Catalog) []presentation.ArtifactLink {
	artifacts := make([]presentation.ArtifactLink, 0, 24)
	for _, scenario := range catalog.Scenarios {
		artifacts = append(artifacts, scenario.Claim.Evidence...)
		for _, episode := range scenario.Episodes {
			artifacts = append(artifacts, episode.NativeHistory)
			artifacts = append(artifacts, episode.RawEvidence...)
			artifacts = append(artifacts, episode.Provenance.Manifest, episode.Provenance.Report)
			artifacts = append(artifacts, episode.Provenance.CorrectionLineage...)
		}
	}
	return artifacts
}

func assertDigest(t *testing.T, path, want string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatalf("hash %s: %v", path, err)
	}
	got := fmt.Sprintf("sha256:%x", hash.Sum(nil))
	if got != want {
		t.Fatalf("digest %s = %s, want %s", path, got, want)
	}
}

func tarMembers(t *testing.T, path string) map[string]bool {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open archive %s: %v", path, err)
	}
	defer file.Close()
	compressed, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("open gzip %s: %v", path, err)
	}
	defer compressed.Close()
	members := make(map[string]bool)
	reader := tar.NewReader(compressed)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read archive %s: %v", path, err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		cleaned := pathpkg.Clean(header.Name)
		if strings.HasPrefix(header.Name, "/") || strings.Contains(header.Name, "\\") ||
			cleaned != header.Name || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
			t.Fatalf("archive %s contains unconfined member %q", path, header.Name)
		}
		if members[header.Name] {
			t.Fatalf("archive %s duplicates member %q", path, header.Name)
		}
		members[header.Name] = true
	}
	return members
}
