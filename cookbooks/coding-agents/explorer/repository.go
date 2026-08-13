package explorer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/sjarmak/temporal_projects/cookbooks/coding-agents/presentation"
	"github.com/sjarmak/temporal_projects/cookbooks/coding-agents/quickstart"
	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/transport"
)

const (
	maxArchiveBytes        = 16 << 20
	maxEvidenceBundleBytes = 64 << 20
	maxMemberBytes         = 8 << 20
	maxMetadataBytes       = 16 << 20
)

var ErrArtifactNotFound = errors.New("explorer artifact not found")

type Artifact struct {
	Data        []byte
	ContentType string
	Filename    string
}

type Repository struct {
	mu       sync.RWMutex
	root     *os.Root
	rootPath string
	catalog  presentation.Catalog
	episodes map[string]presentation.Episode
	closed   bool
}

func OpenRepository(repositoryRoot string) (*Repository, error) {
	absolute, err := filepath.Abs(repositoryRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve repository root: %w", err)
	}
	absolute = filepath.Clean(absolute)
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil || resolved != absolute {
		return nil, errors.New("repository root must be a real directory without symlink components")
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("repository root must be a real directory")
	}
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, fmt.Errorf("open repository root: %w", err)
	}
	catalog, err := quickstart.LoadCatalog()
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("load explorer catalog: %w", err)
	}
	episodes := make(map[string]presentation.Episode)
	for _, scenario := range catalog.Scenarios {
		for _, episode := range scenario.Episodes {
			episodes[episode.ID] = episode
		}
	}
	return &Repository{
		root: root, rootPath: absolute, catalog: catalog, episodes: episodes,
	}, nil
}

func (repository *Repository) Close() error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.closed {
		return nil
	}
	repository.closed = true
	return repository.root.Close()
}

func (repository *Repository) Catalog() (presentation.Catalog, error) {
	repository.mu.RLock()
	defer repository.mu.RUnlock()
	if repository.closed {
		return presentation.Catalog{}, errors.New("explorer repository is closed")
	}
	data, err := json.Marshal(repository.catalog)
	if err != nil {
		return presentation.Catalog{}, fmt.Errorf("copy catalog: %w", err)
	}
	return presentation.DecodeJSON(data)
}

func (repository *Repository) ReadEpisodeArtifact(
	ctx context.Context,
	episodeID string,
	selector string,
) (Artifact, error) {
	repository.mu.RLock()
	defer repository.mu.RUnlock()
	if repository.closed {
		return Artifact{}, errors.New("explorer repository is closed")
	}
	if err := ctx.Err(); err != nil {
		return Artifact{}, err
	}
	episode, exists := repository.episodes[episodeID]
	if !exists {
		return Artifact{}, ErrArtifactNotFound
	}
	link, err := selectEpisodeArtifact(episode, selector)
	if err != nil {
		return Artifact{}, err
	}
	if link.ArchiveMember == "" {
		data, err := repository.readVerifiedRegular(link, maxMetadataBytes)
		if err != nil {
			return Artifact{}, err
		}
		return Artifact{
			Data: data, ContentType: link.MediaType, Filename: path.Base(link.Path),
		}, nil
	}
	return repository.readVerifiedArchiveMember(ctx, episode, link)
}

func selectEpisodeArtifact(episode presentation.Episode, selector string) (presentation.ArtifactLink, error) {
	switch selector {
	case "history":
		return episode.NativeHistory, nil
	case "manifest":
		return episode.Provenance.Manifest, nil
	case "report":
		return episode.Provenance.Report, nil
	}
	if strings.HasPrefix(selector, "raw-") {
		return indexedArtifact(selector, "raw-", episode.RawEvidence)
	}
	if strings.HasPrefix(selector, "lineage-") {
		return indexedArtifact(selector, "lineage-", episode.Provenance.CorrectionLineage)
	}
	return presentation.ArtifactLink{}, ErrArtifactNotFound
}

func indexedArtifact(
	selector string,
	prefix string,
	links []presentation.ArtifactLink,
) (presentation.ArtifactLink, error) {
	index, err := strconv.Atoi(strings.TrimPrefix(selector, prefix))
	if err != nil || index < 0 || index >= len(links) || selector != prefix+strconv.Itoa(index) {
		return presentation.ArtifactLink{}, ErrArtifactNotFound
	}
	return links[index], nil
}

func (repository *Repository) readVerifiedArchiveMember(
	ctx context.Context,
	episode presentation.Episode,
	link presentation.ArtifactLink,
) (Artifact, error) {
	transportRoot := episode.Provenance.EvidenceRoot
	if !pathWithin(transportRoot, link.Path) || !pathWithin(transportRoot, episode.Provenance.Manifest.Path) {
		return Artifact{}, errors.New("catalog artifact is outside its evidence root")
	}
	if _, err := transport.Verify(ctx, filepath.Join(repository.rootPath, filepath.FromSlash(transportRoot))); err != nil {
		return Artifact{}, fmt.Errorf("verify evidence transport: %w", err)
	}
	archiveData, err := repository.readVerifiedRegular(link, maxArchiveBytes)
	if err != nil {
		return Artifact{}, err
	}
	manifestData, err := repository.readVerifiedRegular(episode.Provenance.Manifest, maxMetadataBytes)
	if err != nil {
		return Artifact{}, err
	}
	manifest, err := decodeManifest(manifestData)
	if err != nil {
		return Artifact{}, err
	}
	if manifest.Archive != path.Base(link.Path) || manifest.TotalBytes > maxEvidenceBundleBytes {
		return Artifact{}, errors.New("transport manifest exceeds the explorer boundary")
	}
	var expected transport.Artifact
	found := false
	for _, candidate := range manifest.Files {
		if candidate.Path == link.ArchiveMember {
			expected = candidate
			found = true
			break
		}
	}
	if !found || expected.Size < 0 || expected.Size > maxMemberBytes {
		return Artifact{}, errors.New("catalog archive member is absent or oversized")
	}
	data, err := readArchiveMember(archiveData, expected)
	if err != nil {
		return Artifact{}, err
	}
	return Artifact{
		Data: data, ContentType: "application/json", Filename: path.Base(link.ArchiveMember),
	}, nil
}

func (repository *Repository) readVerifiedRegular(
	link presentation.ArtifactLink,
	maximum int64,
) ([]byte, error) {
	data, err := readRegularAt(repository.root, link.Path, maximum)
	if err != nil {
		return nil, err
	}
	if digest(data) != link.SHA256 {
		return nil, errors.New("catalog artifact digest differs")
	}
	return data, nil
}

func readRegularAt(root *os.Root, name string, maximum int64) ([]byte, error) {
	if !confinedPath(name) {
		return nil, errors.New("artifact path is not confined")
	}
	components := strings.Split(name, "/")
	current := ""
	var before fs.FileInfo
	for index, component := range components {
		current = path.Join(current, component)
		info, err := root.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("artifact path contains a missing or symlinked component")
		}
		if index < len(components)-1 && !info.IsDir() {
			return nil, errors.New("artifact parent is not a directory")
		}
		before = info
	}
	if before == nil || !before.Mode().IsRegular() || before.Size() < 0 || before.Size() > maximum {
		return nil, errors.New("artifact is not a bounded regular file")
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() != before.Size() {
		return nil, errors.New("artifact changed before read")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != before.Size() || int64(len(data)) > maximum {
		return nil, errors.New("artifact changed or exceeded its bound")
	}
	return data, nil
}

func decodeManifest(data []byte) (transport.BundleManifest, error) {
	if err := inspectManifestJSON(data); err != nil {
		return transport.BundleManifest{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest transport.BundleManifest
	if err := decoder.Decode(&manifest); err != nil {
		return transport.BundleManifest{}, fmt.Errorf("decode transport manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return transport.BundleManifest{}, errors.New("transport manifest has trailing JSON")
	}
	return manifest, nil
}

func inspectManifestJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanManifestJSONValue(decoder, 1); err != nil {
		return fmt.Errorf("inspect transport manifest: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("transport manifest has trailing JSON")
	}
	return nil
}

func scanManifestJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("transport manifest exceeds the JSON nesting bound")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	if delimiter == '[' {
		for items := 0; decoder.More(); items++ {
			if items >= 10_000 {
				return errors.New("transport manifest array exceeds the item bound")
			}
			if err := scanManifestJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		_, err := decoder.Token()
		return err
	}
	if delimiter != '{' {
		return errors.New("transport manifest has an unexpected JSON delimiter")
	}
	seen := make(map[string]struct{})
	for fields := 0; decoder.More(); fields++ {
		if fields >= 10_000 {
			return errors.New("transport manifest object exceeds the field bound")
		}
		keyToken, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok {
			return errors.New("transport manifest object key is not a string")
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("transport manifest has duplicate object key %q", key)
		}
		seen[key] = struct{}{}
		if err := scanManifestJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}

func readArchiveMember(data []byte, expected transport.Artifact) ([]byte, error) {
	compressed, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("open evidence archive")
	}
	defer compressed.Close()
	compressed.Multistream(false)
	reader := tar.NewReader(compressed)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil, ErrArtifactNotFound
		}
		if err != nil {
			return nil, errors.New("read evidence archive")
		}
		if header.Name != expected.Path {
			continue
		}
		if header.Typeflag != tar.TypeReg || header.Size != expected.Size ||
			fs.FileMode(header.Mode).Perm() != expected.Mode.Perm() || !confinedPath(header.Name) {
			return nil, errors.New("archive member metadata differs from manifest")
		}
		member, err := io.ReadAll(io.LimitReader(reader, expected.Size+1))
		if err != nil || int64(len(member)) != expected.Size || digest(member) != "sha256:"+expected.SHA256 {
			return nil, errors.New("archive member content differs from manifest")
		}
		return member, nil
	}
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func confinedPath(value string) bool {
	if value == "" || strings.ContainsAny(value, "\\\x00:?#%") || strings.HasPrefix(value, "/") {
		return false
	}
	cleaned := path.Clean(value)
	return cleaned == value && cleaned != "." && cleaned != ".." && !strings.HasPrefix(cleaned, "../")
}

func pathWithin(root, candidate string) bool {
	return candidate == root || strings.HasPrefix(candidate, root+"/")
}
