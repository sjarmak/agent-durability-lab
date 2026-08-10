package transport

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

var archiveEpoch = time.Unix(0, 0).UTC()

func writeArchive(ctx context.Context, sourceRoot, archivePath string, manifest BundleManifest) (returnErr error) {
	file, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	gzipWriter := gzip.NewWriter(file)
	gzipWriter.ModTime = archiveEpoch
	gzipWriter.OS = 255
	tarWriter := tar.NewWriter(gzipWriter)
	closed := false
	closeWriters := func() error {
		return errors.Join(tarWriter.Close(), gzipWriter.Close(), file.Sync(), file.Close())
	}
	defer func() {
		if !closed {
			returnErr = errors.Join(returnErr, closeWriters())
		}
	}()

	for _, artifact := range manifest.Files {
		if err := checkContext(ctx); err != nil {
			return err
		}
		header := tar.Header{
			Name:     manifest.Bundle + "/" + artifact.Path,
			Mode:     int64(artifact.Mode.Perm()),
			Size:     artifact.Size,
			ModTime:  archiveEpoch,
			Typeflag: tar.TypeReg,
			Format:   tar.FormatPAX,
		}
		if err := tarWriter.WriteHeader(&header); err != nil {
			return err
		}
		artifactPath := filepath.Join(sourceRoot, filepath.FromSlash(artifact.Path))
		if err := hashStableInto(tarWriter, artifactPath, artifact); err != nil {
			return err
		}
	}
	closeErr := closeWriters()
	closed = true
	return closeErr
}

func verifyArchive(ctx context.Context, archivePath string, manifest BundleManifest) error {
	return readArchive(ctx, archivePath, manifest, func(_ string, artifact Artifact, reader io.Reader) error {
		written, err := io.Copy(io.Discard, reader)
		if err != nil {
			return err
		}
		if written != artifact.Size {
			return fmt.Errorf("%w: short archive artifact %s", ErrInvalidTransport, artifact.Path)
		}
		return nil
	})
}

func extractArchive(ctx context.Context, archivePath string, manifest BundleManifest, destination *os.Root) error {
	return readArchive(ctx, archivePath, manifest, func(name string, artifact Artifact, reader io.Reader) (returnErr error) {
		parent := path.Dir(name)
		if parent != "." {
			if err := destination.MkdirAll(parent, 0o700); err != nil {
				return err
			}
		}
		file, err := destination.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, artifact.Mode.Perm())
		if err != nil {
			return err
		}
		written, err := io.Copy(file, reader)
		if err != nil {
			return errors.Join(err, file.Close())
		}
		if written != artifact.Size {
			return errors.Join(fmt.Errorf("%w: short restored artifact %s", ErrInvalidTransport, artifact.Path), file.Close())
		}
		if err := file.Sync(); err != nil {
			return errors.Join(err, file.Close())
		}
		if err := file.Close(); err != nil {
			return err
		}
		return destination.Chmod(name, artifact.Mode.Perm())
	})
}

type archiveConsumer func(name string, artifact Artifact, reader io.Reader) error

func readArchive(ctx context.Context, archivePath string, manifest BundleManifest, consume archiveConsumer) (returnErr error) {
	if manifest.ArchiveSHA256 != "" {
		digest, err := hashRegularFile(archivePath, maxArchiveBytes)
		if err != nil {
			return err
		}
		if digest != manifest.ArchiveSHA256 {
			return fmt.Errorf("%w: archive hash for %s", ErrInvalidTransport, manifest.Bundle)
		}
	}
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	buffered := bufio.NewReader(file)
	gzipReader, err := gzip.NewReader(buffered)
	if err != nil {
		return fmt.Errorf("%w: gzip %s: %v", ErrInvalidTransport, manifest.Bundle, err)
	}
	gzipReader.Multistream(false)
	counted := &countingReader{reader: gzipReader}
	if err := consumeArchiveEntries(ctx, tar.NewReader(counted), manifest, consume); err != nil {
		return errors.Join(err, gzipReader.Close())
	}
	return finishArchiveStream(buffered, gzipReader, counted, manifest.Bundle)
}

func consumeArchiveEntries(ctx context.Context, reader *tar.Reader, manifest BundleManifest, consume archiveConsumer) error {
	index := 0
	for {
		if err := checkContext(ctx); err != nil {
			return err
		}
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("%w: tar %s: %v", ErrInvalidTransport, manifest.Bundle, err)
		}
		artifact, err := validateArchiveHeader(header, manifest, index)
		if err != nil {
			return err
		}
		if err := consumeArchiveArtifact(reader, header.Name, artifact, consume); err != nil {
			return err
		}
		index++
	}
	if index != len(manifest.Files) {
		return fmt.Errorf("%w: archive file count for %s", ErrInvalidTransport, manifest.Bundle)
	}
	return nil
}

func validateArchiveHeader(header *tar.Header, manifest BundleManifest, index int) (Artifact, error) {
	if header.Typeflag != tar.TypeReg {
		return Artifact{}, fmt.Errorf("%w: archive entry %q is not regular", ErrInvalidTransport, header.Name)
	}
	relative, err := archiveRelativePath(header.Name, manifest.Bundle)
	if err != nil {
		return Artifact{}, err
	}
	if index >= len(manifest.Files) {
		return Artifact{}, fmt.Errorf("%w: unexpected archive entry %q", ErrInvalidTransport, header.Name)
	}
	artifact := manifest.Files[index]
	if relative != artifact.Path || header.Size != artifact.Size || fs.FileMode(header.Mode).Perm() != artifact.Mode.Perm() ||
		header.Uid != 0 || header.Gid != 0 || header.Uname != "" || header.Gname != "" || !header.ModTime.Equal(archiveEpoch) {
		return Artifact{}, fmt.Errorf("%w: archive metadata for %q", ErrInvalidTransport, header.Name)
	}
	return artifact, nil
}

func consumeArchiveArtifact(reader *tar.Reader, name string, artifact Artifact, consume archiveConsumer) error {
	limited := &io.LimitedReader{R: reader, N: artifact.Size}
	hasher := sha256.New()
	if err := consume(name, artifact, io.TeeReader(limited, hasher)); err != nil {
		return err
	}
	if limited.N != 0 || hex.EncodeToString(hasher.Sum(nil)) != artifact.SHA256 {
		return fmt.Errorf("%w: archive content for %q", ErrInvalidTransport, name)
	}
	return nil
}

func finishArchiveStream(buffered *bufio.Reader, gzipReader *gzip.Reader, counted *countingReader, bundle string) error {
	tarEndBytes := counted.bytesRead
	_, copyErr := io.Copy(io.Discard, counted)
	closeErr := gzipReader.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return fmt.Errorf("%w: finish gzip %s: %v", ErrInvalidTransport, bundle, err)
	}
	if counted.bytesRead != tarEndBytes {
		return fmt.Errorf("%w: trailing payload after tar end for %s", ErrInvalidTransport, bundle)
	}
	if _, err := buffered.Peek(1); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing archive stream for %s", ErrInvalidTransport, bundle)
	}
	return nil
}

type countingReader struct {
	reader    io.Reader
	bytesRead int64
}

func (r *countingReader) Read(data []byte) (int, error) {
	count, err := r.reader.Read(data)
	r.bytesRead += int64(count)
	return count, err
}

func archiveRelativePath(name, bundle string) (string, error) {
	if name == "" || name[0] == '/' || path.Clean(name) != name {
		return "", fmt.Errorf("%w: unsafe archive path %q", ErrInvalidTransport, name)
	}
	prefix := bundle + "/"
	if !safeBundleName(bundle) || len(name) <= len(prefix) || name[:len(prefix)] != prefix {
		return "", fmt.Errorf("%w: archive path outside bundle %q", ErrInvalidTransport, name)
	}
	relative := name[len(prefix):]
	if !safeRelativePath(relative) {
		return "", fmt.Errorf("%w: unsafe archive path %q", ErrInvalidTransport, name)
	}
	return relative, nil
}

func safeRelativePath(name string) bool {
	return name != "" && name != "." && name != ".." && name[0] != '/' &&
		!strings.HasPrefix(name, "../") && !strings.Contains(name, `\`) && path.Clean(name) == name && !path.IsAbs(name)
}
