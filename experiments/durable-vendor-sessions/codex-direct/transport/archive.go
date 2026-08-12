package transport

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"
)

var archiveEpoch = time.Unix(0, 0).UTC()

func writeArchive(ctx context.Context, source, destination string, files []Artifact) (returnErr error) {
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	compressed, err := gzip.NewWriterLevel(file, gzip.BestSpeed)
	if err != nil {
		return err
	}
	compressed.Header.ModTime = archiveEpoch
	compressed.Header.OS = 255
	archive := tar.NewWriter(compressed)
	for _, artifact := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		header := &tar.Header{
			Name: artifact.Path, Mode: int64(artifact.Mode.Perm()), Size: artifact.Size,
			ModTime: archiveEpoch, AccessTime: archiveEpoch, ChangeTime: archiveEpoch,
			Typeflag: tar.TypeReg, Format: tar.FormatPAX,
		}
		if err := archive.WriteHeader(header); err != nil {
			return err
		}
		if err := copyStableArtifact(archive, filepath.Join(source, filepath.FromSlash(artifact.Path)), artifact); err != nil {
			return err
		}
	}
	if err := archive.Close(); err != nil {
		return err
	}
	if err := compressed.Close(); err != nil {
		return err
	}
	return file.Sync()
}

func copyStableArtifact(destination io.Writer, path string, want Artifact) error {
	before, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !before.Mode().IsRegular() || before.Size() != want.Size || before.Mode().Perm() != want.Mode.Perm() {
		return fmt.Errorf("%w: source changed before archive: %s", ErrInvalidTransport, path)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	hasher := newHashingWriter(destination)
	written, copyErr := io.Copy(hasher, io.LimitReader(file, want.Size+1))
	after, statErr := file.Stat()
	closeErr := file.Close()
	if copyErr != nil || statErr != nil || closeErr != nil {
		return errors.Join(copyErr, statErr, closeErr)
	}
	if written != want.Size || !os.SameFile(before, after) || after.Size() != want.Size ||
		after.Mode().Perm() != want.Mode.Perm() || !after.ModTime().Equal(before.ModTime()) || hasher.Sum() != want.SHA256 {
		return fmt.Errorf("%w: source changed during archive: %s", ErrInvalidTransport, path)
	}
	return nil
}

func verifyArchive(ctx context.Context, archivePath string, manifest BundleManifest) error {
	data, err := readArchiveBytes(archivePath)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(data)
	compressed, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("%w: open gzip: %v", ErrInvalidTransport, err)
	}
	compressed.Multistream(false)
	archive := tar.NewReader(compressed)
	for _, want := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		header, err := archive.Next()
		if err != nil {
			return fmt.Errorf("%w: archive entry for %s: %v", ErrInvalidTransport, want.Path, err)
		}
		if header.Name != want.Path || header.Typeflag != tar.TypeReg || header.Size != want.Size ||
			os.FileMode(header.Mode).Perm() != want.Mode.Perm() || !safeRelativePath(header.Name) {
			return fmt.Errorf("%w: archive header differs for %s", ErrInvalidTransport, want.Path)
		}
		hasher := newHashingWriter(io.Discard)
		written, err := io.Copy(hasher, archive)
		if err != nil || written != want.Size || hasher.Sum() != want.SHA256 {
			return fmt.Errorf("%w: archive content differs for %s", ErrInvalidTransport, want.Path)
		}
	}
	if header, err := archive.Next(); err != io.EOF || header != nil {
		return fmt.Errorf("%w: archive contains undeclared entries", ErrInvalidTransport)
	}
	buffer := make([]byte, 1)
	if count, err := compressed.Read(buffer); count != 0 || err != io.EOF {
		return fmt.Errorf("%w: archive contains decompressed data after tar boundary", ErrInvalidTransport)
	}
	if err := compressed.Close(); err != nil || reader.Len() != 0 {
		return fmt.Errorf("%w: archive has an invalid or trailing gzip stream", ErrInvalidTransport)
	}
	return nil
}

func extractArchive(ctx context.Context, archivePath, destination string, manifest BundleManifest) error {
	data, err := readArchiveBytes(archivePath)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != manifest.ArchiveSHA256 {
		return fmt.Errorf("%w: archive changed before extraction", ErrInvalidTransport)
	}
	reader := bytes.NewReader(data)
	compressed, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	defer compressed.Close()
	compressed.Multistream(false)
	archive := tar.NewReader(compressed)
	for _, want := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		header, err := archive.Next()
		if err != nil || header.Name != want.Path || !safeRelativePath(header.Name) ||
			header.Typeflag != tar.TypeReg || header.Size != want.Size ||
			os.FileMode(header.Mode).Perm() != want.Mode.Perm() {
			return fmt.Errorf("%w: archive extraction entry differs", ErrInvalidTransport)
		}
		target := filepath.Join(destination, filepath.FromSlash(header.Name))
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return err
		}
		file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, want.Mode.Perm())
		if err != nil {
			return err
		}
		hasher := newHashingWriter(file)
		written, copyErr := io.Copy(hasher, archive)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil || written != want.Size || hasher.Sum() != want.SHA256 {
			return fmt.Errorf("%w: extracted content differs for %s: %v", ErrInvalidTransport, want.Path, errors.Join(copyErr, closeErr))
		}
	}
	if header, err := archive.Next(); err != io.EOF || header != nil {
		return fmt.Errorf("%w: archive contains undeclared extraction entries", ErrInvalidTransport)
	}
	buffer := make([]byte, 1)
	if count, err := compressed.Read(buffer); count != 0 || err != io.EOF {
		return fmt.Errorf("%w: archive contains decompressed data after extraction boundary", ErrInvalidTransport)
	}
	if err := compressed.Close(); err != nil || reader.Len() != 0 {
		return fmt.Errorf("%w: archive has an invalid or trailing extraction stream", ErrInvalidTransport)
	}
	return nil
}

func readArchiveBytes(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxArchiveBytes {
		return nil, fmt.Errorf("%w: archive is not a bounded regular file", ErrInvalidTransport)
	}
	return os.ReadFile(path)
}

type hashingWriter struct {
	destination io.Writer
	hash        hash.Hash
}

func newHashingWriter(destination io.Writer) *hashingWriter {
	return &hashingWriter{destination: destination, hash: sha256.New()}
}

func (writer *hashingWriter) Write(data []byte) (int, error) {
	count, err := writer.destination.Write(data)
	if count > 0 {
		_, _ = writer.hash.Write(data[:count])
	}
	return count, err
}

func (writer *hashingWriter) Sum() string {
	return hex.EncodeToString(writer.hash.Sum(nil))
}
