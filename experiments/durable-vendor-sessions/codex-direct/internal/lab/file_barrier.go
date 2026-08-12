package lab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"golang.org/x/sys/unix"
)

const fileBarrierRelease = "released"

type fileBarrier struct {
	directory string
}

func newFileBarrier(directory string) (*fileBarrier, error) {
	if directory == "" {
		return nil, errors.New("file barrier directory is required")
	}
	if err := os.Mkdir(directory, 0o750); err != nil {
		return nil, fmt.Errorf("create file barrier: %w", err)
	}
	return &fileBarrier{directory: directory}, nil
}

func (b *fileBarrier) WaitForArrivals(ctx context.Context, count int) ([]failureinject.Arrival, error) {
	if b == nil || b.directory == "" || count < 1 {
		return nil, errors.New("file barrier and positive arrival count are required")
	}
	for {
		arrivals, err := readFileBarrierArrivals(b.directory)
		if err != nil {
			return nil, err
		}
		if len(arrivals) >= count {
			return arrivals, nil
		}
		if err := waitForFileBarrierChange(ctx, b.directory, func() (bool, error) {
			current, err := readFileBarrierArrivals(b.directory)
			return len(current) >= count, err
		}); err != nil {
			return nil, err
		}
	}
}

func (b *fileBarrier) Release() error {
	if b == nil || b.directory == "" {
		return errors.New("file barrier is required")
	}
	path := filepath.Join(b.directory, fileBarrierRelease)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create file barrier release: %w", err)
	}
	if _, err := file.WriteString("released\n"); err != nil {
		_ = file.Close()
		return fmt.Errorf("write file barrier release: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync file barrier release: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close file barrier release: %w", err)
	}
	return syncDirectory(b.directory)
}

func arriveFileBarrier(ctx context.Context, directory string, arrival failureinject.Arrival) error {
	if directory == "" || arrival.ID == "" || arrival.Point == "" ||
		arrival.SessionID == "" || arrival.ActorID == "" {
		return errors.New("file barrier arrival requires directory and complete identity")
	}
	arrival.Time = time.Now().UTC()
	path := fileBarrierArrivalPath(directory, arrival.ID)
	if err := writeFileBarrierArrival(path, arrival); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := readStrictJSON[failureinject.Arrival](path)
		if readErr != nil || !sameFileBarrierArrival(existing, arrival) {
			return errors.New("file barrier arrival identity conflicts with existing receipt")
		}
	}
	if err := syncDirectory(directory); err != nil {
		return err
	}
	releasePath := filepath.Join(directory, fileBarrierRelease)
	return waitForFileBarrierChange(ctx, directory, func() (bool, error) {
		info, err := os.Lstat(releasePath)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if !info.Mode().IsRegular() {
			return false, errors.New("file barrier release is not a regular file")
		}
		return true, nil
	})
}

func writeFileBarrierArrival(path string, arrival failureinject.Arrival) error {
	return writeJSONAtomicExclusive(path, arrival)
}

func readFileBarrierArrivals(directory string) ([]failureinject.Arrival, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read file barrier: %w", err)
	}
	arrivals := make([]failureinject.Arrival, 0)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		arrival, err := readStrictJSON[failureinject.Arrival](filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, err
		}
		if arrival.ID == "" || arrival.Point == "" || arrival.SessionID == "" ||
			arrival.ActorID == "" || arrival.Time.IsZero() ||
			entry.Name() != filepath.Base(fileBarrierArrivalPath(directory, arrival.ID)) {
			return nil, errors.New("file barrier contains an invalid arrival receipt")
		}
		arrivals = append(arrivals, arrival)
	}
	sort.Slice(arrivals, func(left, right int) bool {
		if arrivals[left].Time.Equal(arrivals[right].Time) {
			return arrivals[left].ID < arrivals[right].ID
		}
		return arrivals[left].Time.Before(arrivals[right].Time)
	})
	return arrivals, nil
}

func fileBarrierArrivalPath(directory, id string) string {
	digest := sha256.Sum256([]byte(id))
	return filepath.Join(directory, "arrival-"+hex.EncodeToString(digest[:])+".json")
}

func sameFileBarrierArrival(left, right failureinject.Arrival) bool {
	return left.ID == right.ID && left.Point == right.Point && left.SessionID == right.SessionID &&
		left.OwnerTokenHash == right.OwnerTokenHash && left.Generation == right.Generation &&
		left.ActorID == right.ActorID && left.PID == right.PID && left.ProcessStart == right.ProcessStart
}

func waitForFileBarrierChange(ctx context.Context, directory string, condition func() (bool, error)) error {
	ready, err := condition()
	if err != nil || ready {
		return err
	}
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return fmt.Errorf("initialize file barrier notification: %w", err)
	}
	defer unix.Close(fd)
	if _, err := unix.InotifyAddWatch(fd, directory, unix.IN_CLOSE_WRITE|unix.IN_MOVED_TO|unix.IN_CREATE); err != nil {
		return fmt.Errorf("watch file barrier: %w", err)
	}
	ready, err = condition()
	if err != nil || ready {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		descriptors := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		observed, err := unix.Poll(descriptors, 100)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("poll file barrier: %w", err)
		}
		if observed == 0 {
			continue
		}
		buffer := make([]byte, unix.SizeofInotifyEvent*8+unix.NAME_MAX+1)
		if _, err := unix.Read(fd, buffer); err != nil && !errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("observe file barrier: %w", err)
		}
		return nil
	}
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file barrier directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync file barrier directory: %w", err)
	}
	return directory.Close()
}
