package lab

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

type barrierService struct {
	coordinator *failureinject.Coordinator
	listener    net.Listener
	server      *http.Server
}

func startBarrierService() (*barrierService, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for failure barriers: %w", err)
	}
	coordinator := failureinject.NewCoordinator()
	server := &http.Server{
		Handler: coordinator.Handler(), ReadHeaderTimeout: 5 * time.Second,
	}
	service := &barrierService{coordinator: coordinator, listener: listener, server: server}
	go func() { _ = server.Serve(listener) }()
	return service, nil
}

func (s *barrierService) URL() string {
	return "http://" + s.listener.Addr().String()
}

func (s *barrierService) stop(ctx context.Context) error {
	err := s.server.Shutdown(ctx)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
