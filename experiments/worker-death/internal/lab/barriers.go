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
	serveError  chan error
}

func startBarrierService() (*barrierService, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for failure-injection barriers: %w", err)
	}
	coordinator := failureinject.NewCoordinator()
	server := &http.Server{
		Handler: coordinator.Handler(), ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout: 30 * time.Second,
	}
	service := &barrierService{
		coordinator: coordinator, listener: listener, server: server, serveError: make(chan error, 1),
	}
	go func() {
		service.serveError <- server.Serve(listener)
	}()
	return service, nil
}

func (s *barrierService) URL() string {
	return "http://" + s.listener.Addr().String()
}

func (s *barrierService) stop(ctx context.Context) error {
	shutdownErr := s.server.Shutdown(ctx)
	serveErr := <-s.serveError
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(shutdownErr, serveErr)
}

func arrivalMatchesExpected(actual, expected failureinject.Arrival) bool {
	return actual.ID == expected.ID &&
		actual.Point == expected.Point &&
		actual.SessionID == expected.SessionID &&
		actual.OwnerTokenHash == expected.OwnerTokenHash &&
		actual.Generation == expected.Generation &&
		actual.ActorID == expected.ActorID &&
		actual.PID == expected.PID &&
		actual.ProcessStart == expected.ProcessStart
}
