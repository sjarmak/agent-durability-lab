package lab

import (
	"context"
	"errors"
	"net/http/httptest"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

type barrierService struct {
	coordinator *failureinject.Coordinator
	server      *httptest.Server
	point       string
}

func startBarrierService(
	credential failureinject.Credential,
	expectation failureinject.Expectation,
) (*barrierService, error) {
	coordinator, err := failureinject.NewAuthenticatedCoordinator(credential, expectation)
	if err != nil {
		return nil, err
	}
	server := httptest.NewServer(coordinator.Handler())
	return &barrierService{coordinator: coordinator, server: server, point: expectation.Point}, nil
}

func (s *barrierService) URL() string {
	return s.server.URL
}

func (s *barrierService) stop(_ context.Context) error {
	releaseErr := s.coordinator.Release(s.point)
	if errors.Is(releaseErr, failureinject.ErrBarrierNotFound) {
		releaseErr = nil
	}
	s.server.CloseClientConnections()
	s.server.Close()
	return releaseErr
}
