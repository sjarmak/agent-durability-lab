package lab

import "github.com/sjarmak/temporal_projects/internal/failureinject"

func httpBarrierExpectations(mode RecoveryMode, boundary FaultBoundary, sessionID string) []failureinject.Expectation {
	if mode.normalized() != RecoveryModeFenced {
		point := ""
		switch boundary {
		case FaultBeforeThreadObservation:
			point = preThreadBarrier
		case FaultAfterFinalOutput:
			point = finalOutputBarrier
		default:
			return nil
		}
		return []failureinject.Expectation{
			{Point: point, SessionID: sessionID, Generation: 1, ActorID: "worker-one-attempt-1"},
			{Point: point, SessionID: sessionID, Generation: 1, ActorID: "worker-two-attempt-2"},
		}
	}
	point := ""
	switch boundary {
	case FaultAfterClaimBeforeExec:
		point = claimBeforeExecBarrier
	case FaultBeforeThreadObservation, FaultProcessFailureReplacement:
		point = preThreadBarrier
	case FaultAfterThreadBeforeRegistration, FaultCancellationWhileExecuting:
		point = threadRegistrationBarrier
	case FaultAfterFinalOutput:
		point = finalOutputBarrier
	default:
		return nil
	}
	expected := []failureinject.Expectation{
		{Point: point, SessionID: sessionID, Generation: 1, ActorID: "codex-supervisor-g1"},
	}
	if boundary == FaultProcessFailureReplacement {
		expected = append(expected, failureinject.Expectation{
			Point: point, SessionID: sessionID, Generation: 2, ActorID: "codex-supervisor-g2",
		})
	}
	return expected
}

func newBarrierClient(url string, credential failureinject.Credential) *failureinject.Client {
	if !credential.IsSet() {
		return failureinject.NewClient(url)
	}
	return failureinject.NewAuthenticatedClient(url, credential)
}
