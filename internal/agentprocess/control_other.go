//go:build !linux

package agentprocess

import "fmt"

func CaptureIdentity(pid int) (ProcessIdentity, error) {
	return ProcessIdentity{}, fmt.Errorf("%w on this operating system", ErrProcessControlUnsupported)
}

func Probe(identity ProcessIdentity) (Disposition, error) {
	return "", fmt.Errorf("%w on this operating system", ErrProcessControlUnsupported)
}

func ProbeProcessGroup(identity ProcessIdentity) (Disposition, error) {
	return "", fmt.Errorf("%w on this operating system", ErrProcessControlUnsupported)
}

func SignalProcessGroup(identity ProcessIdentity, signal ControlSignal) error {
	return fmt.Errorf("%w on this operating system", ErrProcessControlUnsupported)
}

func Signal(request ControlRequest) (ControlResult, error) {
	return ControlResult{}, fmt.Errorf("%w on this operating system", ErrProcessControlUnsupported)
}
