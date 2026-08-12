package lab

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func addBarrierCredential(command *exec.Cmd, credential failureinject.Credential) (*os.File, error) {
	if !credential.IsSet() {
		return nil, nil
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("create barrier credential pipe: %w", err)
	}
	if err := credential.Write(writer); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("close barrier credential writer: %w", err)
	}
	childFD := 3 + len(command.ExtraFiles)
	command.ExtraFiles = append(command.ExtraFiles, reader)
	command.Env = mergeEnvironment(command.Env, []string{
		fmt.Sprintf("%s=%d", failureinject.CredentialFDEnvironment, childFD),
	})
	return reader, nil
}
