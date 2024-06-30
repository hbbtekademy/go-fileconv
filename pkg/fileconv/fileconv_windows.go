package fileconv

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

type fileconv struct {
	dbFile   string
	commands []string
}

func New(ctx context.Context, dbFile string, duckdbConfigs ...DuckDBConfig) (*fileconv, error) {
	fconv := &fileconv{
		dbFile:   dbFile,
		commands: make([]string, 0, len(duckdbConfigs)+4),
	}

	fconv.commands = append(fconv.commands, "INSTALL 'icu';")
	fconv.commands = append(fconv.commands, "LOAD 'icu';")
	fconv.commands = append(fconv.commands, "INSTALL 'json';")
	fconv.commands = append(fconv.commands, "LOAD 'json';")

	for _, config := range duckdbConfigs {
		fconv.commands = append(fconv.commands, fmt.Sprintf("%s;", config))
	}

	stdout, stderr, err := fconv.execDuckDbCli(ctx, fconv.commands)
	if err != nil {
		return nil, fmt.Errorf("failed executing duckdb cli. stdout: %s, stderr: %s. error: %v", stdout, stderr, err)
	}

	return fconv, nil
}

func GetDuckDBVersion() (string, error) {
	cmd := exec.Command("duckdb", "--version")

	var stdoutBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("failed starting duckdb process. error: %v", err)
	}

	err := cmd.Wait()
	if err != nil {
		return "", fmt.Errorf("failed waiting on duckdb process. error: %v", err)
	}

	return stdoutBuf.String(), nil
}
