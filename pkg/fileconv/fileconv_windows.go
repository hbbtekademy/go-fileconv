package fileconv

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

type fileconv struct {
	dbFile         string
	duckdbSettings []string
}

func New(ctx context.Context, dbFile string, duckdbConfigs ...DuckDBConfig) (*fileconv, error) {
	fconv := &fileconv{
		dbFile:         dbFile,
		duckdbSettings: make([]string, 0, len(duckdbConfigs)),
	}

	for _, config := range duckdbConfigs {
		fconv.duckdbSettings = append(fconv.duckdbSettings, fmt.Sprintf("%s;", config))
	}

	initCmds := []string{
		"INSTALL 'icu';",
		"LOAD 'icu';",
		"INSTALL 'json';",
		"LOAD 'json';",
	}
	initCmds = append(initCmds, fconv.duckdbSettings...)

	stdout, stderr, err := fconv.execDuckDbCli(ctx, initCmds)
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
