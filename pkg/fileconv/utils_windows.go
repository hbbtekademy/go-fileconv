package fileconv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/hbbtekademy/go-fileconv/pkg/model"
)

func (c *fileconv) GetTableDesc(ctx context.Context, table string) (*model.TableDesc, error) {
	descQuery := getDescribeQuery(table)
	stdout, stderr, err := c.execDuckDbCli(ctx, []string{descQuery}, "-json")
	if err != nil {
		return nil, fmt.Errorf("failed getting table desc. stdout: %s, stderr: %s. error: %v", stdout, stderr, err)
	}

	columnDescs := []model.ColumnDesc{}
	err = json.Unmarshal([]byte(stdout), &columnDescs)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshalling table desc json. error: %v", err)
	}

	tableDesc := &model.TableDesc{
		ColumnDescs: make([]*model.ColumnDesc, 0, len(columnDescs)),
	}

	for i := range columnDescs {
		tableDesc.ColumnDescs = append(tableDesc.ColumnDescs, &columnDescs[i])
	}

	return tableDesc, nil
}

func (c *fileconv) createStructColTable(ctx context.Context, columnDesc *model.ColumnDesc) (string, error) {
	tableName := fmt.Sprintf("%s_tmp_%d", columnDesc.ColName, time.Now().UnixNano())
	_, stderr, err := c.execDuckDbCli(ctx, []string{fmt.Sprintf("CREATE TABLE %s (C1 %s)", tableName, columnDesc.ColType)})
	if err != nil {
		return "", fmt.Errorf("failed creating table: %s. stderr: %s. error: %v", tableName, stderr, err)
	}

	return tableName, nil
}

func (c *fileconv) dropTable(ctx context.Context, tableName string) error {
	_, stderr, err := c.execDuckDbCli(ctx, []string{}, "-c", fmt.Sprintf("DROP TABLE %s", tableName))
	if err != nil {
		return fmt.Errorf("failed dropping table: %s. stderr: %s. error: %v", tableName, stderr, err)
	}

	return nil
}

func (c *fileconv) executeCmd(ctx context.Context, cmd string) error {
	_, stderr, err := c.execDuckDbCli(ctx, []string{cmd})
	if err != nil {
		return fmt.Errorf("failed executing cmd: %s. stderr: %s. error: %v", cmd, stderr, err)
	}
	return nil
}

func (c *fileconv) execDuckDbCli(ctx context.Context, cmds []string, args ...string) (string, string, error) {
	duckdbArgs := make([]string, 0, len(args)+1)
	duckdbArgs = append(duckdbArgs, c.dbFile)
	duckdbArgs = append(duckdbArgs, args...)

	cmd := exec.CommandContext(ctx, "duckdb", duckdbArgs...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return "", "", fmt.Errorf("failed getting duckdb stdin pipe. error: %v", err)
	}
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return stdoutBuf.String(), stderrBuf.String(), fmt.Errorf("failed starting duckdb process. error: %v", err)
	}

	for _, setting := range c.duckdbSettings {
		io.WriteString(stdin, setting)
	}

	for _, cmd := range cmds {
		io.WriteString(stdin, cmd)
	}

	err = stdin.Close()
	if err != nil {
		return stdoutBuf.String(), stderrBuf.String(), fmt.Errorf("failed closing duckdb stdin pipe. error: %v", err)
	}

	err = cmd.Wait()
	if err != nil {
		return stdoutBuf.String(), stderrBuf.String(), fmt.Errorf("duckdb error: %v", err)
	}

	if exitCode := cmd.ProcessState.ExitCode(); exitCode != 0 {
		return stdoutBuf.String(), stderrBuf.String(), fmt.Errorf("duckdb exit code: %d. error: %s",
			exitCode, stderrBuf.String())
	}
	if len(stderrBuf.String()) > 0 {
		return stdoutBuf.String(), stderrBuf.String(), fmt.Errorf("duckdb exited with error: %s", stderrBuf.String())
	}

	return stdoutBuf.String(), stderrBuf.String(), nil
}
