package fileconv

import (
	"context"
	"encoding/json"
	"fmt"
)

func getParquetRowCount(conv *fileconv, parquetFile string) (int, error) {
	type rowcount struct {
		Count int `json:"count"`
	}

	rc := []rowcount{
		{Count: 0},
	}
	stdout, _, err := conv.execDuckDbCli(context.Background(), []string{}, "-json", "-c", fmt.Sprintf("select count(1) as count from '%s'", parquetFile))
	if err != nil {
		return 0, err
	}

	err = json.Unmarshal([]byte(stdout), &rc)
	if err != nil {
		return 0, err
	}

	return rc[0].Count, nil
}
