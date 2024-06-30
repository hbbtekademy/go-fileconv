//go:build !windows

package fileconv

import (
	"context"
	"fmt"
)

func getParquetRowCount(conv *fileconv, parquetFile string) (int, error) {
	rowcount := 0
	if err := conv.db.QueryRowContext(context.Background(),
		fmt.Sprintf("select count(1) from '%s'", parquetFile)).Scan(&rowcount); err != nil {
		return 0, fmt.Errorf("failed validating parquet rowcount. error: %v", err)
	}

	return rowcount, nil
}
