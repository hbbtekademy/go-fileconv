package fileconv

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
)

const testdataPath = "../../testdata"

func validateParquetOutput(conv *pqconv, outputParquet, outputPartitionedParquetRegex string, expectedRowCount int) error {
	parquetFile := outputParquet
	if outputPartitionedParquetRegex != "" {
		parquetFile = outputPartitionedParquetRegex
	}

	rowcount := 0
	if err := conv.db.QueryRowContext(context.Background(),
		fmt.Sprintf("select count(1) from '%s'", parquetFile)).Scan(&rowcount); err != nil {
		return fmt.Errorf("failed validating parquet rowcount. error: %v", err)
	}

	if rowcount != expectedRowCount {
		return fmt.Errorf("expected: %d rows but got: %d", expectedRowCount, rowcount)
	}

	return nil
}

func deleteOutput(outputPath string) error {
	outputPath = path.Clean(outputPath)
	if !strings.HasPrefix(outputPath, testdataPath) {
		return fmt.Errorf("cannot delete files outside %s", testdataPath)
	}

	if outputPath == testdataPath {
		return fmt.Errorf("cannot delete entire testdata directory")
	}

	return os.RemoveAll(outputPath)
}
