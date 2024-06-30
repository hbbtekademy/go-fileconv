package fileconv

import (
	"fmt"
	"os"
	"path"
	"strings"
)

const testdataPath = "../../testdata"

func validateParquetOutput(conv *fileconv, outputParquet, outputPartitionedParquetRegex string, expectedRowCount int) error {
	parquetFile := outputParquet
	if outputPartitionedParquetRegex != "" {
		parquetFile = outputPartitionedParquetRegex
	}

	rowcount, err := getParquetRowCount(conv, parquetFile)
	if err != nil {
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
