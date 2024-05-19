//go:build !windows

package pqconv

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/hbbtekademy/parquet-converter/pkg/pqparam"
)

const testdataPath = "../../testdata"

func TestJson2Parquet(t *testing.T) {
	tests := []struct {
		name                          string
		setup                         func() error
		pqParams                      []pqparam.Param
		inputJson                     string
		outputParquet                 string
		outputPartitionedParquetRegex string
		expectedRowCount              int
	}{
		{
			name: "TC1",
			pqParams: []pqparam.Param{
				pqparam.WithCompression(pqparam.Zstd),
				pqparam.WithRowGroupSize(50),
			},
			inputJson:        "../../testdata/iris150.json",
			outputParquet:    "../../testdata/iris150.parquet",
			expectedRowCount: 150,
		},
		{
			name:             "TC2",
			pqParams:         []pqparam.Param{},
			inputJson:        "../../testdata/iris*.json",
			outputParquet:    "../../testdata/iris155.parquet",
			expectedRowCount: 155,
		},
		{
			name: "TC3",
			setup: func() error {
				err := os.RemoveAll("../../testdata/partition")
				if err != nil {
					return err
				}
				return os.Mkdir("../../testdata/partition", 0700)
			},
			pqParams: []pqparam.Param{
				pqparam.WithHivePartitionConfig(
					pqparam.WithPartitionBy("species"),
					pqparam.WithFilenamePattern("iris_{i}"),
					pqparam.WithOverwriteOrIgnore(true),
				),
			},
			inputJson:                     "../../testdata/iris150.json",
			outputParquet:                 "../../testdata/partition",
			outputPartitionedParquetRegex: "../../testdata/partition/species*/iris_*.parquet",
			expectedRowCount:              150,
		},
	}

	conv, err := New(context.Background(), "")
	if err != nil {
		t.Fatalf("failed getting duckdb client. error: %v", err)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != nil {
				err := tc.setup()
				if err != nil {
					t.Fatalf("setup failed. error: %v", err)
				}
			}

			err = conv.Json2Parquet(context.Background(), tc.inputJson, tc.outputParquet, pqparam.New(tc.pqParams...))
			if err != nil {
				t.Fatalf("failed converting json to parquet. error: %v", err)
			}

			parquetFile := tc.outputParquet
			if tc.outputPartitionedParquetRegex != "" {
				parquetFile = tc.outputPartitionedParquetRegex
			}

			rowcount := 0
			if err = conv.db.QueryRowContext(context.Background(),
				fmt.Sprintf("select count(1) from '%s'", parquetFile)).Scan(&rowcount); err != nil {
				t.Fatalf("failed validating parquet rowcount. error: %v", err)
			}

			if rowcount != tc.expectedRowCount {
				t.Fatalf("expected: %d rows but got: %d", tc.expectedRowCount, rowcount)
			}
			defer deleteOutput(tc.outputParquet)
		})
	}
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
