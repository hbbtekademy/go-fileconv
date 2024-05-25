package pqconv

import (
	"context"
	"testing"

	"github.com/hbbtekademy/parquet-converter/pkg/param/pqparam"
)

func TestCsv2Parquet(t *testing.T) {
	tests := []struct {
		name                          string
		setup                         func() error
		pqParams                      []pqparam.WriteParam
		inputCsv                      string
		outputParquet                 string
		outputPartitionedParquetRegex string
		expectedRowCount              int
	}{
		{
			name: "TC1",
			pqParams: []pqparam.WriteParam{
				pqparam.WithCompression(pqparam.Zstd),
				pqparam.WithRowGroupSize(50),
			},
			inputCsv:         "../../testdata/iris150.csv",
			outputParquet:    "../../testdata/iris150_csv.parquet",
			expectedRowCount: 150,
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

			err = conv.Csv2Parquet(context.Background(), tc.inputCsv, tc.outputParquet, pqparam.NewWriteParams(tc.pqParams...))
			if err != nil {
				t.Fatalf("failed converting json to parquet. error: %v", err)
			}

			err = validateParquetOutput(conv, tc.outputParquet, tc.outputPartitionedParquetRegex, tc.expectedRowCount)
			if err != nil {
				t.Fatal(err)
			}
			defer deleteOutput(tc.outputParquet)
		})
	}
}
