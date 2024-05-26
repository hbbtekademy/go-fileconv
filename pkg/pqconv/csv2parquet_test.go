package pqconv

import (
	"context"
	"testing"

	"github.com/hbbtekademy/parquet-converter/pkg/param/csvparam"
	"github.com/hbbtekademy/parquet-converter/pkg/param/pqparam"
)

func TestCsv2Parquet(t *testing.T) {
	tests := []struct {
		name                          string
		setup                         func() error
		pqParams                      []pqparam.WriteParam
		csvReadParams                 []csvparam.ReadParam
		duckdbConfigs                 []DuckDBConfig
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
			csvReadParams: []csvparam.ReadParam{
				csvparam.WithHeader(true),
				csvparam.WithColumns([]csvparam.Column{
					{Name: "sepal_l", Type: "VARCHAR"},
					{Name: "sepal_w", Type: "INTEGER"},
					{Name: "petal_l", Type: "VARCHAR"},
					{Name: "petal_w", Type: "DOUBLE"},
					{Name: "species", Type: "VARCHAR"},
				}),
			},
			duckdbConfigs: []DuckDBConfig{
				"SET threads TO 2",
				"SET memory_limit = '128MB'",
			},
			inputCsv:         "../../testdata/csv/iris150.csv",
			outputParquet:    "../../testdata/csv/iris150.parquet",
			expectedRowCount: 150,
		},
		{
			name: "TC2",
			pqParams: []pqparam.WriteParam{
				pqparam.WithCompression(pqparam.Zstd),
				pqparam.WithRowGroupSize(50),
			},
			csvReadParams: []csvparam.ReadParam{
				csvparam.WithHeader(false),
				csvparam.WithNames([]string{"sepal_length", "sepal_width", "petal_length", "petal_width"}),
			},
			inputCsv:         "../../testdata/csv/iris150_noheader.csv",
			outputParquet:    "../../testdata/csv/iris150_noheader.parquet",
			expectedRowCount: 150,
		},
		{
			name: "TC3",
			pqParams: []pqparam.WriteParam{
				pqparam.WithCompression(pqparam.Zstd),
				pqparam.WithRowGroupSize(50),
			},
			csvReadParams: []csvparam.ReadParam{
				csvparam.WithHeader(false),
				csvparam.WithNames([]string{"sepal_length", "sepal_width", "petal_length", "petal_width"}),
				csvparam.WithTypes(csvparam.Columns{
					{Name: "sepal_length", Type: "VARCHAR"},
				}),
			},
			inputCsv:         "../../testdata/csv/iris5_quotedNumber.csv",
			outputParquet:    "../../testdata/csv/iris5_quotedNumber.parquet",
			expectedRowCount: 5,
		},
	}

	for _, tc := range tests {
		conv, err := New(context.Background(), "", tc.duckdbConfigs...)
		if err != nil {
			t.Fatalf("failed getting duckdb client. error: %v", err)
		}

		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != nil {
				err := tc.setup()
				if err != nil {
					t.Fatalf("setup failed. error: %v", err)
				}
			}

			err = conv.Csv2Parquet(context.Background(), tc.inputCsv, tc.outputParquet,
				pqparam.NewWriteParams(tc.pqParams...),
				tc.csvReadParams...)
			if err != nil {
				t.Fatalf("failed converting csv to parquet. error: %v", err)
			}

			err = validateParquetOutput(conv, tc.outputParquet, tc.outputPartitionedParquetRegex, tc.expectedRowCount)
			if err != nil {
				t.Fatal(err)
			}
			defer deleteOutput(tc.outputParquet)
		})
	}
}
