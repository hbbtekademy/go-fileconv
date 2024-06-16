//go:build !windows

package fileconv

import (
	"context"
	"os"
	"testing"

	"github.com/hbbtekademy/go-fileconv/pkg/param"
	"github.com/hbbtekademy/go-fileconv/pkg/param/jsonparam"
	"github.com/hbbtekademy/go-fileconv/pkg/param/pqparam"
)

func TestJson2Parquet(t *testing.T) {
	tests := []struct {
		name                          string
		setup                         func() error
		pqParams                      []pqparam.WriteParam
		jsonReadParams                []jsonparam.ReadParam
		duckdbConfigs                 []DuckDBConfig
		inputJson                     string
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
			jsonReadParams: []jsonparam.ReadParam{
				jsonparam.WithColumns(param.Columns{
					{Name: "sepalWidth", Type: "VARCHAR"},
					{Name: "species", Type: "VARCHAR"},
					{Name: "sepalLength", Type: "INTEGER"},
				}),
				jsonparam.WithConvStr2Int(true),
				jsonparam.WithFormat(jsonparam.Array),
			},
			duckdbConfigs: []DuckDBConfig{
				"SET threads TO 2",
				"SET memory_limit = '128MB'",
			},
			inputJson:        "../../testdata/json/iris150.json",
			outputParquet:    "../../testdata/json/iris150.parquet",
			expectedRowCount: 150,
		},
		{
			name:             "TC2",
			pqParams:         []pqparam.WriteParam{},
			inputJson:        "../../testdata/json/iris*.json",
			outputParquet:    "../../testdata/json/iris155.parquet",
			expectedRowCount: 155,
		},
		{
			name: "TC3",
			setup: func() error {
				err := os.RemoveAll("../../testdata/json/partition")
				if err != nil {
					return err
				}
				return os.Mkdir("../../testdata/json/partition", 0700)
			},
			pqParams: []pqparam.WriteParam{
				pqparam.WithHivePartitionConfig(
					pqparam.WithPartitionBy("species"),
					pqparam.WithFilenamePattern("iris_{i}"),
					pqparam.WithOverwriteOrIgnore(true),
				),
			},
			inputJson:                     "../../testdata/json/iris150.json",
			outputParquet:                 "../../testdata/json/partition",
			outputPartitionedParquetRegex: "../../testdata/json/partition/species*/iris_*.parquet",
			expectedRowCount:              150,
		},
		{
			name: "TC4",
			pqParams: []pqparam.WriteParam{
				pqparam.WithCompression(pqparam.Zstd),
			},
			jsonReadParams: []jsonparam.ReadParam{
				jsonparam.WithFlatten(true),
			},
			inputJson:        "../../testdata/json/nested.json",
			outputParquet:    "../../testdata/json/nested.parquet",
			expectedRowCount: 150,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conv, err := New(context.Background(), "", tc.duckdbConfigs...)
			if err != nil {
				t.Fatalf("failed getting duckdb client. error: %v", err)
			}

			if tc.setup != nil {
				err := tc.setup()
				if err != nil {
					t.Fatalf("setup failed. error: %v", err)
				}
			}

			err = conv.Json2Parquet(context.Background(), tc.inputJson, tc.outputParquet,
				pqparam.NewWriteParams(tc.pqParams...), tc.jsonReadParams...)
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
