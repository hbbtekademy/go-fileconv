package fileconv

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

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
			expectedRowCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dbFile := fmt.Sprintf("test_%d.db", time.Now().UnixNano())
			defer os.RemoveAll(dbFile)
			defer os.RemoveAll(dbFile + ".wal")

			conv, err := New(context.Background(), dbFile, tc.duckdbConfigs...)
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

func TestDescribeJson(t *testing.T) {
	tests := []struct {
		name           string
		jsonReadParams []jsonparam.ReadParam
		inputJson      string
		expectedDesc   string
	}{
		{
			name: "TC1",
			jsonReadParams: []jsonparam.ReadParam{
				jsonparam.WithFlatten(true),
				jsonparam.WithDescribe(true),
			},
			inputJson: "../../testdata/json/nested.json",
			expectedDesc: `COLUMN NAME     | COLUMN TYPE 
================|============
a1              | VARCHAR     
a2_b1           | VARCHAR     
a2_b2_c1        | BIGINT      
a2_b2_c2        | VARCHAR     
a2_b2_c3_d1     | BIGINT      
a2_b3_d1        | BIGINT      
a2_b3_d2        | VARCHAR     
a3              | VARCHAR     
a4_b1           | VARCHAR     
`,
		},
		{
			name: "TC2",
			jsonReadParams: []jsonparam.ReadParam{
				jsonparam.WithDescribe(true),
			},
			inputJson: "../../testdata/json/nested.json",
			expectedDesc: `COLUMN NAME| COLUMN TYPE                                                                                                      
=======|=================================================================================================================
a1     | VARCHAR                                                                                                          
a2     | STRUCT(b1 VARCHAR, b2 STRUCT(c1 BIGINT, c2 VARCHAR, c3 STRUCT(d1 BIGINT)), b3 STRUCT(d1 DOUBLE, d2 VARCHAR))     
a3     | VARCHAR                                                                                                          
a4     | STRUCT(b1 VARCHAR)                                                                                               
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dbFile := fmt.Sprintf("test_%d.db", time.Now().UnixNano())
			defer os.RemoveAll(dbFile)
			defer os.RemoveAll(dbFile + ".wal")

			conv, err := New(context.Background(), dbFile)
			if err != nil {
				t.Fatalf("failed getting duckdb client. error: %v", err)
			}

			actual, err := conv.describeJson(context.Background(), tc.inputJson, jsonparam.NewReadParams(tc.jsonReadParams...))
			if err != nil {
				t.Fatalf("failed getting json desc. error: %v", err)
			}

			if actual != tc.expectedDesc {
				t.Fatalf("expected:\n%s\nbut got:\n%s\n", tc.expectedDesc, actual)
			}
		})
	}
}
