package jsonparam

import (
	"testing"

	"github.com/hbbtekademy/go-fileconv/pkg/param"
)

func TestReadParams(t *testing.T) {
	tests := []struct {
		name           string
		params         []ReadParam
		expectedOutput string
	}{
		{
			name:           "TC1",
			params:         []ReadParam{},
			expectedOutput: "",
		},
		{
			name: "TC2",
			params: []ReadParam{
				WithAutoDetect(false),
				WithCompression(param.Gzip),
				WithConvStr2Int(true),
				WithDateFormat("%d"),
				WithFilename(true),
				WithFormat(NewlineDelimited),
				WithHivePartitioning(true),
				WithIgnoreErrors(true),
				WithMaxDepth(4),
				WithMaxObjSize(1024),
				WithRecords(True),
				WithTimestampFormat("%d"),
				WithUnionByName(true),
			},
			expectedOutput: ",auto_detect = false,compression = 'gzip',convert_strings_to_integers = true,dateformat = '%d',filename = true,format = 'newline_delimited',hive_partitioning = true,ignore_errors = true,maximum_depth = 4,maximum_object_size = 1024,records = 'true',timestampformat = '%d',union_by_name = true",
		},
		{
			name: "TC3",
			params: []ReadParam{
				WithColumns(param.Columns{
					{Name: "key1", Type: "INT"},
					{Name: "key2", Type: "VARCHAR"},
				}),
			},
			expectedOutput: ",columns = {key1: 'INT',key2: 'VARCHAR'}",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := NewReadParams(tc.params...)
			actualOutput := params.Params()

			if actualOutput != tc.expectedOutput {
				t.Fatalf("expected: %s\nbut got: %s", tc.expectedOutput, actualOutput)
			}
		})
	}
}
