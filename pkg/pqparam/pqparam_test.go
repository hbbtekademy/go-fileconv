package pqparam

import "testing"

func TestWriteParams(t *testing.T) {
	tests := []struct {
		name           string
		params         []WriteParam
		expectedOutput string
	}{
		{
			name:           "TC1",
			params:         []WriteParam{},
			expectedOutput: "(FORMAT PARQUET)",
		},
		{
			name: "TC2",
			params: []WriteParam{
				WithHivePartitionConfig(
					WithFilenamePattern("output_{uuid}"),
					WithOverwriteOrIgnore(true),
					WithPartitionBy("col1", "col2")),
			},
			expectedOutput: "(FORMAT PARQUET,PARTITION_BY (col1,col2),OVERWRITE_OR_IGNORE 1,FILENAME_PATTERN 'output_{uuid}')",
		},
		{
			name: "TC3",
			params: []WriteParam{
				WithCompression(Zstd),
				WithRowGroupSize(50000),
				WithPerThreadOutput(true),
			},
			expectedOutput: "(FORMAT PARQUET,COMPRESSION 'zstd',ROW_GROUP_SIZE 50000,PER_THREAD_OUTPUT true)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := NewWriteParams(tc.params...)
			actualOutput := params.Params()

			if actualOutput != tc.expectedOutput {
				t.Fatalf("expected: %s but got: %s", tc.expectedOutput, actualOutput)
			}
		})
	}
}

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
			name: "TC1",
			params: []ReadParam{
				WithBinaryAsString(true),
				WithFileRowNum(true),
				WithFilename(true),
				WithHivePartition(true),
				WithUnionByName(true),
			},
			expectedOutput: ",binary_as_string = true,file_row_number = true,filename = true,hive_partitioning = true,union_by_name = true",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := NewReadParams(tc.params...)
			actualOutput := params.Params()

			if actualOutput != tc.expectedOutput {
				t.Fatalf("expected: %s but got: %s", tc.expectedOutput, actualOutput)
			}
		})
	}
}
