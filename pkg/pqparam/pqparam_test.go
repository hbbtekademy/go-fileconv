package pqparam

import "testing"

func TestWriteParams(t *testing.T) {
	tests := []struct {
		name           string
		inParams       []Param
		expectedOutput string
	}{
		{
			name:           "TC1",
			inParams:       []Param{},
			expectedOutput: "(FORMAT PARQUET)",
		},
		{
			name: "TC2",
			inParams: []Param{
				WithHivePartitionConfig(
					WithFilenamePattern("output_{uuid}"),
					WithOverwriteOrIgnore(true),
					WithPartitionBy("col1", "col2")),
			},
			expectedOutput: "(FORMAT PARQUET,PARTITION_BY (col1,col2),OVERWRITE_OR_IGNORE 1,FILENAME_PATTERN 'output_{uuid}')",
		},
		{
			name: "TC3",
			inParams: []Param{
				WithCompression(Zstd),
				WithRowGroupSize(50000),
				WithPerThreadOutput(true),
			},
			expectedOutput: "(FORMAT PARQUET,COMPRESSION 'zstd',ROW_GROUP_SIZE 50000,PER_THREAD_OUTPUT true)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := New(tc.inParams...)
			actualOutput := params.WriteParams()

			if actualOutput != tc.expectedOutput {
				t.Fatalf("expected: %s but got: %s", tc.expectedOutput, actualOutput)
			}
		})
	}
}
