package cmd

import (
	"reflect"
	"testing"

	"github.com/spf13/cobra"
)

func TestGetPqWriteFlags(t *testing.T) {
	tests := []struct {
		name          string
		setFlags      func(cmd *cobra.Command)
		expectedFlags *pqWriteFlags
	}{
		{
			name:     "TC1",
			setFlags: func(cmd *cobra.Command) {},
			expectedFlags: &pqWriteFlags{
				compression:       "snappy",
				partitionBy:       []string{},
				filenamePattern:   "data_{i}.parquet",
				overwriteOrIgnore: false,
				perThreadOutput:   false,
			},
		},
		{
			name: "TC2",
			setFlags: func(cmd *cobra.Command) {
				cmd.PersistentFlags().Set(PQ_COMPRESSION, "zstd")
				cmd.PersistentFlags().Set(PQ_PARTITION_BY, "col1,col2")
				cmd.PersistentFlags().Set(PQ_FILENAME_PATTERN, "file_{i}.parquet")
				cmd.PersistentFlags().Set(PQ_OVERWRITE_OR_IGNORE, "true")
				cmd.PersistentFlags().Set(PQ_PER_THREAD_OUTPUT, "true")
			},
			expectedFlags: &pqWriteFlags{
				compression:       "zstd",
				partitionBy:       []string{"col1", "col2"},
				filenamePattern:   "file_{i}.parquet",
				overwriteOrIgnore: true,
				perThreadOutput:   true,
			},
		},
	}
	mockCmd := &cobra.Command{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCmd.ResetFlags()
			registerGlobalFlags(mockCmd)

			tc.setFlags(mockCmd)
			actual, err := getPqWriteFlags(mockCmd.PersistentFlags())
			if err != nil {
				t.Fatalf("failed getting json read flags. error: %v", err)
			}
			if !reflect.DeepEqual(actual, tc.expectedFlags) {
				t.Fatalf("expected: %v but got: %v", tc.expectedFlags, actual)
			}
		})
	}
}

func TestGetJsonReadFlags(t *testing.T) {
	tests := []struct {
		name          string
		setFlags      func(cmd *cobra.Command)
		expectedFlags *json2ParquetFlags
	}{
		{
			name:     "TC1",
			setFlags: func(cmd *cobra.Command) {},
			expectedFlags: &json2ParquetFlags{
				disableAutodetect: false,
				compression:       "auto",
				convStr2Int:       false,
				dateformat:        "iso",
				filename:          false,
				format:            "array",
				hivePartitioning:  false,
				ignoreErrors:      false,
				maxDepth:          -1,
				maxObjSize:        16777216,
				records:           "auto",
				sampleSize:        20480,
				timestampformat:   "iso",
				unionByName:       false,
				columns:           map[string]string{},
			},
		},
		{
			name: "TC2",
			setFlags: func(cmd *cobra.Command) {
				cmd.LocalFlags().Set("disable-autodetect", "true")
				cmd.LocalFlags().Set("compression", "gzip")
				cmd.LocalFlags().Set("format", "unstructured")
				cmd.LocalFlags().Set("dateformat", "%d")
				cmd.LocalFlags().Set("timestampformat", "%d")
				cmd.LocalFlags().Set("max-depth", "10")
				cmd.LocalFlags().Set("records", "true")
				cmd.LocalFlags().Set("max-obj-size", "1024")
				cmd.LocalFlags().Set("sample-size", "10")
				cmd.LocalFlags().Set("convert-str-to-int", "true")
				cmd.LocalFlags().Set("filename", "true")
				cmd.LocalFlags().Set("hive-partitioning", "true")
				cmd.LocalFlags().Set("ignore-errors", "true")
				cmd.LocalFlags().Set("union-by-name", "true")
				cmd.LocalFlags().Set("columns", "key1:INTEGER,key:2:VARCHAR")
			},
			expectedFlags: &json2ParquetFlags{
				disableAutodetect: true,
				compression:       "gzip",
				convStr2Int:       true,
				dateformat:        "%d",
				filename:          true,
				format:            "unstructured",
				hivePartitioning:  true,
				ignoreErrors:      true,
				maxDepth:          10,
				maxObjSize:        1024,
				records:           "true",
				sampleSize:        10,
				timestampformat:   "%d",
				unionByName:       true,
				columns: map[string]string{
					"key1":  "INTEGER",
					"key:2": "VARCHAR",
				},
			},
		},
	}

	mockCmd := &cobra.Command{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCmd.ResetFlags()
			registerJson2ParquetFlags(mockCmd)

			tc.setFlags(mockCmd)
			actual, err := getJsonReadFlags(mockCmd.LocalFlags())
			if err != nil {
				t.Fatalf("failed getting json read flags. error: %v", err)
			}
			if !reflect.DeepEqual(actual, tc.expectedFlags) {
				t.Fatalf("expected: %v but got: %v", tc.expectedFlags, actual)
			}
		})
	}

}
