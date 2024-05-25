package cmd

import (
	"reflect"
	"testing"

	"github.com/hbbtekademy/parquet-converter/pkg/param"
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
				cmd.Flags().Set("disable-autodetect", "true")
				cmd.Flags().Set("compression", "gzip")
				cmd.Flags().Set("format", "unstructured")
				cmd.Flags().Set("dateformat", "%d")
				cmd.Flags().Set("timestampformat", "%d")
				cmd.Flags().Set("max-depth", "10")
				cmd.Flags().Set("records", "true")
				cmd.Flags().Set("max-obj-size", "1024")
				cmd.Flags().Set("sample-size", "10")
				cmd.Flags().Set("convert-str-to-int", "true")
				cmd.Flags().Set("filename", "true")
				cmd.Flags().Set("hive-partitioning", "true")
				cmd.Flags().Set("ignore-errors", "true")
				cmd.Flags().Set("union-by-name", "true")
				cmd.Flags().Set("columns", "key1:INTEGER,key:2:VARCHAR")
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
				t.Fatalf("expected:\n%#v\nbut got:\n%#v", tc.expectedFlags, actual)
			}
		})
	}

}

func TestGetCsvReadFlags(t *testing.T) {
	tests := []struct {
		name          string
		setFlags      func(cmd *cobra.Command)
		expectedFlags *csv2ParquetFlags
	}{
		{
			name:     "TC1",
			setFlags: func(cmd *cobra.Command) {},
			expectedFlags: &csv2ParquetFlags{
				allVarchar:         false,
				disableQuotedNulls: false,
				disableAutodetect:  false,
				autoTypeCandidates: []string{},
				columns:            param.Columns{},
				compression:        "auto",
				dateformat:         "",
				decimalSeparator:   ".",
				delim:              ",",
				escape:             `"`,
				filename:           false,
				forceNotNull:       []string{},
				header:             false,
				hivePartitioning:   false,
				ignoreErrors:       false,
				maxLineSize:        2097152,
				names:              []string{},
				newLine:            "",
				normalizeNames:     false,
				nullPadding:        false,
				nullStr:            []string{},
				parallel:           false,
				quote:              `"`,
				sampleSize:         20480,
				skip:               0,
				timestampformat:    "",
				types:              param.Columns{},
				unionByName:        false,
			},
		},
		{
			name: "TC2",
			setFlags: func(cmd *cobra.Command) {
				cmd.Flags().Set("delim", "|")
				cmd.Flags().Set("quote", "'")
				cmd.Flags().Set("new-line", "\\n")
				cmd.Flags().Set("decimal-sep", ",")
				cmd.Flags().Set("escape", "'")
				cmd.Flags().Set("dateformat", "%d")
				cmd.Flags().Set("timestampformat", "%d")
				cmd.Flags().Set("compression", "gzip")
				cmd.Flags().Set("max-line-size", "100")
				cmd.Flags().Set("sample-size", "50")
				cmd.Flags().Set("skip", "10")
				cmd.Flags().Set("force-not-null", "col1,col2")
				cmd.Flags().Set("auto-type-candidates", "BIGINT,VARCHAR")
				cmd.Flags().Set("columns", "col1:BIGINT,col2:VARCHAR")
				cmd.Flags().Set("names", "col3,col4")
				cmd.Flags().Set("nullstr", "nul")
				cmd.Flags().Set("types", "col3:VARCHAR")
				cmd.Flags().Set("disable-autodetect", "true")
				cmd.Flags().Set("all-varchar", "true")
				cmd.Flags().Set("disable-quoted-nulls", "true")
				cmd.Flags().Set("normalize-names", "true")
				cmd.Flags().Set("filename", "true")
				cmd.Flags().Set("header", "true")
				cmd.Flags().Set("hive-partitioning", "true")
				cmd.Flags().Set("ignore-errors", "true")
				cmd.Flags().Set("null-padding", "true")
				cmd.Flags().Set("parallel", "true")
				cmd.Flags().Set("union-by-name", "true")
			},
			expectedFlags: &csv2ParquetFlags{
				allVarchar:         true,
				disableQuotedNulls: true,
				disableAutodetect:  true,
				autoTypeCandidates: []string{"BIGINT", "VARCHAR"},
				columns: param.Columns{
					"col1": "BIGINT",
					"col2": "VARCHAR",
				},
				compression:      "gzip",
				dateformat:       "%d",
				decimalSeparator: ",",
				delim:            "|",
				escape:           "'",
				filename:         true,
				forceNotNull:     []string{"col1", "col2"},
				header:           true,
				hivePartitioning: true,
				ignoreErrors:     true,
				maxLineSize:      100,
				names:            []string{"col3", "col4"},
				newLine:          "\\n",
				normalizeNames:   true,
				nullPadding:      true,
				nullStr:          []string{"nul"},
				parallel:         true,
				quote:            "'",
				sampleSize:       50,
				skip:             10,
				timestampformat:  "%d",
				types: param.Columns{
					"col3": "VARCHAR",
				},
				unionByName: true,
			},
		},
	}

	mockCmd := &cobra.Command{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCmd.ResetFlags()
			registerCsv2ParquetFlags(mockCmd)

			tc.setFlags(mockCmd)
			actual, err := getCsvReadFlags(mockCmd.LocalFlags())
			if err != nil {
				t.Fatalf("failed getting csv read flags. error: %v", err)
			}
			if !reflect.DeepEqual(actual, tc.expectedFlags) {
				t.Fatalf("expected:\n%#v\nbut got:\n%#v", tc.expectedFlags, actual)
			}
		})
	}
}
