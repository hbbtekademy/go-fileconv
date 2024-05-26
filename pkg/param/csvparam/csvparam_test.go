package csvparam

import (
	"testing"

	"github.com/hbbtekademy/parquet-converter/pkg/param"
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
				WithAllVarchar(true),
				WithAllowQuotedNulls(true),
				WithAutoDetect(false),
				WithCompression(param.Gzip),
				WithDateformat("%d"),
				WithDecimalSeparator(","),
				WithDelim("|"),
				WithEscape("`"),
				WithFilename(true),
				WithHeader(true),
				WithHivePartitioning(true),
				WithIgnoreErrors(true),
				WithMaxLineSize(100),
				WithNewLine(`\r\n`),
				WithNormalizeNames(true),
				WithNullPadding(true),
				WithParallel(true),
				WithQuote("`"),
				WithSampleSize(10),
				WithSkip(10),
				WithTimestampFormat("%d"),
				WithUnionByName(true),
			},
			expectedOutput: ",all_varchar = true,auto_detect = false,compression = 'gzip',dateformat = '%d',decimal_separator = ',',delim = '|',escape = '`',filename = true,header = true,hive_partitioning = true,ignore_errors = true,max_line_size = 100,new_line = '\\r\\n',normalize_names = true,null_padding = true,parallel = true,quote = '`',sample_size = 10,skip = 10,timestampformat = '%d',union_by_name = true",
		},
		{
			name: "TC3",
			params: []ReadParam{
				WithAutoTypeCandidates([]string{"col1", "col2"}),
				WithColumns(param.Columns{
					{Name: "col1", Type: "BIGINT"},
					{Name: "col2", Type: "VARCHAR"},
				}),
				WithForceNotNull([]string{"col1"}),
				WithNames([]string{"col1"}),
				WithNullStrings([]string{"nul"}),
				WithTypes(param.Columns{
					{Name: "col1", Type: "VARCHAR"},
				}),
			},
			expectedOutput: ",auto_type_candidates = ['col1','col2'],columns = {'col1': 'BIGINT','col2': 'VARCHAR'},force_not_null = ['col1'],names = ['col1'],nullstr = ['nul'],types = {'col1': 'VARCHAR'}",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := NewReadParams(tc.params...)
			actualOutput := params.Params()

			if actualOutput != tc.expectedOutput {
				t.Fatalf("expected:\n%s\nbut got:\n%s", tc.expectedOutput, actualOutput)
			}
		})
	}
}
