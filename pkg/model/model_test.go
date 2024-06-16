package model

import (
	"testing"
)

func TestGetUnnestedColumns(t *testing.T) {
	tests := []struct {
		name           string
		input          *TableDesc
		expectedOutput string
	}{
		{
			name: "TC1",
			input: &TableDesc{
				ColumnDescs: []*ColumnDesc{
					{ColName: "col1", ColType: "varchar"},
					{ColName: "col2", ColType: "integer"},
					{ColName: "col3", ColType: "double"},
				},
			},
			expectedOutput: "col1,col2,col3",
		},
		{
			name: "TC2",
			input: &TableDesc{
				ColumnDescs: []*ColumnDesc{
					{ColName: "a1", ColType: "varchar"},
					{ColName: "a2", ColType: "STRUCT(b1 VARCHAR, b2 STRUCT(c1 BIGINT, c2 VARCHAR, c3 STRUCT(d1 BIGINT)), b3 STRUCT(d1 DOUBLE, d2 VARCHAR))"},
					{ColName: "a3", ColType: "double"},
					{ColName: "a4", ColType: "STRUCT(b1 VARCHAR)"},
				},
			},
			expectedOutput: "a1,unnest(a2, recursive := true),a3,unnest(a4, recursive := true)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := tc.input.GetUnnestedColumns()
			if err != nil {
				t.Fatalf("failed getting unnested columns. error: %v", err)
			}

			if actual != tc.expectedOutput {
				t.Fatalf("expected: %s but got: %s", tc.expectedOutput, actual)
			}
		})
	}
}
