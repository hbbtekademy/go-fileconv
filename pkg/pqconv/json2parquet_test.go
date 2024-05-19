package pqconv

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/hbbtekademy/parquet-converter/pkg/pqparam"
)

const testdataPath = "../../testdata"

func TestJson2Parquet(t *testing.T) {
	tests := []struct {
		name             string
		inputJson        string
		outputParquet    string
		expectedRowCount int
	}{
		{name: "TC1", inputJson: "../../testdata/iris150.json", outputParquet: "../../testdata/iris150.parquet", expectedRowCount: 150},
		{name: "TC2", inputJson: "../../testdata/iris*.json", outputParquet: "../../testdata/iris155.parquet", expectedRowCount: 155},
	}

	conv, err := New(context.Background(), "")
	if err != nil {
		t.Fatalf("failed getting duckdb client. error: %v", err)
	}
	pqParams := pqparam.New()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err = conv.Json2Parquet(context.Background(), tc.inputJson, tc.outputParquet, pqParams)
			if err != nil {
				t.Fatalf("failed converting json to parquet. error: %v", err)
			}

			rowcount := 0
			if err = conv.db.QueryRowContext(context.Background(),
				fmt.Sprintf("select count(1) from '%s'", tc.outputParquet)).Scan(&rowcount); err != nil {
				t.Fatalf("failed validating parquet rowcount. error: %v", err)
			}

			if rowcount != tc.expectedRowCount {
				t.Fatalf("expected 150 rows but got: %d", rowcount)
			}
			defer deleteOutput(tc.outputParquet)
		})
	}
}

func deleteOutput(outputPath string) error {
	outputPath = path.Clean(outputPath)
	if !strings.HasPrefix(outputPath, testdataPath) {
		return fmt.Errorf("cannot delete files outside %s", testdataPath)
	}

	if outputPath == testdataPath {
		return fmt.Errorf("cannot delete entire testdata directory")
	}

	return os.RemoveAll(outputPath)
}
