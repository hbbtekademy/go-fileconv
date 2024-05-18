package pqconv

import (
	"context"
	"testing"
)

func TestJson2Parquet(t *testing.T) {
	_, err := New(context.Background(), "")
	if err != nil {
		t.Fatalf("failed getting duckdb client. error: %v", err)
	}
}
