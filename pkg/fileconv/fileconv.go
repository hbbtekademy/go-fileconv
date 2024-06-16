//go:build !windows

package fileconv

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/marcboeker/go-duckdb"
)

type fileconv struct {
	db *sql.DB
}

type DuckDBConfig string

// Returns an instance of parquet converter
func New(ctx context.Context, dbFile string, duckdbConfigs ...DuckDBConfig) (*fileconv, error) {
	dbConn, err := duckdb.NewConnector(dbFile, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			"INSTALL 'icu'",
			"LOAD 'icu'",
			"INSTALL 'json'",
			"LOAD 'json'",
		}

		for _, config := range duckdbConfigs {
			_, err := execer.ExecContext(ctx, string(config), nil)
			if err != nil {
				return fmt.Errorf("failed setting duckdb config: %s. error: %w", config, err)
			}
		}

		for _, query := range bootQueries {
			_, err := execer.ExecContext(ctx, query, nil)
			if err != nil {
				return fmt.Errorf("failed executing duckdb boot query: %s. error: %w", query, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(dbConn)

	var ver string
	if err := db.QueryRowContext(ctx, "select version()").Scan(&ver); err != nil {
		return nil, err
	}

	return &fileconv{
		db: db,
	}, nil
}

func GetDuckDBVersion() (string, error) {
	dbConn, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed getting duckdb connector. error: %w", err)
	}

	db := sql.OpenDB(dbConn)

	var ver string
	if err := db.QueryRowContext(context.Background(), "select version()").Scan(&ver); err != nil {
		return "", fmt.Errorf("failed getting duckdb version. error: %w", err)
	}

	return ver, nil
}
