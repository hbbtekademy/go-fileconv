//go:build !windows

package pqconv

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb"
)

type pqconv struct {
	db *sql.DB
}

// Returns an instance of parquet converter
func New(ctx context.Context, dbFile string) (*pqconv, error) {
	// TODO: Handle duckdb options.
	dbConn, err := duckdb.NewConnector(dbFile, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			"INSTALL 'icu'",
			"LOAD 'icu'",
			"INSTALL 'json'",
			"LOAD 'json'",
			"INSTALL 'parquet'",
			"LOAD 'parquet'",
		}

		for _, query := range bootQueries {
			_, err := execer.ExecContext(ctx, query, nil)
			if err != nil {
				return err
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

	return &pqconv{
		db: db,
	}, nil
}
