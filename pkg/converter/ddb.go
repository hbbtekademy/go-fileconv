package converter

import (
	"database/sql"
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb"
)

type ddbClient struct {
	db *sql.DB
}

// TODO: Handle duckdb options
func NewClient(dbFile string) (*ddbClient, error) {
	dbConn, err := duckdb.NewConnector(dbFile, func(execer driver.ExecerContext) error {
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &ddbClient{
		db: sql.OpenDB(dbConn),
	}, nil
}
