package pqconv

import (
	"context"
	"fmt"
)

func (c *pqconv) Json2Parquet(ctx context.Context, srcJson string, dest string, params ...jsonParam) error {
	_ = getJsonParameters(params...)

	_, err := c.db.ExecContext(ctx, fmt.Sprintf("COPY (SELECT * FROM read_json('%s')) TO '%s' (FORMAT 'parquet')", srcJson, dest))
	if err != nil {
		return err
	}

	return nil
}
