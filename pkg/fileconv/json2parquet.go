//go:build !windows

package fileconv

import (
	"context"
	"fmt"

	"github.com/hbbtekademy/go-fileconv/pkg/param/jsonparam"
	"github.com/hbbtekademy/go-fileconv/pkg/param/pqparam"
)

// Convert json files to parquet files
func (c *fileconv) Json2Parquet(ctx context.Context, srcJson string, dest string, pqWriteParams *pqparam.WriteParams, jsonParams ...jsonparam.ReadParam) error {
	jsonReadParams := jsonparam.NewReadParams(jsonParams...)

	if !jsonReadParams.GetFlatten() {
		_, err := c.db.ExecContext(ctx, fmt.Sprintf(`
		COPY (
			SELECT * FROM read_json('%s' %s)
		) TO '%s' %s`,
			srcJson,
			jsonReadParams.Params(),
			dest,
			pqWriteParams.Params()))
		if err != nil {
			return err
		}
		return nil
	}

	// Import json into temp table

	// describe table

	// For every struct column flatten it

	// COPY (select with column alias (select with unnest for struct columns)) to parquet

	return nil
}
