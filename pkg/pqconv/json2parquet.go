//go:build !windows

package pqconv

import (
	"context"
	"fmt"

	"github.com/hbbtekademy/go-fileconv/pkg/param/jsonparam"
	"github.com/hbbtekademy/go-fileconv/pkg/param/pqparam"
)

// Convert json files to parquet files
func (c *pqconv) Json2Parquet(ctx context.Context, srcJson string, dest string, pqWriteParams *pqparam.WriteParams, jsonParams ...jsonparam.ReadParam) error {
	jsonReadParams := jsonparam.NewReadParams(jsonParams...)

	_, err := c.db.ExecContext(ctx, fmt.Sprintf("COPY (SELECT * FROM read_json('%s' %s)) TO '%s' %s",
		srcJson,
		jsonReadParams.Params(),
		dest,
		pqWriteParams.Params()))
	if err != nil {
		return err
	}

	return nil
}
