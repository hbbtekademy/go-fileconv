//go:build !windows

package pqconv

import (
	"context"
	"fmt"

	"github.com/hbbtekademy/parquet-converter/pkg/jsonparam"
	"github.com/hbbtekademy/parquet-converter/pkg/pqparam"
)

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
