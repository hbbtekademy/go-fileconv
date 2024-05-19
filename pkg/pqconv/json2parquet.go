//go:build !windows

package pqconv

import (
	"context"
	"fmt"

	"github.com/hbbtekademy/parquet-converter/pkg/jsonparam"
	"github.com/hbbtekademy/parquet-converter/pkg/pqparam"
)

func (c *pqconv) Json2Parquet(ctx context.Context, srcJson string, dest string, pqParams *pqparam.Params, jsonParams ...jsonparam.Param) error {
	_ = jsonparam.New(jsonParams...)

	_, err := c.db.ExecContext(ctx, fmt.Sprintf("COPY (SELECT * FROM read_json('%s')) TO '%s' %s", srcJson, dest, pqParams.WriteParams()))
	if err != nil {
		return err
	}

	return nil
}
