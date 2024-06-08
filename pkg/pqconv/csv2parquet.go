package pqconv

import (
	"context"
	"fmt"

	"github.com/hbbtekademy/go-fileconv/pkg/param/csvparam"
	"github.com/hbbtekademy/go-fileconv/pkg/param/pqparam"
)

// Convert csv files to parquet files
func (c *pqconv) Csv2Parquet(ctx context.Context, srcJson string, dest string, pqWriteParams *pqparam.WriteParams, csvParams ...csvparam.ReadParam) error {
	csvReadParams := csvparam.NewReadParams(csvParams...)

	_, err := c.db.ExecContext(ctx, fmt.Sprintf("COPY (SELECT * FROM read_csv('%s' %s)) TO '%s' %s",
		srcJson,
		csvReadParams.Params(),
		dest,
		pqWriteParams.Params()))
	if err != nil {
		return err
	}

	return nil
}
