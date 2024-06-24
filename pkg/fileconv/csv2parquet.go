package fileconv

import (
	"context"
	"fmt"

	"github.com/hbbtekademy/go-fileconv/pkg/param/csvparam"
	"github.com/hbbtekademy/go-fileconv/pkg/param/pqparam"
)

// Convert csv files to parquet files
func (c *fileconv) Csv2Parquet(ctx context.Context, srcCsv string, dest string, pqWriteParams *pqparam.WriteParams, csvParams ...csvparam.ReadParam) error {
	csvReadParams := csvparam.NewReadParams(csvParams...)

	if csvReadParams.GetDescribe() {
		desc, err := c.describeCsv(ctx, srcCsv, csvReadParams)
		if err != nil {
			return err
		}

		fmt.Println(desc)
		return nil
	}

	_, err := c.db.ExecContext(ctx, fmt.Sprintf("COPY (SELECT * FROM read_csv('%s' %s)) TO '%s' %s",
		srcCsv,
		csvReadParams.Params(),
		dest,
		pqWriteParams.Params()))
	if err != nil {
		return err
	}

	return nil
}

func (c *fileconv) describeCsv(ctx context.Context, srcCsv string, csvReadParams *csvparam.ReadParams) (string, error) {
	table := fmt.Sprintf(`SELECT * FROM read_csv('%s' %s) USING SAMPLE %d`,
		srcCsv,
		csvReadParams.Params(),
		csvReadParams.GetSampleSize())

	tableDesc, err := c.GetTableDesc(ctx, table)
	if err != nil {
		return "", fmt.Errorf("failed getting csv desc. error: %v", err)
	}

	return tableDesc.String(), nil
}
