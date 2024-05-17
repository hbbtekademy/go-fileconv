package converter

import (
	"context"
	"fmt"

	"github.com/hbbtekademy/parquet-converter/pkg/util"
)

func (d *ddbClient) Json2Parquet(ctx context.Context, srcJsonFilepath string, destDirpath string, params ...jsonParam) error {
	err := util.ValidateFilepath(srcJsonFilepath)
	if err != nil {
		return err
	}

	err = util.ValidateDirpath(destDirpath)
	if err != nil {
		return err
	}

	_ = getJsonParameters(params...)

	_, err = d.db.ExecContext(ctx, fmt.Sprintf("COPY (SELECT * FROM read_json('%s')) TO '%s' (FORMAT 'parquet')", srcJsonFilepath, destDirpath))
	if err != nil {
		return err
	}

	return nil
}
