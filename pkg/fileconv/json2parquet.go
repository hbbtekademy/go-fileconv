//go:build !windows

package fileconv

import (
	"context"
	"fmt"
	"time"

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
			return fmt.Errorf("failed converting json to parquet. error: %w", err)
		}

		return nil
	}

	// Flatten json and export

	jsonTableName, err := c.ImportJson(ctx, srcJson, jsonReadParams)
	if err != nil {
		return fmt.Errorf("failed importing json. error: %w", err)
	}
	defer c.dropTable(ctx, jsonTableName)

	flattendTableSelect, err := c.getFlattenedTableSelect(ctx, jsonTableName)
	if err != nil {
		return fmt.Errorf("failed getting flattend table. error: %w", err)
	}

	_, err = c.db.ExecContext(ctx, fmt.Sprintf(`
		COPY (
			%s
		) TO '%s' %s`,
		flattendTableSelect,
		dest,
		pqWriteParams.Params()))
	if err != nil {
		return fmt.Errorf("failed converting flattened json to parquet. error: %w", err)
	}

	return nil
}

func (c *fileconv) ImportJson(ctx context.Context, srcJson string, jsonReadParams *jsonparam.ReadParams) (string, error) {
	tableName := fmt.Sprintf("tmp_%d", time.Now().UnixNano())

	_, err := c.db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM read_json('%s' %s)`,
		tableName,
		srcJson,
		jsonReadParams.Params()))
	if err != nil {
		return "", fmt.Errorf("failed importing json. error: %w", err)
	}

	return tableName, nil
}
