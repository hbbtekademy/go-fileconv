//go:build !windows

package fileconv

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hbbtekademy/go-fileconv/pkg/param/jsonparam"
	"github.com/hbbtekademy/go-fileconv/pkg/param/pqparam"
)

// Convert json files to parquet files
func (c *fileconv) Json2Parquet(ctx context.Context, srcJson string, dest string, pqWriteParams *pqparam.WriteParams, jsonParams ...jsonparam.ReadParam) error {
	jsonReadParams := jsonparam.NewReadParams(jsonParams...)

	if jsonReadParams.GetDescribe() {
		return c.describeJson(ctx, srcJson, jsonReadParams)
	}

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

	jsonTableName, err := c.ImportJson(ctx, srcJson, jsonReadParams, 0)
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

func (c *fileconv) ImportJson(ctx context.Context, srcJson string, jsonReadParams *jsonparam.ReadParams, sampleSize uint64) (string, error) {
	tableName := fmt.Sprintf("tmp_%d", time.Now().UnixNano())

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM read_json('%s' %s)`,
		tableName,
		srcJson,
		jsonReadParams.Params()))

	if sampleSize > 0 {
		sb.WriteString(fmt.Sprintf(" USING SAMPLE %d", sampleSize))
	}

	_, err := c.db.ExecContext(ctx, sb.String())
	if err != nil {
		return "", fmt.Errorf("failed importing json. error: %w", err)
	}

	return tableName, nil
}

func (c *fileconv) describeJson(ctx context.Context, srcJson string, jsonReadParams *jsonparam.ReadParams) error {
	if !jsonReadParams.GetFlatten() {
		table := fmt.Sprintf(`SELECT * FROM read_json('%s' %s) USING SAMPLE %d`,
			srcJson,
			jsonReadParams.Params(),
			jsonReadParams.GetSampleSize())

		tableDesc, err := c.GetTableDesc(ctx, table)
		if err != nil {
			return fmt.Errorf("failed getting json desc. error: %v", err)
		}

		fmt.Println(tableDesc)
		return nil
	}

	jsonTableName, err := c.ImportJson(ctx, srcJson, jsonReadParams, jsonReadParams.GetSampleSize())
	if err != nil {
		return fmt.Errorf("failed importing json. error: %w", err)
	}
	defer c.dropTable(ctx, jsonTableName)

	flattendTableSelect, err := c.getFlattenedTableSelect(ctx, jsonTableName)
	if err != nil {
		return fmt.Errorf("failed getting flattend table. error: %w", err)
	}

	tableDesc, err := c.GetTableDesc(ctx, flattendTableSelect)
	if err != nil {
		return fmt.Errorf("failed getting json desc. error: %v", err)
	}

	fmt.Println(tableDesc)
	return nil
}
