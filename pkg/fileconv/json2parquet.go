//go:build !windows

package fileconv

import (
	"context"
	"fmt"
	"time"

	"github.com/hbbtekademy/go-fileconv/pkg/model"
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

	tableDesc, err := c.GetTableDesc(ctx, jsonTableName)
	if err != nil {
		return fmt.Errorf("failed getting imported json table desc. error: %w", err)
	}

	flattenedColumns := []*model.ColumnDesc{}
	for i := range tableDesc.ColumnDescs {
		if tableDesc.ColumnDescs[i].ColType.IsStruct() {
			cols, err := c.FlattenStructColumn(ctx, tableDesc.ColumnDescs[i])
			if err != nil {
				return fmt.Errorf("failed flattening column: %s. error: %w",
					tableDesc.ColumnDescs[i].ColName, err)
			}
			flattenedColumns = append(flattenedColumns, cols...)
			continue
		}
		flattenedColumns = append(flattenedColumns, tableDesc.ColumnDescs[i])

	}

	unnestedCols, err := tableDesc.GetUnnestedColumns()
	if err != nil {
		return fmt.Errorf("failed getting unnested columns. error: %w", err)
	}

	unnestedTable := fmt.Sprintf("SELECT %s FROM %s", unnestedCols, jsonTableName)
	unnestedTableDesc, err := c.GetTableDesc(ctx, unnestedTable)
	if err != nil {
		return fmt.Errorf("failed getting unnested table desc. error: %w", err)
	}

	if len(unnestedTableDesc.ColumnDescs) != len(flattenedColumns) {
		return fmt.Errorf("unnested table columns and flattened columns not matching. unnested table cols: %v, flattened cols: %v",
			unnestedTableDesc.ColumnDescs, flattenedColumns)
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
