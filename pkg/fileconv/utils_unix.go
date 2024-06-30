//go:build !windows

package fileconv

import (
	"context"
	"fmt"
	"time"

	"github.com/hbbtekademy/go-fileconv/pkg/model"
)

func (c *fileconv) GetTableDesc(ctx context.Context, table string) (*model.TableDesc, error) {
	rows, err := c.db.QueryContext(ctx, getDescribeQuery(table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := []*model.ColumnDesc{}
	for rows.Next() {
		var colName, colType string
		if err := rows.Scan(&colName, &colType); err != nil {
			return nil, err
		}
		columnDesc := &model.ColumnDesc{
			ColName: colName,
			ColType: model.ColumnType(colType),
		}
		columns = append(columns, columnDesc)
	}

	return &model.TableDesc{ColumnDescs: columns}, nil
}

func (c *fileconv) createStructColTable(ctx context.Context, columnDesc *model.ColumnDesc) (string, error) {
	tableName := fmt.Sprintf("%s_tmp_%d", columnDesc.ColName, time.Now().UnixNano())
	_, err := c.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (C1 %s)", tableName, columnDesc.ColType))
	if err != nil {
		return "", err
	}

	return tableName, nil
}

func (c *fileconv) dropTable(ctx context.Context, tableName string) error {
	_, err := c.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", tableName))
	return err
}

func (c *fileconv) executeCmd(ctx context.Context, cmd string) error {
	_, err := c.db.ExecContext(ctx, cmd)
	if err != nil {
		return err
	}

	return nil
}
