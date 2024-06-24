package model

import (
	"fmt"
	"strings"
)

type ColumnType string

func (ct ColumnType) IsStruct() bool {
	str := strings.ToUpper(string(ct))
	return strings.HasPrefix(str, "STRUCT(") && strings.HasSuffix(str, ")")
}

type ColumnDesc struct {
	ColName string
	ColType ColumnType
}

type TableDesc struct {
	ColumnDescs []*ColumnDesc
}

func (t *TableDesc) GetUnnestedColumns() (string, error) {
	l := len(t.ColumnDescs)
	var sb strings.Builder
	for i := range t.ColumnDescs {
		var err error
		if t.ColumnDescs[i].ColType.IsStruct() {
			_, err = sb.WriteString(fmt.Sprintf("unnest(%s, recursive := true)", t.ColumnDescs[i].ColName))
		} else {
			_, err = sb.WriteString(t.ColumnDescs[i].ColName)
		}
		if err != nil {
			return "", err
		}

		if i < l-1 {
			_, err := sb.WriteRune(',')
			if err != nil {
				return "", err
			}
		}
	}

	return sb.String(), nil
}

func (t *TableDesc) String() string {
	var sb strings.Builder
	maxColLen, maxColTypeLen := t.getMaxLen()

	formatter := fmt.Sprintf("%%-%ds| %%-%ds\n", maxColLen+5, maxColTypeLen+5)
	sb.WriteString(fmt.Sprintf(formatter, "COLUMN NAME", "COLUMN TYPE"))
	sb.WriteString(fmt.Sprintf("%s|%s\n", strings.Repeat("=", maxColLen+5), strings.Repeat("=", maxColTypeLen+5)))

	for i := range t.ColumnDescs {
		sb.WriteString(fmt.Sprintf(formatter, t.ColumnDescs[i].ColName, t.ColumnDescs[i].ColType))
	}

	return sb.String()
}

func (t *TableDesc) getMaxLen() (int, int) {
	maxColName := 0
	maxColType := 0
	colDescs := t.ColumnDescs

	for i := range colDescs {
		if len(colDescs[i].ColName) > maxColName {
			maxColName = len(colDescs[i].ColName)
		}
		if len(colDescs[i].ColType) > maxColType {
			maxColType = len(colDescs[i].ColType)
		}
	}

	return maxColName, maxColType
}
