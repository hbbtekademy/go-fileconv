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
