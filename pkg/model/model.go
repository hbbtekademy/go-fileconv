package model

import "strings"

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
