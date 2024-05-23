package pqparam

import (
	"strings"
)

type ReadParams struct {
	binaryAsString bool
	filename       bool
	fileRowNum     bool
	unionByName    bool
	hivePartition  bool
}

type ReadParam func(*ReadParams)

const (
	dfltBinaryAsString bool = false
	dfltFilename       bool = false
	dfltFileRowNum     bool = false
	dfltUnionByName    bool = false
	dfltHivePartition  bool = false
)

func WithBinaryAsString(binaryAsString bool) ReadParam {
	return func(p *ReadParams) {
		p.binaryAsString = binaryAsString
	}
}

func WithFilename(filename bool) ReadParam {
	return func(p *ReadParams) {
		p.filename = filename
	}
}

func WithFileRowNum(fileRowNum bool) ReadParam {
	return func(p *ReadParams) {
		p.fileRowNum = fileRowNum
	}
}

func WithUnionByName(unionByName bool) ReadParam {
	return func(p *ReadParams) {
		p.unionByName = unionByName
	}
}

func WithHivePartition(hivePartition bool) ReadParam {
	return func(p *ReadParams) {
		p.hivePartition = hivePartition
	}
}

func NewReadParams(params ...ReadParam) *ReadParams {
	pqParameters := &ReadParams{
		binaryAsString: dfltBinaryAsString,
		filename:       dfltFilename,
		fileRowNum:     dfltFileRowNum,
		unionByName:    dfltUnionByName,
		hivePartition:  dfltHivePartition,
	}

	for _, param := range params {
		param(pqParameters)
	}

	return pqParameters
}

func (p *ReadParams) Params() string {
	params := []string{}

	if p.binaryAsString {
		params = append(params, "binary_as_string = true")
	}

	if p.fileRowNum {
		params = append(params, "file_row_number = true")
	}

	if p.filename {
		params = append(params, "filename = true")
	}

	if p.hivePartition {
		params = append(params, "hive_partitioning = true")
	}

	if p.unionByName {
		params = append(params, "union_by_name = true")
	}

	prefix := ""
	if len(params) > 0 {
		prefix = ","
	}
	return prefix + strings.Join(params, ",")
}
