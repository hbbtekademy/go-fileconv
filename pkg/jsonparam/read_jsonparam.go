package jsonparam

import (
	"fmt"
	"strings"
)

type Compression string

const (
	None            Compression = "none"
	Gzip            Compression = "gzip"
	Zstd            Compression = "zstd"
	AutoCompression Compression = "auto"
)

type Format string

const (
	AutoFormat       Format = "auto"
	Unstructured     Format = "unstructured"
	NewlineDelimited Format = "newline_delimited"
	Array            Format = "array"
)

type Records string

const (
	AutoRecords Records = "auto"
	True        Records = "true"
	False       Records = "false"
)

type ReadParams struct {
	autodetect       bool
	columns          map[string]string
	compression      Compression
	convStr2Int      bool
	dateformat       string
	filename         bool
	format           Format
	hivePartitioning bool
	ignoreErrors     bool
	maxDepth         int64
	maxObjSize       uint64
	records          Records
	sampleSize       uint64
	timestampformat  string
	unionByName      bool
}

type ReadParam func(*ReadParams)

const (
	dfltAutodetect      bool        = true
	dfltCompression     Compression = AutoCompression
	dfltConvStr2Int     bool        = false
	dfltDateFormat      string      = "iso"
	dfltFilename        bool        = false
	dfltFormat          Format      = Array
	dfltHivePartition   bool        = false
	dfltIgnoreErrors    bool        = false
	dfltMaxDept         int64       = -1
	dfltMaxObjSize      uint64      = 16777216
	dfltRecords         Records     = AutoRecords
	dfltSampleSize      uint64      = 20480
	dfltTimestampFormat string      = "iso"
	dfltUnionByName     bool        = false
)

func WithAutoDetect(autodetect bool) ReadParam {
	return func(jp *ReadParams) {
		jp.autodetect = autodetect
	}
}

func WithColumns(columns map[string]string) ReadParam {
	return func(jp *ReadParams) {
		jp.columns = columns
	}
}

func WithCompression(compression Compression) ReadParam {
	return func(jp *ReadParams) {
		jp.compression = compression
	}
}

func WithConvStr2Int(convInt2Str bool) ReadParam {
	return func(jp *ReadParams) {
		jp.convStr2Int = convInt2Str
	}
}

func WithDateFormat(dateFormat string) ReadParam {
	return func(jp *ReadParams) {
		jp.dateformat = dateFormat
	}
}

func WithFilename(filename bool) ReadParam {
	return func(jp *ReadParams) {
		jp.filename = filename
	}
}

func WithFormat(format Format) ReadParam {
	return func(jp *ReadParams) {
		jp.format = format
	}
}

func WithHivePartitioning(hivePartitioning bool) ReadParam {
	return func(jp *ReadParams) {
		jp.hivePartitioning = hivePartitioning
	}
}

func WithIgnoreErrors(ignoreErrors bool) ReadParam {
	return func(jp *ReadParams) {
		jp.ignoreErrors = ignoreErrors
	}
}

func WithMaxDepth(maxDepth int64) ReadParam {
	return func(jp *ReadParams) {
		jp.maxDepth = maxDepth
	}
}

func WithMaxObjSize(maxObjSize uint64) ReadParam {
	return func(jp *ReadParams) {
		jp.maxObjSize = maxObjSize
	}
}

func WithRecords(records Records) ReadParam {
	return func(jp *ReadParams) {
		jp.records = records
	}
}

func WithSampleSize(sampleSize uint64) ReadParam {
	return func(jp *ReadParams) {
		jp.sampleSize = sampleSize
	}
}

func WithTimestampFormat(timestampformat string) ReadParam {
	return func(jp *ReadParams) {
		jp.timestampformat = timestampformat
	}
}

func WithUnionByName(unionByName bool) ReadParam {
	return func(jp *ReadParams) {
		jp.unionByName = unionByName
	}
}

func NewReadParams(params ...ReadParam) *ReadParams {
	jsonParams := &ReadParams{
		autodetect:       dfltAutodetect,
		columns:          make(map[string]string),
		compression:      dfltCompression,
		convStr2Int:      dfltConvStr2Int,
		dateformat:       dfltDateFormat,
		filename:         dfltFilename,
		format:           dfltFormat,
		hivePartitioning: dfltHivePartition,
		ignoreErrors:     dfltIgnoreErrors,
		maxDepth:         dfltMaxDept,
		maxObjSize:       dfltMaxObjSize,
		records:          dfltRecords,
		sampleSize:       dfltSampleSize,
		timestampformat:  dfltTimestampFormat,
		unionByName:      dfltUnionByName,
	}

	for _, param := range params {
		param(jsonParams)
	}

	return jsonParams
}

func (p *ReadParams) Params() string {
	params := []string{}

	if !p.autodetect {
		params = append(params, "auto_detect = false")
	}

	if p.compression != dfltCompression {
		params = append(params, fmt.Sprintf("compression = '%s'", p.compression))
	}

	if p.convStr2Int {
		params = append(params, "convert_strings_to_integers = true")
	}

	if p.dateformat != dfltDateFormat {
		params = append(params, fmt.Sprintf("dateformat = '%s'", p.dateformat))
	}

	if p.filename {
		params = append(params, "filename = true")
	}

	if p.format != dfltFormat {
		params = append(params, fmt.Sprintf("format = '%s'", p.format))
	}

	if p.hivePartitioning {
		params = append(params, "hive_partitioning = true")
	}

	if p.ignoreErrors {
		params = append(params, "ignore_errors = true")
	}

	if p.maxDepth != dfltMaxDept {
		params = append(params, fmt.Sprintf("maximum_depth = %d", p.maxDepth))
	}

	if p.maxObjSize != dfltMaxObjSize {
		params = append(params, fmt.Sprintf("maximum_object_size = %d", p.maxObjSize))
	}

	if p.records != dfltRecords {
		params = append(params, fmt.Sprintf("records = '%s'", p.records))
	}

	if p.sampleSize != dfltSampleSize {
		params = append(params, fmt.Sprintf("sample_size = %d", p.sampleSize))
	}

	if p.timestampformat != dfltTimestampFormat {
		params = append(params, fmt.Sprintf("timestampformat = '%s'", p.timestampformat))
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
