package jsonparam

import (
	"fmt"
	"strings"

	"github.com/hbbtekademy/parquet-converter/pkg/param"
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

// Parameters for reading a JSON file
type ReadParams struct {
	autodetect       bool
	columns          param.Columns
	compression      param.Compression
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
	dfltAutodetect      bool              = true
	dfltCompression     param.Compression = param.AutoCompression
	dfltConvStr2Int     bool              = false
	dfltDateFormat      string            = "iso"
	dfltFilename        bool              = false
	dfltFormat          Format            = Array
	dfltHivePartition   bool              = false
	dfltIgnoreErrors    bool              = false
	dfltMaxDept         int64             = -1
	dfltMaxObjSize      uint64            = 16777216
	dfltRecords         Records           = AutoRecords
	dfltSampleSize      uint64            = 20480
	dfltTimestampFormat string            = "iso"
	dfltUnionByName     bool              = false
)

/*
Whether to auto-detect the names of the keys and data types of the values automatically.
Default true

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithAutoDetect(autodetect bool) ReadParam {
	return func(jp *ReadParams) {
		jp.autodetect = autodetect
	}
}

/*
A map of key names and value types contained within the JSON file. (e.g., key1: 'INTEGER', key2: 'VARCHAR').
If auto_detect is enabled these will be inferred.
Default empty

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithColumns(columns param.Columns) ReadParam {
	return func(jp *ReadParams) {
		jp.columns = columns
	}
}

/*
The compression type for the file.
By default this will be detected automatically from the file extension.

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithCompression(compression param.Compression) ReadParam {
	return func(jp *ReadParams) {
		jp.compression = compression
	}
}

/*
Whether strings representing integer values should be converted to a numerical type.
Default false

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithConvStr2Int(convInt2Str bool) ReadParam {
	return func(jp *ReadParams) {
		jp.convStr2Int = convInt2Str
	}
}

/*
Specifies the date format to use when parsing dates.
Default 'iso'

https://duckdb.org/docs/sql/functions/dateformat
*/
func WithDateFormat(dateFormat string) ReadParam {
	return func(jp *ReadParams) {
		jp.dateformat = dateFormat
	}
}

/*
Whether or not an extra filename column should be included in the result.
Default false

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithFilename(filename bool) ReadParam {
	return func(jp *ReadParams) {
		jp.filename = filename
	}
}

/*
Can be one of ('auto', 'unstructured', 'newline_delimited', 'array').
Default 'auto'

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithFormat(format Format) ReadParam {
	return func(jp *ReadParams) {
		jp.format = format
	}
}

/*
Whether or not to interpret the path as a Hive partitioned path.
Default false

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithHivePartitioning(hivePartitioning bool) ReadParam {
	return func(jp *ReadParams) {
		jp.hivePartitioning = hivePartitioning
	}
}

/*
Whether to ignore parse errors (only possible when format is 'newline_delimited').
Default false

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithIgnoreErrors(ignoreErrors bool) ReadParam {
	return func(jp *ReadParams) {
		jp.ignoreErrors = ignoreErrors
	}
}

/*
Maximum nesting depth to which the automatic schema detection detects types.
Set to -1 to fully detect nested JSON types.

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithMaxDepth(maxDepth int64) ReadParam {
	return func(jp *ReadParams) {
		jp.maxDepth = maxDepth
	}
}

/*
The maximum size of a JSON object (in bytes).
Default 16777216 bytes

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithMaxObjSize(maxObjSize uint64) ReadParam {
	return func(jp *ReadParams) {
		jp.maxObjSize = maxObjSize
	}
}

/*
Can be one of ('auto', 'true', 'false').
Default 'auto'

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithRecords(records Records) ReadParam {
	return func(jp *ReadParams) {
		jp.records = records
	}
}

/*
Option to define number of sample objects for automatic JSON type detection.
Set to -1 to scan the entire input file
Default 20480

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithSampleSize(sampleSize uint64) ReadParam {
	return func(jp *ReadParams) {
		jp.sampleSize = sampleSize
	}
}

/*
Specifies the date format to use when parsing timestamps.
Default 'iso'

https://duckdb.org/docs/sql/functions/dateformat
*/
func WithTimestampFormat(timestampformat string) ReadParam {
	return func(jp *ReadParams) {
		jp.timestampformat = timestampformat
	}
}

/*
Whether the schema’s of multiple JSON files should be unified.
Default false

https://duckdb.org/docs/data/json/overview#parameters
*/
func WithUnionByName(unionByName bool) ReadParam {
	return func(jp *ReadParams) {
		jp.unionByName = unionByName
	}
}

// https://duckdb.org/docs/data/json/overview#parameters
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

// Returns formatted parameters for reading JSON file
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

	if len(p.columns) > 0 {
		params = append(params, fmt.Sprintf("columns = %s", p.columns))
	}

	prefix := ""
	if len(params) > 0 {
		prefix = ","
	}

	return prefix + strings.Join(params, ",")
}
