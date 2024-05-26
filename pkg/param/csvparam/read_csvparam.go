package csvparam

import (
	"fmt"
	"strings"

	"github.com/hbbtekademy/parquet-converter/pkg/param"
)

type Column struct {
	Name string
	Type string
}

type Columns []Column

func (c Columns) String() string {
	if len(c) == 0 {
		return ""
	}

	cols := []string{}

	for _, col := range c {
		cols = append(cols, fmt.Sprintf("'%s': '%s'", col.Name, col.Type))
	}

	var sb strings.Builder
	sb.WriteString("{")
	sb.WriteString(strings.Join(cols, ","))
	sb.WriteString("}")
	return sb.String()
}

// Parameters for reading a CSV file
type ReadParams struct {
	allVarchar         bool
	allowQuotedNulls   bool
	autoDetect         bool
	autoTypeCandidates []string
	columns            Columns
	compression        param.Compression
	dateformat         string
	decimalSeparator   string
	delim              string
	escape             string
	filename           bool
	forceNotNull       []string
	header             bool
	hivePartitioning   bool
	ignoreErrors       bool
	maxLineSize        int64
	names              []string
	newLine            string
	normalizeNames     bool
	nullPadding        bool
	nullStr            []string
	parallel           bool
	quote              string
	sampleSize         int64
	skip               int64
	timestampformat    string
	types              Columns
	unionByName        bool
}

type ReadParam func(*ReadParams)

const (
	dfltAllVarchar       bool              = false
	dfltAllowQuotedNulls bool              = true
	dfltAutoDetect       bool              = true
	dfltCompression      param.Compression = param.AutoCompression
	dfltDateformat       string            = ""
	dfltDecimalSeparator string            = "."
	dfltDelim            string            = ","
	dfltEscape           string            = `"`
	dfltFilename         bool              = false
	dfltHeader           bool              = false
	dfltHivePartitioning bool              = false
	dfltIgnoreErrors     bool              = false
	dfltMaxLineSize      int64             = 2097152
	dfltNewLine          string            = ""
	dfltNormalizeNames   bool              = false
	dfltNullPadding      bool              = false
	dfltParallel         bool              = false
	dfltQuote            string            = `"`
	dfltSampleSize       int64             = 20480
	dfltSkip             int64             = 0
	dfltTimestampformat  string            = ""
	dfltUnionByName      bool              = false
)

func WithAllVarchar(allVarchar bool) ReadParam {
	return func(rp *ReadParams) {
		rp.allVarchar = allVarchar
	}
}

func WithAllowQuotedNulls(allowQuotedNulls bool) ReadParam {
	return func(rp *ReadParams) {
		rp.allowQuotedNulls = allowQuotedNulls
	}
}

func WithAutoDetect(autodetect bool) ReadParam {
	return func(jp *ReadParams) {
		jp.autoDetect = autodetect
	}
}

func WithAutoTypeCandidates(autoTypeCandidates []string) ReadParam {
	return func(rp *ReadParams) {
		rp.autoTypeCandidates = autoTypeCandidates
	}
}

func WithColumns(columns Columns) ReadParam {
	return func(rp *ReadParams) {
		rp.columns = columns
	}
}

func WithCompression(compression param.Compression) ReadParam {
	return func(rp *ReadParams) {
		rp.compression = compression
	}
}

func WithDateformat(dateformat string) ReadParam {
	return func(rp *ReadParams) {
		rp.dateformat = dateformat
	}
}

func WithDecimalSeparator(decimalSeparator string) ReadParam {
	return func(rp *ReadParams) {
		rp.decimalSeparator = decimalSeparator
	}
}

func WithDelim(delim string) ReadParam {
	return func(rp *ReadParams) {
		rp.delim = delim
	}
}

func WithEscape(escape string) ReadParam {
	return func(rp *ReadParams) {
		rp.escape = escape
	}
}

func WithFilename(filename bool) ReadParam {
	return func(rp *ReadParams) {
		rp.filename = filename
	}
}

func WithForceNotNull(forceNotNull []string) ReadParam {
	return func(rp *ReadParams) {
		rp.forceNotNull = forceNotNull
	}
}

func WithHeader(header bool) ReadParam {
	return func(rp *ReadParams) {
		rp.header = header
	}
}

func WithHivePartitioning(hivePartitioning bool) ReadParam {
	return func(rp *ReadParams) {
		rp.hivePartitioning = hivePartitioning
	}
}

func WithIgnoreErrors(ignoreErrors bool) ReadParam {
	return func(rp *ReadParams) {
		rp.ignoreErrors = ignoreErrors
	}
}

func WithMaxLineSize(maxLineSize int64) ReadParam {
	return func(rp *ReadParams) {
		rp.maxLineSize = maxLineSize
	}
}

func WithNames(names []string) ReadParam {
	return func(rp *ReadParams) {
		rp.names = names
	}
}

func WithNewLine(newLine string) ReadParam {
	return func(rp *ReadParams) {
		rp.newLine = newLine
	}
}

func WithNormalizeNames(normalizeNames bool) ReadParam {
	return func(rp *ReadParams) {
		rp.normalizeNames = normalizeNames
	}
}

func WithNullPadding(nullPadding bool) ReadParam {
	return func(rp *ReadParams) {
		rp.nullPadding = nullPadding
	}
}

func WithNullStrings(nullStr []string) ReadParam {
	return func(rp *ReadParams) {
		rp.nullStr = nullStr
	}
}

func WithParallel(parallel bool) ReadParam {
	return func(rp *ReadParams) {
		rp.parallel = parallel
	}
}

func WithQuote(quote string) ReadParam {
	return func(rp *ReadParams) {
		rp.quote = quote
	}
}

func WithSampleSize(sampleSize int64) ReadParam {
	return func(rp *ReadParams) {
		rp.sampleSize = sampleSize
	}
}

func WithSkip(skip int64) ReadParam {
	return func(rp *ReadParams) {
		rp.skip = skip
	}
}

func WithTimestampFormat(timestampformat string) ReadParam {
	return func(rp *ReadParams) {
		rp.timestampformat = timestampformat
	}
}

func WithTypes(types Columns) ReadParam {
	return func(rp *ReadParams) {
		rp.types = types
	}
}

func WithUnionByName(unionByName bool) ReadParam {
	return func(rp *ReadParams) {
		rp.unionByName = unionByName
	}
}

// https://duckdb.org/docs/data/csv/overview#parameters
func NewReadParams(params ...ReadParam) *ReadParams {
	csvReadParams := &ReadParams{
		allVarchar:         dfltAllVarchar,
		allowQuotedNulls:   dfltAllowQuotedNulls,
		autoDetect:         dfltAutoDetect,
		autoTypeCandidates: []string{},
		columns:            Columns{},
		compression:        dfltCompression,
		dateformat:         dfltDateformat,
		decimalSeparator:   dfltDecimalSeparator,
		delim:              dfltDelim,
		escape:             dfltEscape,
		filename:           dfltFilename,
		forceNotNull:       []string{},
		header:             dfltHeader,
		hivePartitioning:   dfltHivePartitioning,
		ignoreErrors:       dfltIgnoreErrors,
		maxLineSize:        dfltMaxLineSize,
		names:              []string{},
		newLine:            dfltNewLine,
		normalizeNames:     dfltNormalizeNames,
		nullPadding:        dfltNullPadding,
		nullStr:            []string{},
		parallel:           dfltParallel,
		quote:              dfltQuote,
		sampleSize:         dfltSampleSize,
		skip:               dfltSkip,
		timestampformat:    dfltTimestampformat,
		types:              Columns{},
		unionByName:        dfltUnionByName,
	}

	for _, param := range params {
		param(csvReadParams)
	}

	return csvReadParams
}

// Returns formatted parameters for reading CSV file
func (p *ReadParams) Params() string {
	params := []string{}

	if p.allVarchar {
		params = append(params, "all_varchar = true")
	}
	if !p.allowQuotedNulls {
		params = append(params, "allow_quoted_nulls = false")
	}
	if !p.autoDetect {
		params = append(params, "auto_detect = false")
	}
	if len(p.autoTypeCandidates) > 0 {
		params = append(params, fmt.Sprintf("auto_type_candidates = ['%s']", strings.Join(p.autoTypeCandidates, "','")))
	}
	if len(p.columns) > 0 {
		params = append(params, fmt.Sprintf("columns = %s", p.columns))
	}
	if p.compression != dfltCompression {
		params = append(params, fmt.Sprintf("compression = '%s'", p.compression))
	}
	if p.dateformat != dfltDateformat {
		params = append(params, fmt.Sprintf("dateformat = '%s'", p.dateformat))
	}
	if p.decimalSeparator != dfltDecimalSeparator {
		params = append(params, fmt.Sprintf("decimal_separator = '%s'", p.decimalSeparator))
	}
	if p.delim != dfltDelim {
		params = append(params, fmt.Sprintf("delim = '%s'", p.delim))
	}
	if p.escape != dfltEscape {
		params = append(params, fmt.Sprintf("escape = '%s'", p.escape))
	}
	if p.filename {
		params = append(params, "filename = true")
	}
	if len(p.forceNotNull) > 0 {
		params = append(params, fmt.Sprintf("force_not_null = ['%s']", strings.Join(p.forceNotNull, "','")))
	}
	if p.header {
		params = append(params, "header = true")
	}
	if p.hivePartitioning {
		params = append(params, "hive_partitioning = true")
	}
	if p.ignoreErrors {
		params = append(params, "ignore_errors = true")
	}
	if p.maxLineSize != dfltMaxLineSize {
		params = append(params, fmt.Sprintf("max_line_size = %d", p.maxLineSize))
	}
	if len(p.names) > 0 {
		params = append(params, fmt.Sprintf("names = ['%s']", strings.Join(p.names, "','")))
	}
	if p.newLine != dfltNewLine {
		params = append(params, fmt.Sprintf("new_line = '%s'", p.newLine))
	}
	if p.normalizeNames {
		params = append(params, "normalize_names = true")
	}
	if p.nullPadding {
		params = append(params, "null_padding = true")
	}
	if len(p.nullStr) > 0 {
		params = append(params, fmt.Sprintf("nullstr = ['%s']", strings.Join(p.nullStr, "','")))
	}
	if p.parallel {
		params = append(params, "parallel = true")
	}
	if p.quote != dfltQuote {
		params = append(params, fmt.Sprintf("quote = '%s'", p.quote))
	}
	if p.sampleSize != dfltSampleSize {
		params = append(params, fmt.Sprintf("sample_size = %d", p.sampleSize))
	}
	if p.skip != dfltSkip {
		params = append(params, fmt.Sprintf("skip = %d", p.skip))
	}
	if p.timestampformat != dfltTimestampformat {
		params = append(params, fmt.Sprintf("timestampformat = '%s'", p.timestampformat))
	}
	if len(p.types) > 0 {
		params = append(params, fmt.Sprintf("types = %s", p.types))
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
