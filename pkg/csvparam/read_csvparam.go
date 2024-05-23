package csvparam

type Compression string

const (
	None            Compression = "none"
	Gzip            Compression = "gzip"
	Zstd            Compression = "zstd"
	AutoCompression Compression = "auto"
)

type Columns map[string]string

// Parameters for reading a CSV file
type ReadParams struct {
	allVarchar         bool
	allowQuotedNulls   bool
	autoDetect         bool
	autoTypeCandidates []string
	columns            Columns
	compression        Compression
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
	types              []string
	unionByName        bool
}

type ReadParam func(*ReadParams)

const (
	dfltAllVarchar       bool        = false
	dfltAllowQuotedNulls bool        = true
	dfltAutoDetect       bool        = true
	dfltCompression      Compression = AutoCompression
	dfltDateformat       string      = ""
	dfltDecimalSeparator string      = "."
	dfltDelim            string      = ","
	dfltEscape           string      = `"`
	dfltFilename         bool        = false
	dfltHeader           bool        = false
	dfltHivePartitioning bool        = false
	dfltIgnoreErrors     bool        = false
	dfltMaxLineSize      int64       = 2097152
	dfltNewLine          string      = ""
	dfltNormalizeNames   bool        = false
	dfltNullPadding      bool        = false
	dfltParallel         bool        = false
	dfltQuote            string      = `"`
	dfltSampleSize       int64       = 20480
	dfltSkip             int64       = 0
	dfltTimestampformat  string      = ""
	dfltUnionByName      bool        = false
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

func WithautoTypeCandidates(autoTypeCandidates []string) ReadParam {
	return func(rp *ReadParams) {
		rp.autoTypeCandidates = autoTypeCandidates
	}
}

func WithColumns(columns Columns) ReadParam {
	return func(rp *ReadParams) {
		rp.columns = columns
	}
}

func WithCompression(compression Compression) ReadParam {
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

func WithTimestampformat(timestampformat string) ReadParam {
	return func(rp *ReadParams) {
		rp.timestampformat = timestampformat
	}
}

func WithTypes(types []string) ReadParam {
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
		columns:            make(Columns),
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
		types:              []string{},
		unionByName:        dfltUnionByName,
	}

	for _, param := range params {
		param(csvReadParams)
	}

	return csvReadParams
}

// Returns formatted parameters for reading CSV file
func (p *ReadParams) Params() string {
	return ""
}
