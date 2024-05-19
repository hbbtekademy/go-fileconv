package jsonparam

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

type jsonParameters struct {
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

type Param func(*jsonParameters)

func WithAutoDetect(autodetect bool) Param {
	return func(jp *jsonParameters) {
		jp.autodetect = autodetect
	}
}

func WithColumns(columns map[string]string) Param {
	return func(jp *jsonParameters) {
		jp.columns = columns
	}
}

func WithCompression(compression Compression) Param {
	return func(jp *jsonParameters) {
		jp.compression = compression
	}
}

func WithConvStr2Int(convInt2Str bool) Param {
	return func(jp *jsonParameters) {
		jp.convStr2Int = convInt2Str
	}
}

func WithDateFormat(dateFormat string) Param {
	return func(jp *jsonParameters) {
		jp.dateformat = dateFormat
	}
}

func WithFilename(filename bool) Param {
	return func(jp *jsonParameters) {
		jp.filename = filename
	}
}

func WithFormat(format Format) Param {
	return func(jp *jsonParameters) {
		jp.format = format
	}
}

func WithHivePartitioning(hivePartitioning bool) Param {
	return func(jp *jsonParameters) {
		jp.hivePartitioning = hivePartitioning
	}
}

func WithIgnoreErrors(ignoreErrors bool) Param {
	return func(jp *jsonParameters) {
		jp.ignoreErrors = ignoreErrors
	}
}

func WithMaxDepth(maxDepth int64) Param {
	return func(jp *jsonParameters) {
		jp.maxDepth = maxDepth
	}
}

func WithMaxObjSize(maxObjSize uint64) Param {
	return func(jp *jsonParameters) {
		jp.maxObjSize = maxObjSize
	}
}

func WithRecords(records Records) Param {
	return func(jp *jsonParameters) {
		jp.records = records
	}
}

func WithSampleSize(sampleSize uint64) Param {
	return func(jp *jsonParameters) {
		jp.sampleSize = sampleSize
	}
}

func WithTimestampFormat(timestampformat string) Param {
	return func(jp *jsonParameters) {
		jp.timestampformat = timestampformat
	}
}

func WithUnionByName(unionByName bool) Param {
	return func(jp *jsonParameters) {
		jp.unionByName = unionByName
	}
}

func New(params ...Param) *jsonParameters {
	jsonParams := &jsonParameters{
		columns:         make(map[string]string),
		compression:     AutoCompression,
		dateformat:      "iso",
		format:          Array,
		maxDepth:        -1,
		maxObjSize:      16777216,
		records:         True,
		sampleSize:      20480,
		timestampformat: "iso",
	}

	for _, param := range params {
		param(jsonParams)
	}

	return jsonParams
}
