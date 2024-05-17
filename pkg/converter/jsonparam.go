package converter

type Compression string

const (
	None            Compression = "none"
	Gzip            Compression = "gzip"
	Zstd            Compression = "zstd"
	AutoCompression Compression = "auto"
)

type JsonFormat string

const (
	AutoFormat       JsonFormat = "auto"
	Unstructured     JsonFormat = "unstructured"
	NewlineDelimited JsonFormat = "newline_delimited"
	Array            JsonFormat = "array"
)

type JsonRecords string

const (
	AutoRecords JsonRecords = "auto"
	True        JsonRecords = "true"
	False       JsonRecords = "false"
)

type jsonParameters struct {
	autodetect       bool
	columns          map[string]string
	compression      Compression
	convStr2Int      bool
	dateformat       string
	filename         bool
	format           JsonFormat
	hivePartitioning bool
	ignoreErrors     bool
	maxDepth         int64
	maxObjSize       uint64
	records          JsonRecords
	sampleSize       uint64
	timestampformat  string
	unionByName      bool
}

type jsonParam func(*jsonParameters)

func WithAutoDetect(autodetect bool) jsonParam {
	return func(jp *jsonParameters) {
		jp.autodetect = autodetect
	}
}

func WithColumns(columns map[string]string) jsonParam {
	return func(jp *jsonParameters) {
		jp.columns = columns
	}
}

func WithCompression(compression Compression) jsonParam {
	return func(jp *jsonParameters) {
		jp.compression = compression
	}
}

func WithConvStr2Int(convInt2Str bool) jsonParam {
	return func(jp *jsonParameters) {
		jp.convStr2Int = convInt2Str
	}
}

func WithDateFormat(dateFormat string) jsonParam {
	return func(jp *jsonParameters) {
		jp.dateformat = dateFormat
	}
}

func WithFilename(filename bool) jsonParam {
	return func(jp *jsonParameters) {
		jp.filename = filename
	}
}

func WithFormat(format JsonFormat) jsonParam {
	return func(jp *jsonParameters) {
		jp.format = format
	}
}

func WithHivePartitioning(hivePartitioning bool) jsonParam {
	return func(jp *jsonParameters) {
		jp.hivePartitioning = hivePartitioning
	}
}

func WithIgnoreErrors(ignoreErrors bool) jsonParam {
	return func(jp *jsonParameters) {
		jp.ignoreErrors = ignoreErrors
	}
}

func WithMaxDepth(maxDepth int64) jsonParam {
	return func(jp *jsonParameters) {
		jp.maxDepth = maxDepth
	}
}

func WithMaxObjSize(maxObjSize uint64) jsonParam {
	return func(jp *jsonParameters) {
		jp.maxObjSize = maxObjSize
	}
}

func WithRecords(records JsonRecords) jsonParam {
	return func(jp *jsonParameters) {
		jp.records = records
	}
}

func WithSampleSize(sampleSize uint64) jsonParam {
	return func(jp *jsonParameters) {
		jp.sampleSize = sampleSize
	}
}

func WithtimestampFormat(timestampformat string) jsonParam {
	return func(jp *jsonParameters) {
		jp.timestampformat = timestampformat
	}
}

func WithUnionByName(unionByName bool) jsonParam {
	return func(jp *jsonParameters) {
		jp.unionByName = unionByName
	}
}

func getJsonParameters(params ...jsonParam) *jsonParameters {
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
