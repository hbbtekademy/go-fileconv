package pqparam

type Compression string

const (
	Snappy Compression = "snappy"
	Zstd   Compression = "zstd"
	Gzip   Compression = "gzip"
	LZ4    Compression = "LZ4"
)

type hivePartitionConfig struct {
	partitionBy       []string
	overwriteOrIgnore int8
	filenamePattern   string
}

type HivePartitionOption func(*hivePartitionConfig)

func WithPartitionBy(partitionBy []string) HivePartitionOption {
	return func(hpc *hivePartitionConfig) {
		hpc.partitionBy = partitionBy
	}
}

func WithOverwriteOrIgnore(overwriteOrIgnore bool) HivePartitionOption {
	return func(hpc *hivePartitionConfig) {
		hpc.overwriteOrIgnore = 0
		if overwriteOrIgnore {
			hpc.overwriteOrIgnore = 1
		}
	}
}

func WithFilenamePattern(filenamePattern string) HivePartitionOption {
	return func(hpc *hivePartitionConfig) {
		hpc.filenamePattern = filenamePattern
	}
}

type Params struct {
	compression         Compression
	rowGroupSize        int64
	binaryAsString      bool
	filename            bool
	fileRowNum          bool
	unionByName         bool
	hivePartition       bool
	hivePartitionConfig *hivePartitionConfig
}

type Param func(*Params)

const (
	DfltCompression    Compression = "zstd"
	DfltRowGroupSize   int64       = 100000
	DfltBinaryAsString bool        = false
	DfltFilename       bool        = false
	DfltFileRowNum     bool        = false
	DfltUnionyName     bool        = false
	DfltHivePartition  bool        = false
)

var (
	DfltHivePartitionConfig *hivePartitionConfig = &hivePartitionConfig{
		partitionBy:       []string{},
		overwriteOrIgnore: 0,
		filenamePattern:   "data_{i}",
	}
)

func WithCompression(compression Compression) Param {
	return func(p *Params) {
		p.compression = compression
	}
}

func WithRowGroupSize(rowGroupSize int64) Param {
	return func(p *Params) {
		p.rowGroupSize = rowGroupSize
	}
}

func WithBinaryAsString(binaryAsString bool) Param {
	return func(p *Params) {
		p.binaryAsString = binaryAsString
	}
}

func WithFilename(filename bool) Param {
	return func(p *Params) {
		p.filename = filename
	}
}

func WithFileRowNum(fileRowNum bool) Param {
	return func(p *Params) {
		p.fileRowNum = fileRowNum
	}
}

func WithUnionByName(unionByName bool) Param {
	return func(p *Params) {
		p.unionByName = unionByName
	}
}

func WithHivePartition(hivePartition bool) Param {
	return func(p *Params) {
		p.hivePartition = hivePartition
	}
}

func WithHivePartitionConfig(options ...HivePartitionOption) Param {
	return func(p *Params) {
		config := DfltHivePartitionConfig
		for _, opt := range options {
			opt(config)
		}

		p.hivePartitionConfig = config
	}
}

func New(params ...Param) *Params {
	pqParameters := &Params{
		compression:    DfltCompression,
		rowGroupSize:   DfltRowGroupSize,
		binaryAsString: DfltBinaryAsString,
		filename:       DfltFilename,
		fileRowNum:     DfltFileRowNum,
		unionByName:    DfltUnionyName,
		hivePartition:  DfltUnionyName,
	}

	p := WithHivePartitionConfig()
	p(pqParameters)

	for _, param := range params {
		param(pqParameters)
	}

	return pqParameters
}
