package pqparam

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
	binaryAsString      bool
	filename            bool
	fileRowNum          bool
	unionByName         bool
	hivePartition       bool
	hivePartitionConfig *hivePartitionConfig
}

type Param func(*Params)

func WithBinaryAsString(binaryAsString bool) Param {
	return func(pp *Params) {
		pp.binaryAsString = binaryAsString
	}
}

func WithFilename(filename bool) Param {
	return func(pp *Params) {
		pp.filename = filename
	}
}

func WithFileRowNum(fileRowNum bool) Param {
	return func(pp *Params) {
		pp.fileRowNum = fileRowNum
	}
}

func WithUnionByName(unionByName bool) Param {
	return func(pp *Params) {
		pp.unionByName = unionByName
	}
}

func WithHivePartition(hivePartition bool) Param {
	return func(pp *Params) {
		pp.hivePartition = hivePartition
	}
}

func WithHivePartitionConfig(options ...HivePartitionOption) Param {
	return func(pp *Params) {
		config := &hivePartitionConfig{
			partitionBy:       []string{},
			overwriteOrIgnore: 0,
			filenamePattern:   "data_{i}",
		}

		for _, opt := range options {
			opt(config)
		}

		pp.hivePartitionConfig = config
	}
}

func New(params ...Param) *Params {
	pqParameters := &Params{
		binaryAsString: false,
		filename:       false,
		fileRowNum:     false,
		unionByName:    false,
		hivePartition:  false,
	}

	p := WithHivePartitionConfig()
	p(pqParameters)

	for _, param := range params {
		param(pqParameters)
	}

	return pqParameters
}
