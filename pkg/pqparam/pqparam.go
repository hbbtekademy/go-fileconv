package pqparam

import (
	"fmt"
	"strings"
)

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

const (
	DfltOverwriteOrIgnore int8   = 0
	DfltFilenamePattern   string = "data_{i}"
)

func WithPartitionBy(partitionBy ...string) HivePartitionOption {
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
	perThreadOutput     bool
}

type Param func(*Params)

const (
	DfltCompression     Compression = "snappy"
	DfltRowGroupSize    int64       = 122880
	DfltBinaryAsString  bool        = false
	DfltFilename        bool        = false
	DfltFileRowNum      bool        = false
	DfltUnionyName      bool        = false
	DfltHivePartition   bool        = false
	DfltPerThreadOutput bool        = false
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
		p.hivePartitionConfig = &hivePartitionConfig{
			partitionBy:       []string{},
			overwriteOrIgnore: DfltOverwriteOrIgnore,
			filenamePattern:   DfltFilenamePattern,
		}

		for _, opt := range options {
			opt(p.hivePartitionConfig)
		}
	}
}

func WithPerThreadOutput(perThreadOutput bool) Param {
	return func(p *Params) {
		p.perThreadOutput = perThreadOutput
	}
}

func New(params ...Param) *Params {
	pqParameters := &Params{
		compression:     DfltCompression,
		rowGroupSize:    DfltRowGroupSize,
		binaryAsString:  DfltBinaryAsString,
		filename:        DfltFilename,
		fileRowNum:      DfltFileRowNum,
		unionByName:     DfltUnionyName,
		hivePartition:   DfltUnionyName,
		perThreadOutput: DfltPerThreadOutput,
	}

	p := WithHivePartitionConfig()
	p(pqParameters)

	for _, param := range params {
		param(pqParameters)
	}

	return pqParameters
}

func (p *Params) WriteParams() string {
	params := []string{"FORMAT PARQUET"}

	if p.compression != DfltCompression {
		params = append(params, fmt.Sprintf("COMPRESSION '%s'", p.compression))
	}

	if p.rowGroupSize != DfltRowGroupSize {
		params = append(params, fmt.Sprintf("ROW_GROUP_SIZE %d", p.rowGroupSize))
	}

	if len(p.hivePartitionConfig.partitionBy) > 0 {
		params = append(params, fmt.Sprintf("PARTITION_BY (%s)", strings.Join(p.hivePartitionConfig.partitionBy, ",")))
	}

	if p.perThreadOutput {
		params = append(params, "PER_THREAD_OUTPUT true")
	}

	if p.hivePartitionConfig.overwriteOrIgnore != DfltOverwriteOrIgnore {
		params = append(params, fmt.Sprintf("OVERWRITE_OR_IGNORE %d", p.hivePartitionConfig.overwriteOrIgnore))
	}

	if p.hivePartitionConfig.filenamePattern != DfltFilenamePattern {
		params = append(params, fmt.Sprintf("FILENAME_PATTERN '%s'", p.hivePartitionConfig.filenamePattern))
	}

	return fmt.Sprintf("(%s)", strings.Join(params, ","))
}

func (p *Params) ReadParams() string {
	return ""
}
