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
)

type hivePartitionConfig struct {
	partitionBy       []string
	overwriteOrIgnore int8
	filenamePattern   string
}

type HivePartitionOption func(*hivePartitionConfig)

const (
	dfltOverwriteOrIgnore int8   = 0
	dfltFilenamePattern   string = "data_{i}.parquet"
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

type WriteParams struct {
	compression         Compression
	rowGroupSize        int64
	hivePartitionConfig *hivePartitionConfig
	perThreadOutput     bool
}

type WriteParam func(*WriteParams)

const (
	dfltCompression     Compression = "snappy"
	dfltRowGroupSize    int64       = 122880
	dfltPerThreadOutput bool        = false
)

func WithCompression(compression Compression) WriteParam {
	return func(p *WriteParams) {
		p.compression = compression
	}
}

func WithRowGroupSize(rowGroupSize int64) WriteParam {
	return func(p *WriteParams) {
		p.rowGroupSize = rowGroupSize
	}
}

func WithHivePartitionConfig(options ...HivePartitionOption) WriteParam {
	return func(p *WriteParams) {
		p.hivePartitionConfig = &hivePartitionConfig{
			partitionBy:       []string{},
			overwriteOrIgnore: dfltOverwriteOrIgnore,
			filenamePattern:   dfltFilenamePattern,
		}

		for _, opt := range options {
			opt(p.hivePartitionConfig)
		}
	}
}

func WithPerThreadOutput(perThreadOutput bool) WriteParam {
	return func(p *WriteParams) {
		p.perThreadOutput = perThreadOutput
	}
}

func NewWriteParams(params ...WriteParam) *WriteParams {
	pqParameters := &WriteParams{
		compression:     dfltCompression,
		rowGroupSize:    dfltRowGroupSize,
		perThreadOutput: dfltPerThreadOutput,
	}

	p := WithHivePartitionConfig()
	p(pqParameters)

	for _, param := range params {
		param(pqParameters)
	}

	return pqParameters
}

func (p *WriteParams) Params() string {
	params := []string{"FORMAT PARQUET"}

	if p.compression != dfltCompression {
		params = append(params, fmt.Sprintf("COMPRESSION '%s'", p.compression))
	}

	if p.rowGroupSize != dfltRowGroupSize {
		params = append(params, fmt.Sprintf("ROW_GROUP_SIZE %d", p.rowGroupSize))
	}

	if len(p.hivePartitionConfig.partitionBy) > 0 {
		params = append(params, fmt.Sprintf("PARTITION_BY (%s)", strings.Join(p.hivePartitionConfig.partitionBy, ",")))
	}

	if p.perThreadOutput {
		params = append(params, "PER_THREAD_OUTPUT true")
	}

	if p.hivePartitionConfig.overwriteOrIgnore != dfltOverwriteOrIgnore {
		params = append(params, fmt.Sprintf("OVERWRITE_OR_IGNORE %d", p.hivePartitionConfig.overwriteOrIgnore))
	}

	if p.hivePartitionConfig.filenamePattern != dfltFilenamePattern {
		params = append(params, fmt.Sprintf("FILENAME_PATTERN '%s'", p.hivePartitionConfig.filenamePattern))
	}

	return fmt.Sprintf("(%s)", strings.Join(params, ","))
}
