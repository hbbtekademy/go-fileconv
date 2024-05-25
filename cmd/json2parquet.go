package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/hbbtekademy/parquet-converter/pkg/param"
	"github.com/hbbtekademy/parquet-converter/pkg/param/jsonparam"
	"github.com/hbbtekademy/parquet-converter/pkg/param/pqparam"
	"github.com/hbbtekademy/parquet-converter/pkg/pqconv"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type json2ParquetFlags struct {
	disableAutodetect bool
	compression       string
	convStr2Int       bool
	dateformat        string
	filename          bool
	format            string
	hivePartitioning  bool
	ignoreErrors      bool
	maxDepth          int64
	maxObjSize        uint64
	records           string
	sampleSize        uint64
	timestampformat   string
	unionByName       bool
	columns           map[string]string
}

var json2parquetCmd = &cobra.Command{
	Use:   "json2parquet",
	Short: "convert json files to apache parquet files",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		err := runCmd(cmd)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(json2parquetCmd)
	registerJson2ParquetFlags(json2parquetCmd)
}

func runCmd(cmd *cobra.Command) error {
	source, err := cmd.Flags().GetString("source")
	if err != nil {
		return fmt.Errorf("error: %w. failed getting source flag", err)
	}

	dest, err := cmd.Flags().GetString("dest")
	if err != nil {
		return fmt.Errorf("error: %w. failed getting dest flag", err)
	}

	pqWriteFlags, err := getPqWriteFlags(cmd.Parent().PersistentFlags())
	if err != nil {
		return fmt.Errorf("error: %w. failed getting parquet write flags", err)
	}

	jsonFlags, err := getJsonReadFlags(cmd.Flags())
	if err != nil {
		return fmt.Errorf("error: %w. failed getting json read flags", err)
	}

	dbFile := getDBFile(cmd)
	defer deleteDBFile(dbFile)
	client, err := pqconv.New(context.Background(), dbFile)
	if err != nil {
		return fmt.Errorf("error: %w. failed getting duckdb client", err)
	}

	err = client.Json2Parquet(context.Background(), source, dest,
		pqparam.NewWriteParams(
			pqparam.WithCompression(pqparam.Compression(pqWriteFlags.compression)),
			pqparam.WithPerThreadOutput(pqWriteFlags.perThreadOutput),
			pqparam.WithHivePartitionConfig(
				pqparam.WithFilenamePattern(pqWriteFlags.filenamePattern),
				pqparam.WithOverwriteOrIgnore(pqWriteFlags.overwriteOrIgnore),
				pqparam.WithPartitionBy(pqWriteFlags.partitionBy...),
			),
		),
		jsonparam.WithAutoDetect(!jsonFlags.disableAutodetect),
		jsonparam.WithColumns(jsonFlags.columns),
		jsonparam.WithCompression(param.Compression(jsonFlags.compression)),
		jsonparam.WithConvStr2Int(jsonFlags.convStr2Int),
		jsonparam.WithDateFormat(jsonFlags.dateformat),
		jsonparam.WithFilename(jsonFlags.filename),
		jsonparam.WithFormat(jsonparam.Format(jsonFlags.format)),
		jsonparam.WithHivePartitioning(jsonFlags.hivePartitioning),
		jsonparam.WithIgnoreErrors(jsonFlags.ignoreErrors),
		jsonparam.WithMaxDepth(jsonFlags.maxDepth),
		jsonparam.WithMaxObjSize(jsonFlags.maxObjSize),
		jsonparam.WithRecords(jsonparam.Records(jsonFlags.records)),
		jsonparam.WithSampleSize(jsonFlags.sampleSize),
		jsonparam.WithTimestampFormat(jsonFlags.timestampformat),
		jsonparam.WithUnionByName(jsonFlags.unionByName),
	)
	if err != nil {
		return fmt.Errorf("error: %w. failed converting json to parquet", err)
	}
	return nil
}

func registerJson2ParquetFlags(json2parquetCmd *cobra.Command) {
	json2parquetCmd.Flags().SortFlags = false

	json2parquetCmd.Flags().String("source", "", "full path of json file or regex for multiple json files.")
	err := json2parquetCmd.MarkFlagRequired("source")
	checkErr("failed setting source flag as required", err)

	json2parquetCmd.Flags().String("dest", "", "filename of output parquet file or directory in which to write hive partitioned parquet files.\n")
	err = json2parquetCmd.MarkFlagRequired("dest")
	checkErr("failed setting dest flag as required", err)

	json2parquetCmd.Flags().Bool("disable-autodetect", false, "(Optional) Disable automatically detecting the names of the keys and data types of the values.")
	json2parquetCmd.Flags().String("compression", "auto", "(Optional) The compression type for the file (auto, gzip, zstd).")
	json2parquetCmd.Flags().StringSlice("columns", []string{}, `(Optional) A list of key names and value types contained within the JSON file. (e.g., "key1:INTEGER,key2:VARCHAR"). If auto detect is enabled these will be inferred.`)
	json2parquetCmd.Flags().String("format", "array", "(Optional) Can be one of ('auto', 'unstructured', 'newline_delimited', 'array').")
	json2parquetCmd.Flags().String("dateformat", "iso", "(Optional) Specifies the date format to use when parsing dates. https://duckdb.org/docs/sql/functions/dateformat")
	json2parquetCmd.Flags().String("timestampformat", "iso", "(Optional) Specifies the date format to use when parsing timestamps. https://duckdb.org/docs/sql/functions/dateformat")
	json2parquetCmd.Flags().Int64("max-depth", -1, "(Optional) Maximum nesting depth to which the automatic schema detection detects types.")
	json2parquetCmd.Flags().Uint64("max-obj-size", 16777216, "(Optional) The maximum size of a JSON object (in bytes).")
	json2parquetCmd.Flags().String("records", "auto", "(Optional) Can be one of ('auto', 'true', 'false').")
	json2parquetCmd.Flags().Uint64("sample-size", 20480, "(Optional) Flag to define number of sample objects for automatic JSON type detection. Set to -1 to scan the entire input file.")
	json2parquetCmd.Flags().Bool("convert-str-to-int", false, "(Optional) Whether strings representing integer values should be converted to a numerical type.")
	json2parquetCmd.Flags().Bool("filename", false, "(Optional) Whether or not an extra filename column should be included in the result.")
	json2parquetCmd.Flags().Bool("hive-partitioning", false, "(Optional) Whether or not to interpret the path as a Hive partitioned path.")
	json2parquetCmd.Flags().Bool("ignore-errors", false, "(Optional) Whether to ignore parse errors (only possible when format is 'newline_delimited').")
	json2parquetCmd.Flags().Bool("union-by-name", false, "(Optional) Whether the schema’s of multiple JSON files should be unified.")
}

func getJsonReadFlags(flags *pflag.FlagSet) (*json2ParquetFlags, error) {
	disableAutodetect, err := flags.GetBool("disable-autodetect")
	if err != nil {
		return nil, err
	}
	compression, err := flags.GetString("compression")
	if err != nil {
		return nil, err
	}
	convStr2Int, err := flags.GetBool("convert-str-to-int")
	if err != nil {
		return nil, err
	}
	dateformat, err := flags.GetString("dateformat")
	if err != nil {
		return nil, err
	}
	filename, err := flags.GetBool("filename")
	if err != nil {
		return nil, err
	}
	format, err := flags.GetString("format")
	if err != nil {
		return nil, err
	}
	hivePartitioning, err := flags.GetBool("hive-partitioning")
	if err != nil {
		return nil, err
	}
	ignoreErrors, err := flags.GetBool("ignore-errors")
	if err != nil {
		return nil, err
	}
	maxDepth, err := flags.GetInt64("max-depth")
	if err != nil {
		return nil, err
	}
	maxObjSize, err := flags.GetUint64("max-obj-size")
	if err != nil {
		return nil, err
	}
	records, err := flags.GetString("records")
	if err != nil {
		return nil, err
	}
	sampleSize, err := flags.GetUint64("sample-size")
	if err != nil {
		return nil, err
	}
	timestampformat, err := flags.GetString("timestampformat")
	if err != nil {
		return nil, err
	}
	unionByName, err := flags.GetBool("union-by-name")
	if err != nil {
		return nil, err
	}
	columns := map[string]string{}
	cols, err := flags.GetStringSlice("columns")
	if err != nil {
		return nil, err
	}
	for _, col := range cols {
		keyDataType := strings.Split(col, ":")
		l := len(keyDataType)
		switch {
		case l < 2:
			return nil, fmt.Errorf("incorrect columns format: %s", strings.Join(cols, ","))
		case l == 2:
			columns[keyDataType[0]] = keyDataType[1]
		case l > 2:
			columns[strings.Join(keyDataType[0:l-1], ":")] = keyDataType[l-1]
		}
	}

	return &json2ParquetFlags{
		disableAutodetect: disableAutodetect,
		compression:       compression,
		convStr2Int:       convStr2Int,
		dateformat:        dateformat,
		filename:          filename,
		format:            format,
		hivePartitioning:  hivePartitioning,
		ignoreErrors:      ignoreErrors,
		maxDepth:          maxDepth,
		maxObjSize:        maxObjSize,
		records:           records,
		sampleSize:        sampleSize,
		timestampformat:   timestampformat,
		unionByName:       unionByName,
		columns:           columns,
	}, nil
}
