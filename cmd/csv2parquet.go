package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/hbbtekademy/go-fileconv/pkg/fileconv"
	"github.com/hbbtekademy/go-fileconv/pkg/param"
	"github.com/hbbtekademy/go-fileconv/pkg/param/csvparam"
	"github.com/hbbtekademy/go-fileconv/pkg/param/pqparam"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type csv2ParquetFlags struct {
	allVarchar         bool
	disableQuotedNulls bool
	disableAutodetect  bool
	autoTypeCandidates []string
	columns            param.Columns
	compression        string
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
	types              param.Columns
	unionByName        bool
}

var csv2parquetCmd = &cobra.Command{
	Use:   "csv2parquet",
	Short: "Convert CSV files to Apache Parquet files (https://duckdb.org/docs/data/csv/overview#parameters)",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		err := runCsv2ParquetCmd(cmd)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(csv2parquetCmd)
	registerCsv2ParquetFlags(csv2parquetCmd)
}

func runCsv2ParquetCmd(cmd *cobra.Command) error {
	source, err := cmd.Flags().GetString("source")
	if err != nil {
		return fmt.Errorf("error: %w. failed getting source flag", err)
	}

	dest, err := cmd.Flags().GetString("dest")
	if err != nil {
		return fmt.Errorf("error: %w. failed getting dest flag", err)
	}

	pqWriteFlags, err := getPqWriteFlags(cmd.Root().PersistentFlags())
	if err != nil {
		return fmt.Errorf("error: %w. failed getting parquet write flags", err)
	}

	csvFlags, err := getCsvReadFlags(cmd.Flags())
	if err != nil {
		return fmt.Errorf("error: %w. failed getting csv read flags", err)
	}

	dbFile := getDBFile(cmd)
	defer deleteDBFile(dbFile)
	client, err := fileconv.New(context.Background(), dbFile)
	if err != nil {
		return fmt.Errorf("error: %w. failed getting duckdb client", err)
	}

	err = client.Csv2Parquet(context.Background(), source, dest,
		pqparam.NewWriteParams(
			pqparam.WithCompression(pqparam.Compression(pqWriteFlags.compression)),
			pqparam.WithPerThreadOutput(pqWriteFlags.perThreadOutput),
			pqparam.WithHivePartitionConfig(
				pqparam.WithFilenamePattern(pqWriteFlags.filenamePattern),
				pqparam.WithOverwriteOrIgnore(pqWriteFlags.overwriteOrIgnore),
				pqparam.WithPartitionBy(pqWriteFlags.partitionBy...),
			),
		),
		csvparam.WithAllVarchar(csvFlags.allVarchar),
		csvparam.WithAllowQuotedNulls(!csvFlags.disableQuotedNulls),
		csvparam.WithAutoDetect(!csvFlags.disableAutodetect),
		csvparam.WithAutoTypeCandidates(csvFlags.autoTypeCandidates),
		csvparam.WithColumns(csvFlags.columns),
		csvparam.WithCompression(param.Compression(csvFlags.compression)),
		csvparam.WithDateformat(csvFlags.dateformat),
		csvparam.WithDecimalSeparator(csvFlags.decimalSeparator),
		csvparam.WithDelim(csvFlags.delim),
		csvparam.WithEscape(csvFlags.escape),
		csvparam.WithFilename(csvFlags.filename),
		csvparam.WithForceNotNull(csvFlags.forceNotNull),
		csvparam.WithHeader(csvFlags.header),
		csvparam.WithHivePartitioning(csvFlags.hivePartitioning),
		csvparam.WithIgnoreErrors(csvFlags.ignoreErrors),
		csvparam.WithMaxLineSize(csvFlags.maxLineSize),
		csvparam.WithNames(csvFlags.names),
		csvparam.WithNewLine(csvFlags.newLine),
		csvparam.WithNormalizeNames(csvFlags.normalizeNames),
		csvparam.WithNullPadding(csvFlags.nullPadding),
		csvparam.WithNullStrings(csvFlags.nullStr),
		csvparam.WithParallel(csvFlags.parallel),
		csvparam.WithQuote(csvFlags.quote),
		csvparam.WithSampleSize(csvFlags.sampleSize),
		csvparam.WithSkip(csvFlags.skip),
		csvparam.WithTimestampFormat(csvFlags.timestampformat),
		csvparam.WithTypes(csvFlags.types),
		csvparam.WithUnionByName(csvFlags.unionByName),
	)
	if err != nil {
		return fmt.Errorf("error: %w. failed converting csv to parquet", err)
	}
	return nil
}

func registerCsv2ParquetFlags(cmd *cobra.Command) {
	cmd.Flags().SortFlags = false

	cmd.Flags().String("source", "", "full path of csv file or regex for multiple csv files.")
	err := cmd.MarkFlagRequired("source")
	checkErr("failed setting source flag as required", err)

	cmd.Flags().String("dest", "", "filename of output parquet file or directory in which to write hive partitioned parquet files.\n")
	err = cmd.MarkFlagRequired("dest")
	checkErr("failed setting dest flag as required", err)

	cmd.Flags().String("delim", ",", "(Optional) Specifies the character that separates columns within each row (line) of the file.")
	cmd.Flags().String("quote", `"`, "(Optional) Specifies the quoting string to be used when a data value is quoted.")
	cmd.Flags().String("new-line", "", "(Optional) Set the new line character(s) in the file. Options are '\\r','\\n', or '\\r\\n'.")
	cmd.Flags().String("decimal-sep", ".", "(Optional) The decimal separator of numbers.")
	cmd.Flags().String("escape", `"`, "(Optional) Specifies the string that should appear before a data character sequence that matches the quote value.")
	cmd.Flags().String("dateformat", "", "(Optional) Specifies the date format to use when parsing dates. https://duckdb.org/docs/sql/functions/dateformat")
	cmd.Flags().String("timestampformat", "", "(Optional) Specifies the date format to use when parsing timestamps. https://duckdb.org/docs/sql/functions/dateformat")
	cmd.Flags().String("compression", "auto", "(Optional) The compression type for the file (auto, gzip, zstd).")

	cmd.Flags().Int64("max-line-size", 2097152, "(Optional) The maximum line size in bytes.")
	cmd.Flags().Int64("sample-size", 20480, "(Optional) The number of sample rows for auto detection of parameters.")
	cmd.Flags().Int64("skip", 0, "(Optional) The number of lines at the top of the file to skip.")

	cmd.Flags().StringSlice("force-not-null", []string{}, `(Optional) Do not match the specified columns’ values against the NULL string. 
In the default case where the NULL string is empty, this means that empty values will be read as zero-length strings rather than NULLs.`)
	cmd.Flags().StringSlice("auto-type-candidates", []string{}, `(Optional) This option allows you to specify the types that the sniffer will use when detecting CSV column types. 
The VARCHAR type is always included in the detected types (as a fallback option). 
Valid values (SQLNULL, BOOLEAN, BIGINT, DOUBLE, TIME, DATE, TIMESTAMP, VARCHAR).`)
	cmd.Flags().StringSlice("columns", []string{}, `(Optional) A list that specifies the column names and column types contained within the CSV file (e.g., col1:INTEGER,col2:VARCHAR).
The order of the Name:Type definitions should match the order of columns in the CSV file.
Using this option implies that auto detection is not used.`)
	cmd.Flags().StringSlice("names", []string{}, `(Optional) If the file does not contain a header, names will be auto-generated by default. You can provide your own names with the names option.`)
	cmd.Flags().StringSlice("nullstr", []string{}, `(Optional) Specifies a list of strings that represent a NULL value.`)
	cmd.Flags().StringSlice("types", []string{}, `(Optional) The types flag can be used to override types of only certain columns by providing a list of name:type mappings (e.g., col1:INTEGER,col2:VARCHAR)`)

	cmd.Flags().Bool("disable-autodetect", false, "(Optional) Disable auto detection of CSV parameters.")
	cmd.Flags().Bool("all-varchar", false, "(Optional) Option to skip type detection for CSV parsing and assume all columns to be of type VARCHAR.")
	cmd.Flags().Bool("disable-quoted-nulls", false, "(Optional) Disable the conversion of quoted values to NULL values.")
	cmd.Flags().Bool("normalize-names", false, "(Optional) Boolean value that specifies whether or not column names should be normalized, removing any non-alphanumeric characters from them.")
	cmd.Flags().Bool("filename", false, "(Optional) Whether or not an extra filename column should be included in the result.")
	cmd.Flags().Bool("header", false, "(Optional) Specifies that the file contains a header line with the names of each column in the file.")
	cmd.Flags().Bool("hive-partitioning", false, "(Optional) Whether or not to interpret the path as a Hive partitioned path.")
	cmd.Flags().Bool("ignore-errors", false, "(Optional) Whether to ignore parse errors (only possible when format is 'newline_delimited').")
	cmd.Flags().Bool("null-padding", false, "(Optional) If this option is enabled, when a row lacks columns, it will pad the remaining columns on the right with null values.")
	cmd.Flags().Bool("parallel", false, "(Optional) Whether or not the parallel CSV reader is used.")
	cmd.Flags().Bool("union-by-name", false, "(Optional) Whether the schema's of multiple CSV files should be unified.")
}

func getCsvReadFlags(flags *pflag.FlagSet) (*csv2ParquetFlags, error) {
	delim, err := flags.GetString("delim")
	if err != nil {
		return nil, err
	}
	quote, err := flags.GetString("quote")
	if err != nil {
		return nil, err
	}
	newLine, err := flags.GetString("new-line")
	if err != nil {
		return nil, err
	}
	decimapSep, err := flags.GetString("decimal-sep")
	if err != nil {
		return nil, err
	}
	escape, err := flags.GetString("escape")
	if err != nil {
		return nil, err
	}
	dateformat, err := flags.GetString("dateformat")
	if err != nil {
		return nil, err
	}
	timestampformat, err := flags.GetString("timestampformat")
	if err != nil {
		return nil, err
	}
	compression, err := flags.GetString("compression")
	if err != nil {
		return nil, err
	}
	maxLineSize, err := flags.GetInt64("max-line-size")
	if err != nil {
		return nil, err
	}
	sampleSize, err := flags.GetInt64("sample-size")
	if err != nil {
		return nil, err
	}
	skip, err := flags.GetInt64("skip")
	if err != nil {
		return nil, err
	}
	disableAutodetect, err := flags.GetBool("disable-autodetect")
	if err != nil {
		return nil, err
	}
	allVarchar, err := flags.GetBool("all-varchar")
	if err != nil {
		return nil, err
	}
	disableQuotedNulls, err := flags.GetBool("disable-quoted-nulls")
	if err != nil {
		return nil, err
	}
	normalizeNames, err := flags.GetBool("normalize-names")
	if err != nil {
		return nil, err
	}
	filename, err := flags.GetBool("filename")
	if err != nil {
		return nil, err
	}
	header, err := flags.GetBool("header")
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
	nullPadding, err := flags.GetBool("null-padding")
	if err != nil {
		return nil, err
	}
	parallel, err := flags.GetBool("parallel")
	if err != nil {
		return nil, err
	}
	unionByName, err := flags.GetBool("union-by-name")
	if err != nil {
		return nil, err
	}
	columns, err := getColumnsFlag(flags, "columns")
	if err != nil {
		return nil, err
	}
	forceNotNull, err := flags.GetStringSlice("force-not-null")
	if err != nil {
		return nil, err
	}
	autoTypeCandidates, err := flags.GetStringSlice("auto-type-candidates")
	if err != nil {
		return nil, err
	}
	names, err := flags.GetStringSlice("names")
	if err != nil {
		return nil, err
	}
	nullStr, err := flags.GetStringSlice("nullstr")
	if err != nil {
		return nil, err
	}
	types, err := getColumnsFlag(flags, "types")
	if err != nil {
		return nil, err
	}
	if len(columns) > 0 {
		disableAutodetect = true
	}

	return &csv2ParquetFlags{
		delim:              delim,
		quote:              quote,
		newLine:            newLine,
		decimalSeparator:   decimapSep,
		escape:             escape,
		dateformat:         dateformat,
		timestampformat:    timestampformat,
		compression:        compression,
		maxLineSize:        maxLineSize,
		sampleSize:         sampleSize,
		skip:               skip,
		disableAutodetect:  disableAutodetect,
		allVarchar:         allVarchar,
		disableQuotedNulls: disableQuotedNulls,
		normalizeNames:     normalizeNames,
		filename:           filename,
		header:             header,
		hivePartitioning:   hivePartitioning,
		ignoreErrors:       ignoreErrors,
		nullPadding:        nullPadding,
		parallel:           parallel,
		unionByName:        unionByName,
		columns:            columns,
		forceNotNull:       forceNotNull,
		autoTypeCandidates: autoTypeCandidates,
		names:              names,
		nullStr:            nullStr,
		types:              types,
	}, nil
}
