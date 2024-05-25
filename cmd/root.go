package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hbbtekademy/parquet-converter/pkg/param"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type pqWriteFlags struct {
	compression       string
	partitionBy       []string
	filenamePattern   string
	overwriteOrIgnore bool
	perThreadOutput   bool
}

const (
	PQ_COMPRESSION             string = "pq-compression"
	PQ_PARTITION_BY            string = "pq-partition-by"
	PQ_FILENAME_PATTERN        string = "pq-filename-pattern"
	PQ_OVERWRITE_OR_IGNORE     string = "pq-overwrite-or-ignore"
	PQ_PER_THREAD_OUTPUT       string = "pq-per-thread-output"
	PQCONV_CLI_CONFIG_DIR      string = "config-dir"
	DFLT_PQCONV_CLI_CONFIG_DIR string = "$HOME/.pqconv-cli"

	VERSION = "v1.0.0, duckdb version: v0.10.2"
)

var rootCmd = &cobra.Command{
	Use:   "pqconv-cli",
	Short: "convert CSV and JSON files Apache Parquet format",
	Long: `Harness the power of DuckDB to effortlessly convert CSV and JSON files to Apache Parquet format.
https://duckdb.org/`,
	Version: VERSION,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return createConfigDir(cmd)
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.EnableCommandSorting = false
	registerGlobalFlags(rootCmd)
}

func registerGlobalFlags(rootCmd *cobra.Command) {
	rootCmd.Flags().SortFlags = false
	rootCmd.PersistentFlags().SortFlags = false
	rootCmd.PersistentFlags().String(PQ_COMPRESSION, "snappy", "(Optional) The compression type for the output parquet file.")
	rootCmd.PersistentFlags().StringSlice(PQ_PARTITION_BY, []string{}, "(Optional) Write to a Hive partitioned data set of Parquet files.")
	rootCmd.PersistentFlags().Bool(PQ_OVERWRITE_OR_IGNORE, false, "(Optional) Use this flag to allow overwriting an existing directory.")
	rootCmd.PersistentFlags().String(PQ_FILENAME_PATTERN, "data_{i}.parquet", "(Optional) With this flag a pattern with {i} or {uuid} can be defined to create specific partition filenames.")
	rootCmd.PersistentFlags().Bool(PQ_PER_THREAD_OUTPUT, false, "(Optional) If the final number of Parquet files is not important, writing one file per thread can significantly improve performance.")
	rootCmd.PersistentFlags().String(PQCONV_CLI_CONFIG_DIR, DFLT_PQCONV_CLI_CONFIG_DIR, "(Optional) Config Directory for the CLI")
}

func getPqWriteFlags(flags *pflag.FlagSet) (*pqWriteFlags, error) {
	compression, err := flags.GetString(PQ_COMPRESSION)
	if err != nil {
		return nil, err
	}
	partitionBy, err := flags.GetStringSlice(PQ_PARTITION_BY)
	if err != nil {
		return nil, err
	}
	filenamePattern, err := flags.GetString(PQ_FILENAME_PATTERN)
	if err != nil {
		return nil, err
	}
	overwriteOrIgnore, err := flags.GetBool(PQ_OVERWRITE_OR_IGNORE)
	if err != nil {
		return nil, err
	}
	perThreadOutput, err := flags.GetBool(PQ_PER_THREAD_OUTPUT)
	if err != nil {
		return nil, err
	}
	return &pqWriteFlags{
		compression:       compression,
		partitionBy:       partitionBy,
		filenamePattern:   filenamePattern,
		overwriteOrIgnore: overwriteOrIgnore,
		perThreadOutput:   perThreadOutput,
	}, nil
}

func checkErr(msg string, err error) {
	if err != nil {
		fmt.Printf("error: %v. %s\n", err, msg)
		os.Exit(1)
	}
}

func createConfigDir(cmd *cobra.Command) error {
	return os.MkdirAll(getConfigDir(cmd), 0700)
}

func getConfigDir(cmd *cobra.Command) string {
	configDir, err := cmd.Root().PersistentFlags().GetString(PQCONV_CLI_CONFIG_DIR)
	if err != nil || configDir == DFLT_PQCONV_CLI_CONFIG_DIR {
		configDir = os.Getenv("HOME") + "/.pqconv-cli"
	}
	return configDir
}

func getDBFile(cmd *cobra.Command) string {
	filename := fmt.Sprintf("%s/%s.%d", getConfigDir(cmd), "db.file", time.Now().UnixNano())
	return filepath.Clean(filename)
}

func deleteDBFile(dbFile string) {
	err := os.Remove(dbFile)
	if err != nil {
		fmt.Println(err)
	}
	err = os.Remove(dbFile + ".wal")
	if err != nil {
		fmt.Println(err)
	}
}

func getColumnsFlag(flags *pflag.FlagSet, name string) (param.Columns, error) {
	cols, err := flags.GetStringSlice(name)
	if err != nil {
		return nil, err
	}

	columns := param.Columns{}
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
	return columns, nil
}
