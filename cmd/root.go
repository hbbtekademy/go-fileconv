package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hbbtekademy/go-fileconv/pkg/fileconv"
	"github.com/hbbtekademy/go-fileconv/pkg/param"
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
	PQ_COMPRESSION         string = "pq-compression"
	PQ_PARTITION_BY        string = "pq-partition-by"
	PQ_FILENAME_PATTERN    string = "pq-filename-pattern"
	PQ_OVERWRITE_OR_IGNORE string = "pq-overwrite-or-ignore"
	PQ_PER_THREAD_OUTPUT   string = "pq-per-thread-output"

	FILECONV_CLI_CONFIG_DIR string = "config-dir"
	FILECONV_CLI_DESC       string = "describe"

	DFLT_FILECONV_CLI_CONFIG_DIR string = "$HOME/.fileconv-cli"
	DFLT_FILECONV_CLI_DESC       bool   = false

	DUCKDB_CONFIG string = "duckdb-config"
)

var Version = "development"

var rootCmd = &cobra.Command{
	Use:     "fileconv-cli",
	Short:   "Convert files between different formats.",
	Long:    `Convert file between different formats like JSON, CSV and Apache Parquet`,
	Version: getVersion(),
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
	rootCmd.PersistentFlags().Bool(FILECONV_CLI_DESC, DFLT_FILECONV_CLI_DESC, "(Optional) Describe the file columns")
	rootCmd.PersistentFlags().String(FILECONV_CLI_CONFIG_DIR, DFLT_FILECONV_CLI_CONFIG_DIR, "(Optional) Config Directory for the CLI")
	rootCmd.PersistentFlags().StringSlice(DUCKDB_CONFIG, []string{}, `(Optional) List of DuckDB configuration parameters. e.g.
--duckdb-config "SET threads TO 1"
--duckdb-config "SET memory_limit = '10GB'"
Refer https://duckdb.org/docs/configuration/overview.html for list of all the configurations`)
}

func registerPqWriteFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String(PQ_COMPRESSION, "snappy", "(Optional) The compression type for the output parquet file.")
	cmd.PersistentFlags().StringSlice(PQ_PARTITION_BY, []string{}, "(Optional) Write to a Hive partitioned data set of Parquet files.")
	cmd.PersistentFlags().Bool(PQ_OVERWRITE_OR_IGNORE, false, "(Optional) Use this flag to allow overwriting an existing directory.")
	cmd.PersistentFlags().String(PQ_FILENAME_PATTERN, "data_{i}.parquet", "(Optional) With this flag a pattern with {i} or {uuid} can be defined to create specific partition filenames.")
	cmd.PersistentFlags().Bool(PQ_PER_THREAD_OUTPUT, false, "(Optional) If the final number of Parquet files is not important, writing one file per thread can significantly improve performance.\n\n")

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
	configDir, err := cmd.Root().PersistentFlags().GetString(FILECONV_CLI_CONFIG_DIR)
	if err != nil || configDir == DFLT_FILECONV_CLI_CONFIG_DIR {
		configDir = os.Getenv("HOME") + "/.fileconv-cli"
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
	if err != nil && !errors.Is(err, os.ErrNotExist) {
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
			columns = append(columns, param.Column{Name: keyDataType[0], Type: keyDataType[1]})
		case l > 2:
			columns = append(columns, param.Column{Name: strings.Join(keyDataType[0:l-1], ":"), Type: keyDataType[l-1]})
		}
	}

	return columns, nil
}

func getVersion() string {
	duckdbVer, err := fileconv.GetDuckDBVersion()
	if err != nil {
		duckdbVer = err.Error()
	}
	return fmt.Sprintf("%s, duckdb version: %s", Version, duckdbVer)
}

func getDuckDBConfig(cmd *cobra.Command) ([]fileconv.DuckDBConfig, error) {
	configs, err := cmd.PersistentFlags().GetStringSlice(DUCKDB_CONFIG)
	if err != nil {
		return nil, err
	}

	duckDBConfigs := make([]fileconv.DuckDBConfig, 0, len(configs))
	for _, c := range configs {
		duckDBConfigs = append(duckDBConfigs, fileconv.DuckDBConfig(c))
	}

	return duckDBConfigs, nil
}

func getDescribeFlag(cmd *cobra.Command) bool {
	desc, err := cmd.PersistentFlags().GetBool(FILECONV_CLI_DESC)
	if err != nil {
		return false
	}

	return desc
}
