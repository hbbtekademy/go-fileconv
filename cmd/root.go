/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"os"

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

	VERSION = "v1.0.0, duckdb version: v0.10.2"
)

var rootCmd = &cobra.Command{
	Use:     "pqconv-cli",
	Short:   "convert CSV and JSON files Apache Parquet format",
	Long:    `Harness the power of DuckDB to effortlessly convert CSV and JSON files to Apache Parquet format.`,
	Version: VERSION,
	//Run:   func(cmd *cobra.Command, args []string) {},
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
	rootCmd.PersistentFlags().String(PQ_COMPRESSION, "snappy", "(Optional) The compression type for the output parquet file.")
	rootCmd.PersistentFlags().StringSlice(PQ_PARTITION_BY, []string{}, "(Optional) Write to a Hive partitioned data set of Parquet files.")
	rootCmd.PersistentFlags().Bool(PQ_OVERWRITE_OR_IGNORE, false, "(Optional) Use this flag to allow overwriting an existing directory.")
	rootCmd.PersistentFlags().String(PQ_FILENAME_PATTERN, "data_{i}.parquet", "(Optional) With this flag a pattern with {i} or {uuid} can be defined to create specific partition filenames.")
	rootCmd.PersistentFlags().Bool(PQ_PER_THREAD_OUTPUT, false, "(Optional) If the final number of Parquet files is not important, writing one file per thread can significantly improve performance.")
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
