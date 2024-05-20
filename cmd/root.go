/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pqconv-cli",
	Short: "command line utility to convert files from/to apache parquet format",
	Long:  ``,
	//Run:   func(cmd *cobra.Command, args []string) {},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().String("pq-compression", "snappy", "(Optional) The compression type for the output parquet file.")
	rootCmd.PersistentFlags().StringSlice("pq-partition-by", []string{}, "(Optional) Write to a Hive partitioned data set of Parquet files.")
	rootCmd.PersistentFlags().String("pq-filename-pattern", "data_{i}.parquet", "(Optional) With this flag a pattern with {i} or {uuid} can be defined to create specific partition filenames.")
	rootCmd.PersistentFlags().Bool("pq-overwrite-or-ignore", false, "(Optional) Use this flag to allow overwriting an existing directory.")
	rootCmd.PersistentFlags().Bool("pq-per-thread-output", false, "(Optional) If the final number of Parquet files is not important, writing one file per thread can significantly improve performance.")
}
