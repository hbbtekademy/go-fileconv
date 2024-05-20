/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var json2parquetCmd = &cobra.Command{
	Use:   "json2parquet",
	Short: "convert json file/files to apache parquet file/files",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(cmd.Parent().PersistentFlags().GetString("pq-compression"))
		fmt.Println(cmd.Flags().GetString("source"))
	},
}

func init() {
	rootCmd.AddCommand(json2parquetCmd)
	json2parquetCmd.Flags().String("source", "", "full path of json file or regex for multiple json files.")
	json2parquetCmd.MarkFlagRequired("source")
	json2parquetCmd.Flags().String("dest", "", "filename of output parquet file or directory in which to write hive partitioned parquet files")
	json2parquetCmd.MarkFlagRequired("dest")

	json2parquetCmd.LocalFlags().Bool("disable-autodetect", false, "(Optional) Disable automatically detecting the names of the keys and data types of the values.")
	json2parquetCmd.LocalFlags().String("compression", "auto", "(Optional) The compression type for the file (auto, gzip, zstd).")
	json2parquetCmd.LocalFlags().Bool("convert-str-to-int", false, "(Optional) Whether strings representing integer values should be converted to a numerical type.")
	json2parquetCmd.LocalFlags().String("dateformat", "iso", "(Optional) Specifies the date format to use when parsing dates. https://duckdb.org/docs/sql/functions/dateformat")
	json2parquetCmd.LocalFlags().Bool("filename", false, "(Optional) Whether or not an extra filename column should be included in the result.")
	json2parquetCmd.LocalFlags().String("format", "array", "(Optional) Can be one of ('auto', 'unstructured', 'newline_delimited', 'array').")
	json2parquetCmd.LocalFlags().Bool("hive-partitioning", false, "(Optional) Whether or not to interpret the path as a Hive partitioned path.")
	json2parquetCmd.LocalFlags().Bool("ignore-errors", false, "(Optional) Whether to ignore parse errors (only possible when format is 'newline_delimited').")
	json2parquetCmd.LocalFlags().Int64("max-depth", -1, "(Optional) Maximum nesting depth to which the automatic schema detection detects types.")
	json2parquetCmd.LocalFlags().Uint64("max-obj-size", 16777216, "(Optional) The maximum size of a JSON object (in bytes).")
	json2parquetCmd.LocalFlags().String("records", "auto", "(Optional) Can be one of ('auto', 'true', 'false').")
	json2parquetCmd.LocalFlags().Uint64("sample-size", 20480, "(Optional) Flag to define number of sample objects for automatic JSON type detection. Set to -1 to scan the entire input file.")
	json2parquetCmd.LocalFlags().String("timestampformat", "iso", "(Optional) Specifies the date format to use when parsing timestamps. https://duckdb.org/docs/sql/functions/dateformat")
	json2parquetCmd.LocalFlags().Bool("union-by-name", false, "(Optional) Whether the schema’s of multiple JSON files should be unified.")
}
