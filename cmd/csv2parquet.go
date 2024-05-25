package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// csv2parquetCmd represents the csv2parquet command
var csv2parquetCmd = &cobra.Command{
	Use:   "csv2parquet",
	Short: "convert csv files to apache parquet files",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("csv2parquet called")
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
}
