package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var json2csvCmd = &cobra.Command{
	Use:   "json2csv",
	Short: "Convert JSON files to CSV files.",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		err := runJson2CsvCmd(cmd)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(json2csvCmd)
}

func runJson2CsvCmd(cmd *cobra.Command) error {
	return nil
}
