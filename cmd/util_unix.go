//go:build !windows

package cmd

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func getConfigDir(cmd *cobra.Command) string {
	configDir, err := cmd.Root().PersistentFlags().GetString(FILECONV_CLI_CONFIG_DIR)
	if err != nil || configDir == DFLT_FILECONV_CLI_CONFIG_DIR {
		configDir = os.Getenv("HOME") + "/.fileconv-cli"
	}
	return filepath.Clean(configDir)
}
