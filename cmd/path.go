package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var PathCmd = &cobra.Command{
	Use:   "mb",
	Short: "Handles multibeam bathymetry data requests",
	Long: `A cruise-lug command for downloading multibeam bathymetry data.

Usage:
	clug [global options] path <prefix arguments> <target_dir>

Global Options:
	-b --background (default: false)
		runs the download process in the background.
	-c --space-check (default: false)
		will attempting checking target's disk space before downloading.
	-v --verbose (default: false)
		includes additional output in the console.
	-d --dry-run (default: false)
		will perform a dry run of command, skipping file download.	
	-p --parallel <number> (default: 3)
		determines the number of parallel downloads for a request.`,
	Args: cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Print("path called")
	},
}

func init() {
	rootCmd.AddCommand(PathCmd)
}
