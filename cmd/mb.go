package cmd

import (
	"github.com/spf13/cobra"
)

var survey bool
var path bool

var MbCmd = &cobra.Command{
	Use:   "mb",
	Short: "Handles multibeam bathymetry data requests",
	Long: `A cruise-lug command for downloading multibeam bathymetry data.

Usage:
	clug [global options] mb <subcommand> [subcommand options] <arguments> <target_dir>

Subcommands:
	survey <survey name(s)>. 
		Specify a valid survey name or list of space-separated survey names to download.
	order <manifest>.
		Specify a valid download manifest json.

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
}

func init() {
	rootCmd.AddCommand(MbCmd)

}
