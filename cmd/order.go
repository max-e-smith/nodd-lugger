package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var orderCmd = &cobra.Command{
	Use:   "order",
	Short: "Download order of files defined in a specified, valid manifest file",
	Long: `Will download files as they are defined in valid file manifest. The
manifest is in regular json and contains information on the data files' source 
(NOAA Open Dissemination bucket, for example) as well as the file cloud paths / 
prefixes. All files defined in the manifest will be downloaded to the target
path specified.

Usage:
	clug order <options> <manifest> <target>

Options:
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
		// TODO
		fmt.Println("order called")
	},
}

func init() {

}
