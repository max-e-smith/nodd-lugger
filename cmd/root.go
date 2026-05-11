package cmd

import (
	"github.com/max-e-smith/cruise-lug/cmd/csb"
	"github.com/max-e-smith/cruise-lug/cmd/mb"
	"github.com/max-e-smith/cruise-lug/cmd/wcd"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"os"
)

var background bool
var verbose bool
var diskCheck bool
var dryRun bool
var parallel int

var RootCmd = &cobra.Command{
	Use:   "clug",
	Short: "A client side retrieval tool for accessible cruise-based datasets",
	Long: `A CLI library for downloading ocean data using a noaa provided manifest or domain driven criteria sourced
directly from openly accessible options such as the NOAA Open Data Dissemination (NODD) cloud.

Subcommands:
mb: will resolve and download multibeam bathymetry data files based on survey name, cloud path, or file manifest.

csb: will resolve and download crowdsourced bathymetry data files based on survey name, cloud path, or file manifest.

wcd: will resolve and download water column data data files based on survey name, cloud path, or file manifest.

help: provides usage information for each subcommand.

Global options:
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

func Execute() {
	err := RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// add subcommands
	RootCmd.AddCommand(wcd.orderCmd)
	RootCmd.AddCommand(mb.mbCmd)
	RootCmd.AddCommand(csb.csbCmd)
	RootCmd.AddCommand(wcd.wcdCmd)

	// behavioral switches
	RootCmd.PersistentFlags().BoolVarP(&background, "background", "b", false,
		"Run download processes in the background. (default: false)")
	RootCmd.PersistentFlags().BoolVarP(&diskCheck, "space-check", "s", false,
		"Check local disk space before downloading. (default: false)")
	RootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false,
		"Display more verbose output in console output. (default: false)")
	RootCmd.PersistentFlags().BoolVarP(&dryRun, "dry-run", "d", false,
		"Perform a dry run of command, skipping file download. (default: false)")

	RootCmd.MarkFlagsMutuallyExclusive("background", "verbose")
	RootCmd.MarkFlagsMutuallyExclusive("background", "dry-run")

	// behavioral config
	RootCmd.PersistentFlags().IntVarP(&parallel, "parallel", "p", 3,
		"Number of parallel downloads. (default: 3, max: 100)")

	// add option bindings to config
	bErr := viper.BindPFlag("background", RootCmd.PersistentFlags().Lookup("background"))
	if bErr != nil {
		log.Fatal(bErr)
	}
	cErr := viper.BindPFlag("space-check", RootCmd.PersistentFlags().Lookup("space-check"))
	if cErr != nil {
		log.Fatal(cErr)
	}
	vErr := viper.BindPFlag("verbose", RootCmd.PersistentFlags().Lookup("verbose"))
	if vErr != nil {
		log.Fatal(vErr)
	}
	tErr := viper.BindPFlag("dry-run", RootCmd.PersistentFlags().Lookup("dry-run"))
	if tErr != nil {
		log.Fatal(tErr)
	}
	pErr := viper.BindPFlag("parallel", RootCmd.PersistentFlags().Lookup("parallel"))
	if pErr != nil {
		log.Fatal(pErr)
	}

}

func getWorkersConfig() int {
	numWorkers := viper.GetInt("parallel")
	if numWorkers < 1 {
		return 1
	}
	if numWorkers > 100 {
		return 100
	}
	return numWorkers
}
