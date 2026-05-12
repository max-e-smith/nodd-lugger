package cmd

import (
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
	Long: `
A CLI library for downloading ocean data using a noaa provided manifest or domain driven criteria sourced
directly from openly accessible options such as the NOAA Open Data Dissemination (NODD) cloud.
	`,
}

func Execute() {
	err := RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {

	// behavioral switches
	RootCmd.PersistentFlags().BoolVarP(&background, "background", "b", false,
		"Run download processes in the background. (default: false)")
	RootCmd.PersistentFlags().BoolVarP(&diskCheck, "disk-check", "k", false,
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
	cErr := viper.BindPFlag("disk-check", RootCmd.PersistentFlags().Lookup("disk-check"))
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
