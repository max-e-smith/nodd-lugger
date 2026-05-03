package cmd

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/max-e-smith/cruise-lug/internal/common"
	"github.com/max-e-smith/cruise-lug/internal/nodd"
	"github.com/max-e-smith/cruise-lug/internal/nodd/mb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var survey bool
var path bool
var sourceNodd bool
var sourceNccf bool
var s3client s3.Client

var mbCmd = &cobra.Command{
	Use:   "mb",
	Short: "Handles multibeam bathymetry data requests",
	Long: `A cruise-lug command for downloading multibeam bathymetry
		   data.

			Usage:
				clug mb <source> <access> <options> <arguments> <target directory>

			Source Options (one is required):
				--sourceNodd: download multibeam bathymetry data from NODD using survey or path criteria.
				--sourceNccf: download multibeam bathymetry data from NOAA cloud archive (future) using survey or path criteria.

			Access Options (one is required):
				--survey <survey name(s); space separated>. 
					Specify a valid survey name or list of survey names to download from NODD.
				--path <path(s); space separated>.
					Specify a valid path or list of path prefixes to download from NODD.

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
					determines the number of parallel downloads for a request.
			`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if sourceNodd && !sourceNccf {
			s3client = nodd.NewNoddClient()
		} else if sourceNccf {
			return fmt.Errorf("sourceNccf not yet implemented")
		}
		return nil // continue
	},
	Args: cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		targetPath, surveys := parseMbArgs(cmd, args)
		parallelDownloads := getWorkersConfig()

		mb.MultibeamDownload(
			mb.MultibeamRequest{
				Surveys:     surveys,
				S3Client:    s3client,
				TargetDir:   targetPath,
				WorkerCount: parallelDownloads,
			},
		)

	},
}

func init() {
	// data source options
	mbCmd.Flags().BoolVar(&sourceNodd, "sourceNodd", false, "Resolve data from NODD cloud archive.")
	mbCmd.Flags().BoolVar(&sourceNccf, "sourceNccf", false, "Resolve data from NOAA cloud archive (future).")

	// data access options
	mbCmd.Flags().BoolVar(&survey, "survey", false,
		"Resolve and download files based on survey name(s).")
	mbCmd.Flags().BoolVar(&path, "path", false,
		"Resolve and download files based on valid cloud path(s).")

	mbCmd.MarkFlagsOneRequired("survey", "path")
	mbCmd.MarkFlagsMutuallyExclusive("manifest", "survey", "path")
	mbCmd.MarkFlagsMutuallyExclusive("sourceNodd", "sourceNccf")

	// bind config
	sErr := viper.BindPFlag("survey", RootCmd.PersistentFlags().Lookup("survey"))
	if sErr != nil {
		log.Fatal(sErr)
	}

	pErr := viper.BindPFlag("path", RootCmd.PersistentFlags().Lookup("path"))
	if pErr != nil {
		log.Fatal(pErr)
	}
}

func parseMbArgs(cmd *cobra.Command, args []string) (string, []string) {
	var length = len(args)
	if length != 2 {
		mbUsageError(cmd, errors.New("please specify download manifest file path and a target directory path"))
	}

	var targetPath = args[length-1]
	var surveys = args[:length-1]

	targetError := common.VerifyTargetPermissions(targetPath)
	if targetError != nil {
		mbUsageError(cmd, targetError)
	}

	return targetPath, surveys
}

func mbUsageError(cmd *cobra.Command, err error) {
	fmt.Println(cmd.UsageString())
	log.Fatal(err)
}
