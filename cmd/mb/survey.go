package mb

import (
	"errors"
	"fmt"
	"github.com/max-e-smith/cruise-lug/internal/common"
	"github.com/max-e-smith/cruise-lug/internal/nodd/bathy/multibeam"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/text/unicode/norm"
	"log"
	"strings"
)

var source string
var normalizedSource string

var surveyCmd = &cobra.Command{
	Use:   "mb",
	Short: "Handles multibeam bathymetry data requests",
	Long: `A cruise-lug command for downloading multibeam bathymetry data on a survey name basis.

Usage:
	clug [options] mb survey [source_option ( -s NODD | NCCF )] <arguments> <target_dir>

Options
	-s --source <source>. 
		Specify a valid source: NODD | NCCF (Currently only NODD is supported, NCCF is a future feature).

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
	PreRunE: func(cmd *cobra.Command, args []string) error {
		normalizedSource = strings.ToUpper(norm.NFC.String(source))

		if normalizedSource != "NODD" && normalizedSource != "NCCF" {
			common.UsageError(cmd, errors.New("please specify a valid source: NODD | NCCF"))
		}

		if normalizedSource == "NCCF" {
			return fmt.Errorf("nccf as a data source has not yet been implemented")
		}

		return nil // continue
	},
	Args: cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		targetPath, surveys := parseMbSurveyArgs(cmd, args)
		parallelDownloads := common.GetWorkersConfig()

		if normalizedSource == "NODD" {

			multibeam.MultibeamDownload(
				multibeam.MultibeamRequest{
					Surveys:     surveys,
					S3Client:    S3client,
					TargetDir:   targetPath,
					WorkerCount: parallelDownloads,
				},
			)
		}

	},
}

func init() {
	MbCmd.AddCommand(surveyCmd)

	// data source options
	surveyCmd.Flags().StringVarP(&source, "source", "s", "NODD", "Define source data repository.")

	// bind config
	sErr := viper.BindPFlag("survey", surveyCmd.PersistentFlags().Lookup("source"))
	if sErr != nil {
		log.Fatal(sErr)
	}

}

func parseMbSurveyArgs(cmd *cobra.Command, args []string) (string, []string) {
	var length = len(args)
	if length != 2 {
		common.UsageError(cmd, errors.New("please specify download manifest file path and a target directory path"))
	}

	var targetPath = args[length-1]
	var surveys = args[:length-1]

	targetError := common.VerifyTargetPermissions(targetPath)
	if targetError != nil {
		common.UsageError(cmd, targetError)
	}

	return targetPath, surveys
}
