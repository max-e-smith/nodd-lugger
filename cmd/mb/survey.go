package mb

import (
	"errors"
	"fmt"
	"github.com/max-e-smith/cruise-lug/internal/common"
	"github.com/max-e-smith/cruise-lug/internal/nodd/bathy/multibeam"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/text/unicode/norm"
	"strings"
)

var source string

var surveyCmd = &cobra.Command{
	Use:   "survey",
	Short: "Handles multibeam bathymetry data requests",
	Long: `
A cruise-lug command for downloading multibeam bathymetry data on a survey name basis.
	
when providing a valid survey name or list of space-separated survey names, clug will attempt to 
resolve those survey names in the configured source location (the default is NODD, NOAA's Open
Data Dissemination initiation, and then download all data files and related files for that survey.
	`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		normalizedSource := strings.ToUpper(norm.NFC.String(source))

		viper.Set("source", normalizedSource)

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

		if (viper.GetString("source")) == "NODD" {

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
