package mb

import (
	"errors"
	"fmt"
	"github.com/max-e-smith/cruise-lug/internal"
	"github.com/max-e-smith/cruise-lug/internal/common"
	"github.com/max-e-smith/cruise-lug/internal/multibeam"
	"github.com/spf13/cobra"
	"golang.org/x/text/unicode/norm"
	"strings"
)

var source string
var target string
var surveys []string
var surveyRequest multibeam.SurveyRequest

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
		target, surveys = parseMbSurveyArgs(cmd, args)
		normalizedSource := strings.ToUpper(norm.NFC.String(source))
		var bucket string

		if normalizedSource != "NODD" && normalizedSource != "NCCF" {
			common.UsageError(cmd, errors.New("please specify a valid source: NODD | NCCF"))
		}

		if normalizedSource == "NCCF" {
			bucket = multibeam.NCCFBucket
			return fmt.Errorf("nccf as a data source has not yet been implemented")
		}

		if normalizedSource == "NODD" {
			bucket = multibeam.NODDBucket
		}

		surveyRequest = multibeam.SurveyRequest{
			Arguments: surveys,
			S3Client:  S3client,
			S3Bucket:  bucket,
			TargetDir: target,
		}

		return nil // continue
	},
	Args: cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		internal.Submit(&surveyRequest)

		if surveyRequest.Error != nil {
			return surveyRequest.Error
		}

		return nil
	},
}

func init() {
	MbCmd.AddCommand(surveyCmd)

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
