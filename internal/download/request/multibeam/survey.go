package multibeam

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/max-e-smith/cruise-lug/internal/common"
	"github.com/spf13/viper"
	"path"
	"strings"
	"time"
)

type SurveyRequest struct {
	Arguments   []string
	Resolved    []string
	S3Bucket    string
	S3Client    s3.Client
	TargetDir   string
	WorkerCount int
	Error       error
}

func (request *SurveyRequest) Resolve() {
	fmt.Println("Resolving bathymetry data for specified surveys: ", request.Arguments)
	var surveyPaths []string
	wantedSurveys := len(request.Arguments)
	foundSurveys := 0

	pt, ptErr := request.S3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(request.S3Bucket),
		Prefix:    aws.String("mb/"),
		Delimiter: aws.String("/"),
	})

	if ptErr != nil {
		request.Error = ptErr
		return
	}

	for _, platformType := range pt.CommonPrefixes {

		platformParams := &s3.ListObjectsV2Input{
			Bucket:    aws.String(request.S3Bucket),
			Prefix:    aws.String(*platformType.Prefix),
			Delimiter: aws.String("/"),
		}

		allPlatforms := s3.NewListObjectsV2Paginator(&request.S3Client, platformParams)

		for allPlatforms.HasMorePages() {
			platsPage, platsErr := allPlatforms.NextPage(context.TODO())

			if platsErr != nil {
				request.Error = platsErr
				return
			}

			for _, platform := range platsPage.CommonPrefixes {
				fmt.Printf("  searching %s\n", *platform.Prefix)

				platformParams := &s3.ListObjectsV2Input{
					Bucket:    aws.String(request.S3Bucket),
					Prefix:    aws.String(*platform.Prefix),
					Delimiter: aws.String("/"),
				}

				platformPaginator := s3.NewListObjectsV2Paginator(&request.S3Client, platformParams)

				for platformPaginator.HasMorePages() {
					surveysPage, err := platformPaginator.NextPage(context.TODO())
					if err != nil {
						request.Error = err
						return
					}

					for _, survey := range surveysPage.CommonPrefixes {
						surveyPrefix := *survey.Prefix
						survey := path.Base(strings.TrimRight(surveyPrefix, "/"))
						if isSurveyMatch(request.Arguments, survey) {
							surveyPaths = append(surveyPaths, surveyPrefix)
							foundSurveys++
						}
					}

				}

				if wantedSurveys == foundSurveys {
					// all surveys are found
					request.Resolved = surveyPaths
					return
				}
			}
		}
	}

	if len(surveyPaths) == 0 {
		fmt.Printf("No matching surveys found for %s\n", request.Arguments)
	} else {
		fmt.Printf("Found %d of %d wanted surveys at: %s\n", len(surveyPaths), len(request.Arguments), surveyPaths)
		request.Resolved = surveyPaths

	}
	return
}

func isSurveyMatch(surveys []string, resolvedSurvey string) bool {
	for _, survey := range surveys {
		if survey == resolvedSurvey {
			fmt.Println("Found matching survey: ", survey)
			return true
		}
	}
	return false
}

func (request *SurveyRequest) VerifyTarget() {
	if request.Error != nil || len(request.Resolved) == 0 {
		return
	}

	checkDisk := viper.GetBool("check")
	if !checkDisk {
		fmt.Println("Skipping disk space check.")
		return
	}

	bytes, estimateErr := common.GetCloudContentsDiskUsageEstimate(request.S3Bucket, request.S3Client, request.Resolved)
	if estimateErr != nil {
		request.Error = errors.Join(errors.New("unable to get disk usage estimate from s3 bucket"), estimateErr)
		return
	}

	spaceErr := common.DiskSpaceCheck(bytes, request.TargetDir)
	if spaceErr != nil {
		request.Error = spaceErr
	}

	return
}

func (request *SurveyRequest) Download() {
	if request.Error != nil || len(request.Resolved) == 0 {
		return
	}
	dryRun := viper.GetBool("try")
	if dryRun {
		fmt.Println("Skipping download due to dry run flag.")
		return
	}

	start := time.Now()
	defer common.LogDownloadTime(start)

	order := common.Order{
		Bucket:      request.S3Bucket,
		Prefixes:    request.Resolved,
		Client:      request.S3Client,
		TargetDir:   request.TargetDir,
		WorkerCount: request.WorkerCount,
	}

	if err := order.DownloadFiles(); err != nil {
		request.Error = err
	}

	return
}
