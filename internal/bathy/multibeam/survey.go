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
	Request common.S3Request
}

func (request *SurveyRequest) Resolve() {
	s3Request := request.Request
	fmt.Println("Resolving bathymetry data for specified surveys: ", s3Request.Arguments)
	var surveyPaths []string
	wantedSurveys := len(s3Request.Arguments)
	foundSurveys := 0

	pt, ptErr := s3Request.S3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(NODDBucket),
		Prefix:    aws.String("mb/"),
		Delimiter: aws.String("/"),
	})

	if ptErr != nil {
		s3Request.Error = ptErr
		return
	}

	for _, platformType := range pt.CommonPrefixes {

		platformParams := &s3.ListObjectsV2Input{
			Bucket:    aws.String(NODDBucket),
			Prefix:    aws.String(*platformType.Prefix),
			Delimiter: aws.String("/"),
		}

		allPlatforms := s3.NewListObjectsV2Paginator(&s3Request.S3Client, platformParams)

		for allPlatforms.HasMorePages() {
			platsPage, platsErr := allPlatforms.NextPage(context.TODO())

			if platsErr != nil {
				s3Request.Error = platsErr
				return
			}

			for _, platform := range platsPage.CommonPrefixes {
				fmt.Printf("  searching %s\n", *platform.Prefix)

				platformParams := &s3.ListObjectsV2Input{
					Bucket:    aws.String(NODDBucket),
					Prefix:    aws.String(*platform.Prefix),
					Delimiter: aws.String("/"),
				}

				platformPaginator := s3.NewListObjectsV2Paginator(&s3Request.S3Client, platformParams)

				for platformPaginator.HasMorePages() {
					surveysPage, err := platformPaginator.NextPage(context.TODO())
					if err != nil {
						s3Request.Error = err
						return
					}

					for _, survey := range surveysPage.CommonPrefixes {
						surveyPrefix := *survey.Prefix
						survey := path.Base(strings.TrimRight(surveyPrefix, "/"))
						if isSurveyMatch(s3Request.Arguments, survey) {
							surveyPaths = append(surveyPaths, surveyPrefix)
							foundSurveys++
						}
					}

				}

				if wantedSurveys == foundSurveys {
					// all surveys are found
					s3Request.Resolved = surveyPaths
					return
				}
			}
		}
	}

	if len(surveyPaths) == 0 {
		fmt.Printf("No matching surveys found for %s\n", s3Request.Arguments)
	} else {
		fmt.Printf("Found %d of %d wanted surveys at: %s\n", len(surveyPaths), len(s3Request.Arguments), surveyPaths)
		s3Request.Resolved = surveyPaths

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

func (request *SurveyRequest) CheckDiskAvailability() {
	s3Request := request.Request
	if s3Request.Error != nil || len(s3Request.Resolved) == 0 {
		return
	}

	checkDisk := viper.GetBool("check")
	if !checkDisk {
		fmt.Println("Skipping disk space check.")
		return
	}

	bytes, estimateErr := common.GetCloudContentsDiskUsageEstimate(NODDBucket, s3Request.S3Client, s3Request.Resolved)
	if estimateErr != nil {
		s3Request.Error = errors.Join(errors.New("unable to get disk usage estimate from s3 bucket"), estimateErr)
		return
	}

	spaceErr := common.DiskSpaceCheck(bytes, s3Request.TargetDir)
	if spaceErr != nil {
		s3Request.Error = spaceErr
	}

	return
}

func (request *SurveyRequest) DownloadSurveys() {
	s3Request := request.Request
	if s3Request.Error != nil || len(s3Request.Resolved) == 0 {
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
		Bucket:      NODDBucket,
		Prefixes:    s3Request.Resolved,
		Client:      s3Request.S3Client,
		TargetDir:   s3Request.TargetDir,
		WorkerCount: s3Request.WorkerCount,
	}

	if err := order.DownloadFiles(); err != nil {
		s3Request.Error = err
	}

	return
}
