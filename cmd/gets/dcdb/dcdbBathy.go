package dcdb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/max-e-smith/cruise-lug/cmd/common"
	"log"
	"path"
	"strings"
	"time"
)

var Bucket = "noaa-dcdb-bathymetry-pds" // https://noaa-dcdb-bathymetry-pds.s3.amazonaws.com/index.html

func logDownloadTime(start time.Time) {
	fmt.Printf("Download completed in %g hours.\n", common.HoursSince(start))
}

func DownloadBathySurveys(surveys []string, targetPath string, s3client s3.Client) {
	start := time.Now()
	defer logDownloadTime(start)

	order := common.DownloadOrder{Bucket: Bucket, Prefixes: surveys, Client: s3client, TargetDir: targetPath, WorkerCount: 5}
	order.DownloadFiles()
}

func ResolveMultibeamSurveys(inputSurveys []string, s3client s3.Client) ([]string, error) {
	fmt.Println("Resolving bathymetry data for specified surveys: ", inputSurveys)
	var surveyPaths []string
	wantedSurveys := len(inputSurveys)
	foundSurveys := 0

	pt, ptErr := s3client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(Bucket),
		Prefix:    aws.String("mb/"),
		Delimiter: aws.String("/"),
	})

	if ptErr != nil {
		log.Fatal(ptErr)
		return surveyPaths, ptErr
	}

	for _, platformType := range pt.CommonPrefixes {

		platformParams := &s3.ListObjectsV2Input{
			Bucket:    aws.String(Bucket),
			Prefix:    aws.String(*platformType.Prefix),
			Delimiter: aws.String("/"),
		}

		allPlatforms := s3.NewListObjectsV2Paginator(&s3client, platformParams)

		for allPlatforms.HasMorePages() {
			platsPage, platsErr := allPlatforms.NextPage(context.TODO())

			if platsErr != nil {
				log.Fatal(platsErr)
				return []string{}, platsErr
			}
			for _, platform := range platsPage.CommonPrefixes {
				fmt.Printf("  searching %s\n", *platform.Prefix)

				platformParams := &s3.ListObjectsV2Input{
					Bucket:    aws.String(Bucket),
					Prefix:    aws.String(*platform.Prefix),
					Delimiter: aws.String("/"),
				}

				platformPaginator := s3.NewListObjectsV2Paginator(&s3client, platformParams)

				for platformPaginator.HasMorePages() {
					surveysPage, err := platformPaginator.NextPage(context.TODO())
					if err != nil {
						log.Fatal(err)
						return []string{}, err
					}

					for _, survey := range surveysPage.CommonPrefixes {
						surveyPrefix := *survey.Prefix
						survey := path.Base(strings.TrimRight(surveyPrefix, "/"))
						if isSurveyMatch(inputSurveys, survey) {
							surveyPaths = append(surveyPaths, surveyPrefix)
							foundSurveys++
						}
					}

				}
				if wantedSurveys == foundSurveys {
					// short circuit when enough surveys are found
					return surveyPaths, nil
				}
			}
		}
	}

	if len(surveyPaths) == 0 {
		return nil, fmt.Errorf("no surveys found")
	} else {

		// TODO additional verification of survey match results
	}
	fmt.Printf("Found %d of %d wanted surveys at: %s\n", len(surveyPaths), len(inputSurveys), surveyPaths)
	return surveyPaths, nil
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
