package common

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/viper"
)

func GetCloudContentsDiskUsageEstimate(bucket string, s3client s3.Client, rootPaths []string) (int64, error) {
	var totalSurveysSize int64 = 0
	verbose := viper.GetBool("verbose")

	for _, surveyRootPath := range rootPaths {
		fmt.Printf("Getting disk usage estimate for s3 files on %s at %s\n", bucket, surveyRootPath)

		params := &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(surveyRootPath),
		}

		filePaginator := s3.NewListObjectsV2Paginator(&s3client, params)
		for filePaginator.HasMorePages() {
			page, err := filePaginator.NextPage(context.TODO())
			if err != nil {
				return totalSurveysSize, err
			}

			for _, object := range page.Contents {
				if verbose {
					fmt.Printf(" key=%s size=%d\n", aws.ToString(object.Key), *object.Size)
				}
				totalSurveysSize = totalSurveysSize + *object.Size
			}
		}

	}

	return totalSurveysSize, nil
}
