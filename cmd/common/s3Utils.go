package common

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/briandowns/spinner"
	"github.com/spf13/viper"
	"log"
	"path"
	"sync"
	"time"
)

var spin *spinner.Spinner

type Order struct {
	Bucket      string
	Prefixes    []string
	Client      s3.Client
	TargetDir   string
	WorkerCount int
}

type Download struct {
	Bucket     string
	ObjectKey  string
	TargetFile string
	Client     s3.Client
	WaitGroup  *sync.WaitGroup
}

func GetDiskUsageEstimate(bucket string, s3client s3.Client, rootPaths []string) (int64, error) {
	var totalSurveysSize int64 = 0

	for _, surveyRootPath := range rootPaths {
		fmt.Printf("Getting disk usage estimate for s3 files on %s at %s\n", bucket, surveyRootPath)
		// TODO paginate
		result, err := s3client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(surveyRootPath),
		})
		if err != nil {
			return totalSurveysSize, err
		}

		for _, object := range result.Contents {
			//log.Printf("key=%s size=%d", aws.ToString(object.Key), *object.Size)
			totalSurveysSize = totalSurveysSize + *object.Size
		}
	}

	return totalSurveysSize, nil
}

func (order Order) DownloadFiles() error {
	fmt.Printf("Downloading files to: %s\n", order.TargetDir)
	spin = spinner.New(spinner.CharSets[24], 100*time.Millisecond)
	spin.Start()

	var wg sync.WaitGroup
	downloads := make(chan Download, order.WorkerCount*2)

	for i := 1; i <= order.WorkerCount; i++ {
		wg.Add(1)
		go downloadWorker(downloads, &wg)
	}

	for _, survey := range order.Prefixes {
		var fileDownloadPageSize int32 = 100

		params := &s3.ListObjectsV2Input{
			Bucket:  aws.String(order.Bucket),
			Prefix:  aws.String(survey),
			MaxKeys: aws.Int32(fileDownloadPageSize),
		}

		filePaginator := s3.NewListObjectsV2Paginator(&order.Client, params)
		for filePaginator.HasMorePages() {
			page, err := filePaginator.NextPage(context.TODO())
			if err != nil {
				return err
			}

			for _, object := range page.Contents {
				downloads <- Download{
					Bucket:     order.Bucket,
					ObjectKey:  *object.Key,
					Client:     order.Client,
					TargetFile: path.Join(order.TargetDir, *object.Key),
					WaitGroup:  &wg,
				}
			}
		}
	}

	close(downloads)
	wg.Wait()
	spin.Stop()

	fmt.Println("  files downloaded.")

	return nil
}

func downloadWorker(requests <-chan Download, wg *sync.WaitGroup) {
	defer wg.Done()
	for request := range requests {
		downloadLargeObject(request.Bucket, request.ObjectKey, request.Client, request.TargetFile)
	}
}

func downloadLargeObject(bucket string, objectKey string, client s3.Client, targetFile string) {
	file, err := createFileWithParents(targetFile)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer closeFileChecked(file)

	start := time.Now()

	downloader := manager.NewDownloader(&client)
	n, err := downloader.Download(context.TODO(), file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		spin.Stop()
		fmt.Printf("failed to download file: %w", err)
		spin.Restart()
		return
	}

	if viper.GetBool("verbose") {
		spin.Stop()
		fmt.Printf("  successfully downloaded %g GB to %s in %g minutes.\n", ByteToGB(n), targetFile, MinutesSince(start))
		spin.Restart()
	}

	return
}
