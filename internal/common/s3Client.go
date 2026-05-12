package common

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func NewS3Client(region string) (s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
		config.WithRegion(region),
	)

	if err != nil {
		fmt.Printf("Error loading AWS config: %s\n", err)
		return s3.Client{}, err
	}

	return *s3.NewFromConfig(cfg), nil
}
