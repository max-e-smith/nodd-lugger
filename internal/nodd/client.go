package nodd

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func NewNoddClient() s3.Client {
	// init aws client for nodd access
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
		config.WithRegion("us-east-1"),
	)

	if err != nil {
		fmt.Printf("Error loading AWS config: %s\n", err)
		fmt.Println("Failed to download multibeam surveys.")
		return
	}

	return *s3.NewFromConfig(cfg)
}
