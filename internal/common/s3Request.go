package common

import "github.com/aws/aws-sdk-go-v2/service/s3"

type S3Request struct {
	Arguments   []string
	Resolved    []string
	S3Bucket    string
	S3Client    s3.Client
	TargetDir   string
	WorkerCount int
	Error       error
}
