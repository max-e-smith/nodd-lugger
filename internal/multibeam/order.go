package multibeam

import "github.com/aws/aws-sdk-go-v2/service/s3"

type OrderRequest struct {
	Resolved  []string
	S3Bucket  string
	S3Client  s3.Client
	TargetDir string
	Error     error
}
