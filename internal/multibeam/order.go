package multibeam

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Manifest interface {
	Validate() error
	Read(chan string)
}

type ManifestUrl struct {
	Url string
}

type ManifestFile struct {
	File string
}

type OrderRequest struct {
	Manifest  Manifest
	Ordered   chan string
	Resolved  chan string
	S3Bucket  string
	S3Client  s3.Client
	TargetDir string
	Error     error
}

func (request *OrderRequest) Resolve() {

}

func (request *OrderRequest) VerifyTarget() {

}

func (request *OrderRequest) Download() {

}

func (manifest ManifestFile) Validate() error {
	return nil
}

func (manifest ManifestFile) Read(chan string) {

}

func (manifest ManifestUrl) Validate() error {
	return nil
}

func (manifest ManifestUrl) Read(chan string) {

}
