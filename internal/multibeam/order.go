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
	Requested chan string
	Resolved  chan string
	Verified  chan string
	S3Bucket  string
	S3Client  s3.Client
	TargetDir string
	Error     error
}

func (request *OrderRequest) Resolve() {

	// run an async go func that:
	//// takes the requested channel
	//// takes the resolved channel

	//// reads from the requested channel using the manifest read function
	//// resolves the file
	//// writes the result to the resolved channel

}

func (request *OrderRequest) VerifyTarget() {

	// run an async go func that:
	//// takes the resolved channel
	//// takes the verified channel

	//// reads from the resolved channel
	//// verifies the file will fit on disk (if configured to check)
	//// writes the result to the verified channel

}

func (request *OrderRequest) Download() {

	// create a worker group
	// create a bunch of workers that pull from the verified channel and run download jobs
	// wg waits for all channels to be closed before allowing function to return.

}

func (manifest ManifestFile) Validate() error {
	// validate the manifest file exists and is of a valid format / any other verification that should occur up front
	return nil
}

func (manifest ManifestFile) Read(chan string) {
	// read function that can be called to parse the file json on a streaming basis and that publishes each
	// file decoded from the json onto the "requested" channel provided as an argument
}

func (manifest ManifestUrl) Validate() error {
	// validate the url exists and that the json can be read / any other up front verification necessary
	return nil
}

func (manifest ManifestUrl) Read(chan string) {
	// read function that can be called to parse the url json on a streaming basis and that publishes each
	// file decoded from the json onto the "requested" channel provided as an argument
}
