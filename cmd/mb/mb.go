package mb

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/max-e-smith/cruise-lug/cmd"
	"github.com/max-e-smith/cruise-lug/internal/common"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var S3client s3.Client
var s3Region string

var MbCmd = &cobra.Command{
	Use:   "mb",
	Short: "Handles multibeam bathymetry data requests",
	Long: `A cruise-lug command for downloading multibeam bathymetry data.

Usage:
	clug [global options] mb <subcommand> [subcommand options] <arguments> <target_dir>

Subcommands:
	survey <survey name(s)>. 
		Specify a valid survey name or list of space-separated survey names to download.
	order <manifest>.
		Specify a valid download manifest json.

Global Options:
	-b --background (default: false)
		runs the download process in the background.
	-c --space-check (default: false)
		will attempting checking target's disk space before downloading.
	-v --verbose (default: false)
		includes additional output in the console.
	-d --dry-run (default: false)
		will perform a dry run of command, skipping file download.	
	-p --parallel <number> (default: 3)
		determines the number of parallel downloads for a request.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {

		client, err := common.NewS3Client(viper.GetString("s3.region")) // default NODD region
		if err != nil {
			return err
		}

		S3client = client

		return nil
	},
}

func init() {
	cmd.RootCmd.AddCommand(MbCmd)

	MbCmd.PersistentFlags().StringVarP(&s3Region, "s3-region", "r", "us-east-1",
		"cloud region to download from. (default: us-east-1)")

	err := viper.BindPFlag("s3.region", MbCmd.PersistentFlags().Lookup("s3-region"))
	if err != nil {
		return
	}

	viper.AutomaticEnv()

}
