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
	Long: `
A cruise-lug command for downloading multibeam bathymetry data.
	`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {

		client, err := common.NewS3Client(viper.GetString("s3.region"))
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
