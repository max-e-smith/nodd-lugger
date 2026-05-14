package mb

import (
	"errors"
	"github.com/max-e-smith/cruise-lug/internal"
	"github.com/max-e-smith/cruise-lug/internal/common"
	"github.com/max-e-smith/cruise-lug/internal/multibeam"
	"github.com/spf13/cobra"
)

var orderRequest multibeam.OrderRequest

var orderCmd = &cobra.Command{
	Use:   "order",
	Short: "Handles multibeam bathymetry data requests",
	Long: `
A cruise-lug command for downloading multibeam bathymetry data inventory provided in a manifest.

The provided manifest should be a json file provided with the same format as the download json 
provided by dcdb bathymetry map viewers. The manifest can be specified by either a file path or a url. If
neither is provided the command will exit.

Manifest format:
{
	"prefixes": [
		"s3://noaa-dcdb-bathymetry-pds/multibeam/2022/01/01/multibeam_survey_name.json",
	]
}
	`,
	Args: cobra.MinimumNArgs(2),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		targetPath := parseMbOrderArgs(cmd, args)
		var manifest multibeam.Manifest

		file := cmd.Flag("file")
		url := cmd.Flag("url")

		if file != nil {
			manifest = multibeam.ManifestFile{
				File: file.Value.String(),
			}
		}

		if url != nil {
			manifest = multibeam.ManifestUrl{
				Url: url.Value.String(),
			}
		}

		err := manifest.Validate()
		if err != nil {
			return err
		}

		orderRequest = multibeam.OrderRequest{
			Manifest:  manifest,
			TargetDir: targetPath,
			S3Client:  S3client,
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		internal.Submit(&orderRequest)

		if orderRequest.Error != nil {
			return orderRequest.Error
		}

		return nil
	},
}

func init() {
	MbCmd.AddCommand(orderCmd)

	//manifest source options
	orderCmd.Flags().StringVarP(&source, "file", "f", "", "Provide file manifest.")
	orderCmd.Flags().StringVarP(&source, "url", "u", "", "Provide url manifest.")

	orderCmd.MarkFlagsOneRequired("file", "url")
	orderCmd.MarkFlagsMutuallyExclusive("file", "url")

}

func parseMbOrderArgs(cmd *cobra.Command, args []string) string {
	var length = len(args)
	if length != 1 {
		common.UsageError(cmd, errors.New("please specify download manifest file path and a target directory path"))
	}

	return args[0]
}
