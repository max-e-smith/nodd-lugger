package mb

import (
	"github.com/max-e-smith/cruise-lug/internal/common"
	"github.com/max-e-smith/cruise-lug/internal/nodd/bathy/multibeam"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

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
	PreRun: func(cmd *cobra.Command, args []string) {
		// bind configs
		fErr := viper.BindPFlag("file", cmd.PersistentFlags().Lookup("file"))
		if fErr != nil {
			log.Fatal(fErr)
		}

		uErr := viper.BindPFlag("url", cmd.PersistentFlags().Lookup("url"))
		if uErr != nil {
			log.Fatal(uErr)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		targetPath, surveys := parseMbSurveyArgs(cmd, args)
		parallelDownloads := common.GetWorkersConfig()
		multibeam.MultibeamDownload(
			multibeam.MultibeamRequest{
				Surveys:     surveys,
				S3Client:    S3client,
				TargetDir:   targetPath,
				WorkerCount: parallelDownloads,
			},
		)
		return
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
