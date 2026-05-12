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
	Long: `A cruise-lug command for downloading multibeam bathymetry data.

Usage:
	clug mb order <manifest_option ( -f | -u )> [options] <arguments> <target_dir>

Options
	-f --file <manifest_file_name>. 
		Specify a valid survey name or list of space-separated survey names to download.
	-u --url <manifest_url>.
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
	Args: cobra.MinimumNArgs(2),
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

	// manifest source options
	surveyCmd.Flags().StringVarP(&source, "file", "f", "", "Provide file manifest.")
	surveyCmd.Flags().StringVarP(&source, "url", "u", "", "Provide url manifest.")

	orderCmd.MarkFlagsOneRequired("file", "url")
	orderCmd.MarkFlagsMutuallyExclusive("file", "url")

	// bind configs
	fErr := viper.BindPFlag("file", surveyCmd.PersistentFlags().Lookup("file"))
	if fErr != nil {
		log.Fatal(fErr)
	}

	uErr := viper.BindPFlag("url", surveyCmd.PersistentFlags().Lookup("url"))
	if uErr != nil {
		log.Fatal(uErr)
	}
}
