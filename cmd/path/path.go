package path

import (
	"fmt"
	"github.com/max-e-smith/cruise-lug/cmd"
	"github.com/spf13/cobra"
)

var pathCmd = &cobra.Command{
	Use:   "path",
	Short: "Handles data requests for a given cloud prefix or set of prefixes",
	Long: `
A cruise-lug command for downloading NCEI data using cloud path prefixes.

Providing an aws cloud path prefix will recursively download all files in that path. Providing a
space-separated list of cloud path prefixes will do the same for each prefix.

Future work will include support for non-aws cloud providers.
	`,
	Args: cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Print("path called")
	},
}

func init() {
	cmd.RootCmd.AddCommand(pathCmd)
}
