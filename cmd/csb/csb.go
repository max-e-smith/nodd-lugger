/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/
package csb

import (
	"github.com/max-e-smith/cruise-lug/cmd"
	"github.com/spf13/cobra"
)

var CsbCmd = &cobra.Command{
	Use:   "csb",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
}

func init() {
	cmd.RootCmd.AddCommand(CsbCmd)
}
