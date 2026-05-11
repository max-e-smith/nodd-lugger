package common

import (
	"fmt"
	"github.com/spf13/cobra"
	"log"
)

func UsageError(cmd *cobra.Command, err error) {
	fmt.Println(cmd.UsageString())
	log.Fatal(err)
}
