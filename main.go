/*
Copyright © 2025 Max E Smith <max.e.smith@proton.me>
*/
package main

import (
	"github.com/max-e-smith/cruise-lug/cmd"
	_ "github.com/max-e-smith/cruise-lug/cmd/csb"
	_ "github.com/max-e-smith/cruise-lug/cmd/mb"
	_ "github.com/max-e-smith/cruise-lug/cmd/path"
	_ "github.com/max-e-smith/cruise-lug/cmd/wcd"
)

func main() {
	cmd.Execute()
}
