package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/you06/go-mikadzuki/util"
)

var rootCmd = &cobra.Command{
	Use:   "MIKADZUKI 🌙",
	Short: "MIKADZUKI is a parallel transaction test tool",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		util.PrintVersion()
	},
}

func mikadzukiExecute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
