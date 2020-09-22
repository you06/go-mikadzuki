package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{}

func main() {
	rootCmd.AddCommand(mikadzukiCmd)
	rootCmd.AddCommand(parseCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
