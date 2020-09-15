package main

import (
	"fmt"

	"github.com/you06/go-mikadzuki/manager"

	"github.com/spf13/cobra"
)

var (
	logPath string
)

var parseCmd = &cobra.Command{
	Use:   "parse",
	Short: "parse logs",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if logPath == "" {
			fmt.Println("log file path must be specified")
			return
		}
		if err := manager.ParseLog(logPath); err != nil {
			fmt.Printf("log parse failed %v\n", err)
			return
		}
	},
}

func init() {
	parseCmd.Flags().StringVar(&logPath, "log-path", "", "path of log files")
}
