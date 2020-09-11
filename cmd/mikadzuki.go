package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/you06/go-mikadzuki/config"
	"github.com/you06/go-mikadzuki/graph"
	"github.com/you06/go-mikadzuki/kv"
)

var (
	cfgFile string
)

var rootCmd = &cobra.Command{
	Use:   "MIKADZUKI 🌙",
	Short: "MIKADZUKI is a parallel transaction test tool",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := config.NewConfig()
		if err := cfg.Load(cfgFile); err != nil {
			panic(err)
		}
		manager := kv.NewManager(&cfg.Global)
		generator := graph.NewGenerator(&manager, &cfg.Global, &cfg.Graph, &cfg.Depend)
		graph := generator.NewGraph(8, 14)
		fmt.Println(graph.String())
	},
}

func init() {
	rootCmd.Flags().StringVar(&cfgFile, "config", "config.toml", "config file")
}

func mikadzukiExecute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
