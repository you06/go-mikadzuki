package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/you06/go-mikadzuki/config"
	"github.com/you06/go-mikadzuki/manager"
)

var (
	cfgFile string
	dryrun  bool
)

var mikadzukiCmd = &cobra.Command{
	Use:   "go",
	Short: "🌙 MIKADZUKI is a parallel transaction test tool",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := config.NewConfig()
		if err := cfg.Load(cfgFile); err != nil {
			panic(err)
		}
		mgr := manager.NewManager(manager.Option{
			Cfg:    &cfg,
			Dryrun: dryrun,
		})
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			sc := make(chan os.Signal, 1)
			signal.Notify(sc,
				os.Interrupt,
				syscall.SIGHUP,
				syscall.SIGINT,
				syscall.SIGTERM,
				syscall.SIGQUIT)

			fmt.Printf("Got signal %d to exit.\n", <-sc)
			cancel()
		}()
		// mgr.Run(ctx)
		if err := mgr.Once(ctx); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	mikadzukiCmd.Flags().StringVar(&cfgFile, "config", "config.toml", "config file")
	mikadzukiCmd.Flags().BoolVar(&dryrun, "dryrun", false, "dry run mode will generate graph only")
}
