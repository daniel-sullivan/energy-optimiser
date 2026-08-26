package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"energy-optimiser/config"
	"energy-optimiser/hub"

	"github.com/spf13/cobra"
)

var dryRun bool

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the energy optimiser service",
	RunE:  runServe,
}

func init() {
	serveCmd.Flags().BoolVar(&dryRun, "dry-run", false,
		"read-only mode: connect to HA/InfluxDB but don't send any commands")
	rootCmd.AddCommand(serveCmd)
}

func runServe(_ *cobra.Command, _ []string) error {
	cfg, err := config.Parse(cfgFile)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	slog.Info("starting energy optimiser",
		"run_mode", map[bool]string{false: "live", true: "dry-run"}[dryRun],
		"web_port", cfg.Service.WebPort,
		"poll_interval", cfg.Service.PollInterval,
		"planning_horizon", cfg.Service.PlanningHorizon,
	)

	h, err := hub.New(cfg, dryRun)
	if err != nil {
		return fmt.Errorf("creating hub: %w", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- h.Run(runCtx) }()
	select {
	case err := <-runDone:
		h.Close()
		return err
	case <-signals:
		stopRun()
		err := <-runDone
		h.Close()
		return err
	}
}
