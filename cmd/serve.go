package cmd

import (
	"context"
	"fmt"
	"log/slog"
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

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	runMode := "live"
	if dryRun {
		runMode = "dry-run"
	}
	slog.Info("starting energy optimiser",
		"run_mode", runMode,
		"web_port", cfg.Service.WebPort,
		"poll_interval", cfg.Service.PollInterval,
		"planning_horizon", cfg.Service.PlanningHorizon,
	)

	h, err := hub.New(cfg, dryRun)
	if err != nil {
		return fmt.Errorf("creating hub: %w", err)
	}
	defer h.Close()

	return h.Run(ctx)
}
