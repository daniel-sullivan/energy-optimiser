package cmd

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"energy-optimiser/config"
	"energy-optimiser/optimizer"
)

const smokeConfig = `
time_zone = "Asia/Tokyo"
[service]
poll_interval = "5m"
planning_horizon = "6h"
near_horizon = "2h"
slot_duration = "30m"
far_slot_duration = "60m"
[battery]
capacity_kwh = 10
max_charge_kw = 5
max_discharge_kw = 5
soc_min = 0.1
soc_max = 0.9
efficiency = 0.9
nominal_voltage_v = 52
[rates]
currency = "JPY"
peak_rate = 30
off_peak_rate = 10
feed_in_rate = 0
[[rates.off_peak_windows]]
start = "01:00"
end = "05:00"
rate = 10
[optimizer]
soc_risk_weight = 2
min_charge_kw = 1
blip_cost = 5
`

func parseSmokeConfig(t *testing.T) *config.Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "smoke.toml")
	if err := os.WriteFile(path, []byte(smokeConfig), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := config.Parse(path)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	return cfg
}

// TestBacktestSolarFillDormant asserts the backtest's shared-fill wiring passes
// the actuals through unchanged (no learned model/weather → perfect foresight
// preserved) while still running the production BlendSolar path.
func TestBacktestSolarFillDormant(t *testing.T) {
	cfg := parseSmokeConfig(t)
	now := time.Date(2026, 3, 1, 6, 0, 0, 0, cfg.Location())
	grid := optimizer.BuildGrid(now, cfg)

	// Uniform 30-min PV actuals, one horizon long.
	pvKW := make([]float64, 48)
	for i := range pvKW {
		pvKW[i] = 2.0
	}
	near := telescopeUniform(pvKW, grid, 30)
	got := backtestSolarFill(grid, now, near)

	if len(got) != len(near) {
		t.Fatalf("fill length %d, want %d", len(got), len(near))
	}
	for i := range near {
		if got[i] != near[i] {
			t.Fatalf("slot %d: fill %.4f != actuals %.4f (learned should be dormant)", i, got[i], near[i])
		}
	}
}

// TestBacktestTickPipeline smokes the backtest's per-tick computational path on a
// short synthetic range: telescope actuals → shared fill → PrepareInput → Solve.
func TestBacktestTickPipeline(t *testing.T) {
	cfg := parseSmokeConfig(t)
	now := time.Date(2026, 3, 1, 6, 0, 0, 0, cfg.Location())
	grid := optimizer.BuildGrid(now, cfg)

	pvKW := make([]float64, 48)
	loadW := make([]float64, 48)
	for i := range pvKW {
		pvKW[i] = 3.0
		loadW[i] = 1500 // W
	}
	pvSlot := backtestSolarFill(grid, now, telescopeUniform(pvKW, grid, 30))
	loadSlot := telescopeUniform(loadW, grid, 30)

	in := optimizer.PrepareInput(now, cfg, pvSlot, loadSlot, 0.5)
	sched, err := optimizer.Solve(in)
	if err != nil {
		t.Fatalf("solve: %v", err)
	}
	if len(sched.Slots) != grid.Len() {
		t.Fatalf("schedule slots %d, want %d", len(sched.Slots), grid.Len())
	}
}
