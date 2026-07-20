package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"energy-optimiser/config"
	"energy-optimiser/influx"
)

// verifyCmd proves the VictoriaMetrics data spine returns samples for every entity the
// load model and PV calibration depend on — the biggest silent-failure risk (an empty
// query degrades the load model to conservative defaults forever, without erroring).
var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Check the metrics datastore returns samples for each configured entity",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Parse(cfgFile)
		if err != nil {
			return err
		}
		db, err := influx.New(cfg.InfluxDB)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		ctx, cancel := context.WithTimeout(cmd.Context(), 90*time.Second)
		defer cancel()
		to := time.Now()
		from := to.Add(-24 * time.Hour)

		type queryFn func(context.Context, string, time.Time, time.Time) ([]influx.Sample, error)
		probes := []struct {
			name, entity string
			fn           queryFn
		}{
			{"battery_soc (%)", cfg.HomeAssistant.Entities.BatterySOC, db.QueryPercentage},
			{"load_power (W)", cfg.HomeAssistant.Entities.LoadPower, db.QueryPower},
			{"pv_power (W)", cfg.HomeAssistant.Entities.PVPower, db.QueryPower},
			{"grid_power (W)", cfg.HomeAssistant.Entities.GridPower, db.QueryPower},
			{"battery_power (W)", cfg.HomeAssistant.Entities.BatteryPower, db.QueryPower},
		}

		fmt.Printf("Metrics datastore %s — last 24h:\n", cfg.InfluxDB.URL)
		incomplete := false
		for _, p := range probes {
			if p.entity == "" {
				fmt.Printf("  %-18s (not configured)\n", p.name)
				continue
			}
			s, err := p.fn(ctx, p.entity, from, to)
			if err != nil {
				fmt.Printf("  %-18s %-42s ERROR: %v\n", p.name, p.entity, err)
				incomplete = true
				continue
			}
			last := "n/a"
			if len(s) > 0 {
				l := s[len(s)-1]
				last = fmt.Sprintf("%.2f @ %s", l.Value, l.Time.Local().Format("15:04"))
			}
			fmt.Printf("  %-18s %-42s -> %5d samples, last=%s\n", p.name, p.entity, len(s), last)
			if len(s) == 0 {
				incomplete = true
			}
		}
		if incomplete {
			return fmt.Errorf("data spine INCOMPLETE — some entities returned no samples (check entity names vs VM, or the export endpoint)")
		}
		fmt.Println("data spine OK — all configured entities return samples")
		return nil
	},
}

func init() { rootCmd.AddCommand(verifyCmd) }
