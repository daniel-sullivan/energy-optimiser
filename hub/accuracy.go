package hub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"energy-optimiser/influx"
	"energy-optimiser/serve"
)

const (
	accuracyFileName    = "accuracy.json"
	accuracyStateVer    = 1
	accuracyWindow      = 7 * 24 * time.Hour // rolling retention at slot resolution
	accuracyVMQueryWait = 20 * time.Second   // bound the per-slot VM actual lookups
	// accuracyResolveGrace delays reading a slot's actual until well after the
	// slot ends, so HA→VM ingestion lag can't lock in a partial-window mean or a
	// false gap (a slot whose samples land in VM a little late).
	accuracyResolveGrace = 5 * time.Minute
)

// actualsSource reads the mean measured value (kW) for a HA entity over a slot
// window. The production implementation wraps the metrics client; a fake drives
// the recorder test. A nil source disables VM-backed actuals (PV/grid stay
// pending) — the recorder still records predictions and SoC actuals.
type actualsSource interface {
	MeanPowerKW(ctx context.Context, entityID string, from, to time.Time) (float64, bool, error)
}

// influxActuals adapts the metrics client: mean of the raw power samples over the
// slot window, converted W→kW. ok=false when no sample covered the window.
type influxActuals struct{ client *influx.Client }

func (a influxActuals) MeanPowerKW(ctx context.Context, entityID string, from, to time.Time) (float64, bool, error) {
	samples, err := a.client.QueryPower(ctx, entityID, from, to)
	if err != nil {
		return 0, false, err
	}
	if len(samples) == 0 {
		return 0, false, nil
	}
	var sum float64
	for _, s := range samples {
		sum += s.Value
	}
	return (sum / float64(len(samples))) / 1000.0, true, nil
}

// accuracyTick is the per-tick input: the predictions made for the current slot
// and the live measured SoC. PV and grid actuals are resolved later from the
// metrics store; SoC is paired here from the HA state cache at the slot boundary.
type accuracyTick struct {
	Now         time.Time // current slot start (floored to the slot boundary)
	SolcastKW   float64
	HasSolcast  bool
	ModelKW     float64
	HasModel    bool
	PlannedSOC  float64 // planned end-of-slot SoC (0-1)
	PlannedGrid float64 // planned net grid (kW; + import / − export)
	HasPlan     bool
	MeasuredSOC float64 // live SoC (0-1); ≤0 = unknown, not recorded
}

// slotAccuracy is one slot's persisted predicted-vs-actual record. Predictions
// are captured on first sighting of the slot; actuals land once it elapses.
type slotAccuracy struct {
	Slot time.Time `json:"slot"`

	SolcastKW   float64 `json:"solcast_kw,omitempty"`
	HasSolcast  bool    `json:"has_solcast,omitempty"`
	ModelKW     float64 `json:"model_kw,omitempty"`
	HasModel    bool    `json:"has_model,omitempty"`
	ActualPVKW  float64 `json:"actual_pv_kw,omitempty"`
	HasActualPV bool    `json:"has_actual_pv,omitempty"`

	PlannedSOC    float64 `json:"planned_soc,omitempty"`
	PlannedGridKW float64 `json:"planned_grid_kw,omitempty"`
	HasPlan       bool    `json:"has_plan,omitempty"`
	ActualSOC     float64 `json:"actual_soc,omitempty"`
	HasActualSOC  bool    `json:"has_actual_soc,omitempty"`

	ActualGridKW  float64 `json:"actual_grid_kw,omitempty"`
	HasActualGrid bool    `json:"has_actual_grid,omitempty"`

	// PVGridResolved marks that a metrics lookup for this slot's PV+grid actuals
	// has completed (successfully — data present or genuinely absent), so an
	// elapsed slot is not re-queried forever. A query error leaves it false to
	// retry next tick.
	PVGridResolved bool `json:"pv_grid_resolved,omitempty"`
}

// accuracyRecorder captures per-slot predictions and pairs them with actuals,
// persisting a rolling window to DataDir (atomic JSON, mirroring pvmodel/store).
// It is read concurrently by the dashboard, written by the tick thread (Record)
// and a background resolver (ResolveActuals); mu guards the record map.
type accuracyRecorder struct {
	mu         sync.Mutex
	path       string
	window     time.Duration
	slotDur    time.Duration
	actuals    actualsSource // nil ⇒ PV/grid actuals stay pending
	pvEntity   string
	gridEntity string
	records    map[int64]*slotAccuracy // keyed by slot-start unix seconds
}

// newAccuracyRecorder builds the recorder, loading any persisted window. A
// missing/corrupt file cold-starts empty (logged, never fatal); a bad data dir
// degrades to in-memory only. actuals may be nil.
func newAccuracyRecorder(dataDir string, slotDur time.Duration, actuals actualsSource, pvEntity, gridEntity string) *accuracyRecorder {
	if slotDur <= 0 {
		slotDur = 30 * time.Minute
	}
	r := &accuracyRecorder{
		path:       filepath.Join(dataDir, accuracyFileName),
		window:     accuracyWindow,
		slotDur:    slotDur,
		actuals:    actuals,
		pvEntity:   pvEntity,
		gridEntity: gridEntity,
		records:    make(map[int64]*slotAccuracy),
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		slog.Warn("accuracy: data dir unavailable — recording in-memory only", "dir", dataDir, "error", err)
		r.path = "" // disable persistence: record in-memory only, no per-tick write spam
	}
	r.load()
	return r
}

// Record captures the current slot's predictions on first sighting and pairs the
// previous slot's SoC actual from the boundary reading. Cheap and in-memory; the
// slower metrics-backed PV/grid actuals are resolved by ResolveActuals.
func (r *accuracyRecorder) Record(in accuracyTick) {
	slot := in.Now.Truncate(r.slotDur)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Pair the previous slot's SoC actual: the SoC measured as we cross into this
	// slot is the SoC at the end of the previous slot.
	if in.MeasuredSOC > 0 {
		if prev := r.records[slot.Add(-r.slotDur).Unix()]; prev != nil && !prev.HasActualSOC {
			prev.ActualSOC = in.MeasuredSOC
			prev.HasActualSOC = true
		}
	}

	if _, ok := r.records[slot.Unix()]; !ok {
		rec := &slotAccuracy{
			Slot:       slot,
			SolcastKW:  in.SolcastKW,
			HasSolcast: in.HasSolcast,
			ModelKW:    in.ModelKW,
			HasModel:   in.HasModel,
		}
		if in.HasPlan {
			rec.PlannedSOC = in.PlannedSOC
			rec.PlannedGridKW = in.PlannedGrid
			rec.HasPlan = true
		}
		r.records[slot.Unix()] = rec
	}

	r.pruneLocked(in.Now)
	r.persistLocked()
}

// ResolveActuals fills PV and grid actuals for elapsed slots from the metrics
// store. It snapshots the due slots under the lock, runs the (blocking) metrics
// lookups without it, then writes results back — so a slow query never stalls
// Record or the dashboard. A no-op without a metrics source.
func (r *accuracyRecorder) ResolveActuals(ctx context.Context, now time.Time) {
	if r.actuals == nil {
		return
	}

	r.mu.Lock()
	var due []time.Time
	for _, rec := range r.records {
		if !rec.PVGridResolved && !now.Before(rec.Slot.Add(r.slotDur + accuracyResolveGrace)) {
			due = append(due, rec.Slot)
		}
	}
	r.mu.Unlock()
	if len(due) == 0 {
		return
	}

	type resolved struct {
		slot                 time.Time
		pvKW, gridKW         float64
		hasPV, hasGrid, done bool
	}
	results := make([]resolved, 0, len(due))
	for _, slot := range due {
		qctx, cancel := context.WithTimeout(ctx, accuracyVMQueryWait)
		from, to := slot, slot.Add(r.slotDur)
		pvKW, hasPV, pvErr := r.actuals.MeanPowerKW(qctx, r.pvEntity, from, to)
		gridKW, hasGrid, gridErr := r.actuals.MeanPowerKW(qctx, r.gridEntity, from, to)
		cancel()
		if pvErr != nil || gridErr != nil {
			slog.Warn("accuracy: metrics actual lookup failed — will retry",
				"slot", slot.Format(time.RFC3339), "pv_err", pvErr, "grid_err", gridErr)
		}
		results = append(results, resolved{
			slot: slot, pvKW: pvKW, gridKW: gridKW,
			hasPV: hasPV, hasGrid: hasGrid,
			done: pvErr == nil && gridErr == nil,
		})
	}

	r.mu.Lock()
	for _, rr := range results {
		rec := r.records[rr.slot.Unix()]
		if rec == nil {
			continue
		}
		if rr.hasPV {
			rec.ActualPVKW, rec.HasActualPV = rr.pvKW, true
		}
		if rr.hasGrid {
			rec.ActualGridKW, rec.HasActualGrid = rr.gridKW, true
		}
		if rr.done {
			rec.PVGridResolved = true
		}
	}
	r.pruneLocked(now)
	r.persistLocked()
	r.mu.Unlock()
}

// Snapshot returns the rolling window as the dashboard's read model, sorted by
// slot ascending.
func (r *accuracyRecorder) Snapshot() serve.AccuracySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	pts := make([]serve.AccuracyPoint, 0, len(r.records))
	for _, rec := range r.records {
		pts = append(pts, serve.AccuracyPoint{
			Slot:           rec.Slot,
			SolcastKW:      rec.SolcastKW,
			HasSolcast:     rec.HasSolcast,
			ModelKW:        rec.ModelKW,
			HasModel:       rec.HasModel,
			ActualPVKW:     rec.ActualPVKW,
			HasActualPV:    rec.HasActualPV,
			PlannedSOC:     rec.PlannedSOC,
			HasPlannedSOC:  rec.HasPlan,
			ActualSOC:      rec.ActualSOC,
			HasActualSOC:   rec.HasActualSOC,
			PlannedGridKW:  rec.PlannedGridKW,
			HasPlannedGrid: rec.HasPlan,
			ActualGridKW:   rec.ActualGridKW,
			HasActualGrid:  rec.HasActualGrid,
		})
	}
	sort.Slice(pts, func(i, j int) bool { return pts[i].Slot.Before(pts[j].Slot) })
	return serve.AccuracySnapshot{
		Points:  pts,
		WindowH: r.window.Hours(),
		SlotMin: r.slotDur.Minutes(),
	}
}

// pruneLocked drops records older than the rolling window. Caller holds mu.
func (r *accuracyRecorder) pruneLocked(now time.Time) {
	cutoff := now.Add(-r.window)
	for k, rec := range r.records {
		if rec.Slot.Before(cutoff) {
			delete(r.records, k)
		}
	}
}

// accuracyState is the persisted JSON form.
type accuracyState struct {
	Version int            `json:"version"`
	Slots   []slotAccuracy `json:"slots"`
}

// persistLocked writes the window atomically (temp + fsync + rename). Best-effort:
// a write failure is logged, never fatal. Caller holds mu.
func (r *accuracyRecorder) persistLocked() {
	if r.path == "" {
		return // in-memory only (data dir unavailable)
	}
	st := accuracyState{Version: accuracyStateVer, Slots: make([]slotAccuracy, 0, len(r.records))}
	for _, rec := range r.records {
		st.Slots = append(st.Slots, *rec)
	}
	sort.Slice(st.Slots, func(i, j int) bool { return st.Slots[i].Slot.Before(st.Slots[j].Slot) })
	data, err := json.MarshalIndent(&st, "", "  ")
	if err != nil {
		slog.Warn("accuracy: encode state failed", "error", err)
		return
	}
	if err := atomicWriteFile(r.path, data); err != nil {
		slog.Warn("accuracy: persist failed", "error", err)
	}
}

// load restores the window from disk. Missing = clean start; corrupt = warn +
// empty. Caller need not hold mu (called only from the constructor).
func (r *accuracyRecorder) load() {
	if r.path == "" {
		return
	}
	data, err := os.ReadFile(r.path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			slog.Warn("accuracy: state read failed — cold start", "error", err)
		}
		return
	}
	var st accuracyState
	if err := json.Unmarshal(data, &st); err != nil {
		slog.Warn("accuracy: state decode failed — cold start", "error", err)
		return
	}
	cutoff := time.Now().Add(-r.window)
	for i := range st.Slots {
		rec := st.Slots[i]
		if rec.Slot.Before(cutoff) {
			continue
		}
		r.records[rec.Slot.Unix()] = &rec
	}
}

// atomicWriteFile writes data to a temp file in the same directory and renames it
// over path, so a crash never leaves a half-written state file.
func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".accuracy-*.tmp")
	if err != nil {
		return fmt.Errorf("accuracy: temp file: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("accuracy: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("accuracy: sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("accuracy: close temp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("accuracy: rename state: %w", err)
	}
	return nil
}
