package hub

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeActuals is a deterministic actualsSource keyed by slot-start unix seconds.
type fakeActuals struct {
	pv   map[int64]float64
	grid map[int64]float64
	err  error
}

func (f fakeActuals) MeanPowerKW(_ context.Context, entityID string, from, _ time.Time) (float64, bool, error) {
	if f.err != nil {
		return 0, false, f.err
	}
	m := f.pv
	if entityID == "grid" {
		m = f.grid
	}
	v, ok := m[from.Unix()]
	return v, ok, nil
}

func newTestRecorder(t *testing.T, actuals actualsSource) *accuracyRecorder {
	t.Helper()
	return newAccuracyRecorder(t.TempDir(), 30*time.Minute, actuals, "pv", "grid")
}

func TestRecordCapturesPredictionsOnce(t *testing.T) {
	r := newTestRecorder(t, nil)
	slot := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)

	r.Record(accuracyTick{Now: slot, SolcastKW: 1.0, HasSolcast: true, HasModel: true, ModelKW: 1.2,
		HasPlan: true, PlannedSOC: 0.5, PlannedGrid: -0.3})
	// A later sighting of the same slot must not overwrite the first prediction.
	r.Record(accuracyTick{Now: slot.Add(3 * time.Minute), SolcastKW: 9.9, HasSolcast: true})

	rec := r.records[slot.Unix()]
	if rec == nil {
		t.Fatal("no record for slot")
	}
	if rec.SolcastKW != 1.0 || rec.ModelKW != 1.2 {
		t.Errorf("prediction overwritten: solcast=%.1f model=%.1f", rec.SolcastKW, rec.ModelKW)
	}
	if !rec.HasPlan || rec.PlannedSOC != 0.5 || rec.PlannedGridKW != -0.3 {
		t.Errorf("plan not captured: %+v", rec)
	}
}

func TestSoCPairedAtSlotBoundary(t *testing.T) {
	r := newTestRecorder(t, nil)
	slot0 := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	slot1 := slot0.Add(30 * time.Minute)

	r.Record(accuracyTick{Now: slot0, HasPlan: true, PlannedSOC: 0.5, MeasuredSOC: 0.40})
	// Crossing into slot1: the SoC now is the actual end-of-slot0 SoC.
	r.Record(accuracyTick{Now: slot1, HasPlan: true, PlannedSOC: 0.6, MeasuredSOC: 0.55})

	rec := r.records[slot0.Unix()]
	if !rec.HasActualSOC || rec.ActualSOC != 0.55 {
		t.Errorf("slot0 SoC actual = (%v, %.2f), want (true, 0.55)", rec.HasActualSOC, rec.ActualSOC)
	}
}

func TestSoCUnknownNotRecorded(t *testing.T) {
	r := newTestRecorder(t, nil)
	slot0 := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	r.Record(accuracyTick{Now: slot0, HasPlan: true, PlannedSOC: 0.5})
	// MeasuredSOC 0 = unknown SoC → must not pair a spurious 0 actual.
	r.Record(accuracyTick{Now: slot0.Add(30 * time.Minute), MeasuredSOC: 0})
	if r.records[slot0.Unix()].HasActualSOC {
		t.Error("unknown SoC (0) was recorded as an actual")
	}
}

func TestResolveActualsFillsElapsedSlots(t *testing.T) {
	slot0 := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	fa := fakeActuals{
		pv:   map[int64]float64{slot0.Unix(): 3.4},
		grid: map[int64]float64{slot0.Unix(): -1.1},
	}
	r := newTestRecorder(t, fa)
	r.Record(accuracyTick{Now: slot0, HasSolcast: true, SolcastKW: 3.0})

	// Before the slot elapses, nothing resolves.
	r.ResolveActuals(context.Background(), slot0.Add(10*time.Minute))
	if r.records[slot0.Unix()].HasActualPV {
		t.Error("resolved PV before the slot elapsed")
	}
	// At the slot end, the resolve grace still holds it back (ingestion lag guard).
	r.ResolveActuals(context.Background(), slot0.Add(30*time.Minute))
	if r.records[slot0.Unix()].HasActualPV {
		t.Error("resolved PV at slot end, before the resolve grace elapsed")
	}
	// Once elapsed + grace, PV+grid land and the slot is marked resolved.
	r.ResolveActuals(context.Background(), slot0.Add(30*time.Minute+accuracyResolveGrace))
	rec := r.records[slot0.Unix()]
	if !rec.HasActualPV || rec.ActualPVKW != 3.4 {
		t.Errorf("PV actual = (%v, %.2f), want (true, 3.4)", rec.HasActualPV, rec.ActualPVKW)
	}
	if !rec.HasActualGrid || rec.ActualGridKW != -1.1 {
		t.Errorf("grid actual = (%v, %.2f), want (true, -1.1)", rec.HasActualGrid, rec.ActualGridKW)
	}
	if !rec.PVGridResolved {
		t.Error("slot not marked resolved")
	}
}

func TestResolveActualsErrorRetries(t *testing.T) {
	slot0 := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	r := newTestRecorder(t, fakeActuals{err: errors.New("vm down")})
	r.Record(accuracyTick{Now: slot0, HasSolcast: true, SolcastKW: 3.0})
	r.ResolveActuals(context.Background(), slot0.Add(30*time.Minute))
	rec := r.records[slot0.Unix()]
	if rec.HasActualPV || rec.PVGridResolved {
		t.Error("a failed lookup must leave the slot unresolved for retry")
	}
}

func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	slot0 := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)

	r1 := newAccuracyRecorder(dir, 30*time.Minute, nil, "pv", "grid")
	r1.Record(accuracyTick{Now: slot0, HasSolcast: true, SolcastKW: 2.2, HasPlan: true, PlannedSOC: 0.7})

	r2 := newAccuracyRecorder(dir, 30*time.Minute, nil, "pv", "grid")
	rec := r2.records[slot0.Unix()]
	if rec == nil || rec.SolcastKW != 2.2 || rec.PlannedSOC != 0.7 {
		t.Fatalf("state did not survive reload: %+v", rec)
	}
}

func TestPruneDropsOldSlots(t *testing.T) {
	r := newTestRecorder(t, nil)
	old := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	fresh := old.Add(8 * 24 * time.Hour) // > 7-day window later

	r.Record(accuracyTick{Now: old, HasSolcast: true, SolcastKW: 1})
	r.Record(accuracyTick{Now: fresh, HasSolcast: true, SolcastKW: 2})

	if _, ok := r.records[old.Unix()]; ok {
		t.Error("slot older than the window was not pruned")
	}
	if _, ok := r.records[fresh.Unix()]; !ok {
		t.Error("in-window slot was pruned")
	}
}

func TestSnapshotSortedWithMeta(t *testing.T) {
	r := newTestRecorder(t, nil)
	base := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	r.Record(accuracyTick{Now: base.Add(30 * time.Minute), HasSolcast: true, SolcastKW: 2})
	r.Record(accuracyTick{Now: base, HasSolcast: true, SolcastKW: 1})

	snap := r.Snapshot()
	if len(snap.Points) != 2 {
		t.Fatalf("want 2 points, got %d", len(snap.Points))
	}
	if snap.Points[0].Slot.After(snap.Points[1].Slot) {
		t.Error("points not sorted ascending by slot")
	}
	if snap.WindowH != 168 || snap.SlotMin != 30 {
		t.Errorf("meta = (%.0fh, %.0fmin), want (168, 30)", snap.WindowH, snap.SlotMin)
	}
}
