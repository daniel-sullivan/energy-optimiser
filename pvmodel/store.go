package pvmodel

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	stateFileName = "pvmodel.json"
	rawFileName   = "pvmodel_raw.jsonl"
	stateVersion  = 1
	dayLayout     = "2006-01-02"
)

// day is a UTC civil date. The watermark and per-day validation work in UTC
// calendar days to match the UTC timestamps of GTI and metrics samples.
type day struct {
	Year  int
	Month time.Month
	Day   int
}

func dayOf(t time.Time) day {
	u := t.UTC()
	return day{Year: u.Year(), Month: u.Month(), Day: u.Day()}
}

func (d day) time() time.Time  { return time.Date(d.Year, d.Month, d.Day, 0, 0, 0, 0, time.UTC) }
func (d day) add(n int) day    { return dayOf(d.time().AddDate(0, 0, n)) }
func (d day) after(o day) bool { return d.time().After(o.time()) }
func (d day) zero() bool       { return d.Year == 0 }

func (d day) MarshalJSON() ([]byte, error) {
	if d.zero() {
		return []byte(`""`), nil
	}
	return json.Marshal(d.time().Format(dayLayout))
}

func (d *day) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if s == "" {
		*d = day{}
		return nil
	}
	t, err := time.Parse(dayLayout, s)
	if err != nil {
		return err
	}
	*d = dayOf(t)
	return nil
}

// RawRecord is one day×hour raw aggregate, appended to the append-only JSONL log
// so the binning/decay can be re-fit from history later. It is separate from the
// decayed bins and also feeds the rolling kWp_ref computation.
type RawRecord struct {
	Hour     time.Time `json:"hour"`       // start of the hour, UTC
	PVMeanKW float64   `json:"pv_mean_kw"` // mean measured PV over the hour (kW)
	GTISum   float64   `json:"gti_sum"`    // Σ per-site GTI for the hour (W/m²)
	Samples  int       `json:"samples"`    // number of raw PV samples aggregated
}

// binEntry is the on-disk form of a non-empty bin.
type binEntry struct {
	HalfMonth int      `json:"half_month"`
	Hour      int      `json:"hour"`
	Stats     binStats `json:"stats"`
}

// persistState is the authoritative JSON state written atomically to DataDir.
type persistState struct {
	Version         int        `json:"version"`
	KWpRef          float64    `json:"kwp_ref"`
	LastIngestedDay day        `json:"last_ingested_day"`
	Global          binStats   `json:"global"`
	Bins            []binEntry `json:"bins"`
}

func ensureDir(dir string) error {
	if dir == "" {
		return errors.New("pvmodel: empty data dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("pvmodel: preparing data dir: %w", err)
	}
	return nil
}

func (m *Model) statePath() string { return filepath.Join(m.dataDir, stateFileName) }
func (m *Model) rawPath() string   { return filepath.Join(m.dataDir, rawFileName) }

// load restores state from DataDir. A missing file is a silent clean cold start;
// a corrupt file cold-starts with a warning. Either way the model stays empty
// and usable — never a panic.
func (m *Model) load() {
	data, err := os.ReadFile(m.statePath())
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			m.logColdStart("state read", err)
		}
		return
	}
	var st persistState
	if err := json.Unmarshal(data, &st); err != nil {
		m.logColdStart("state decode", err)
		return
	}
	m.kWpRef = st.KWpRef
	m.lastIngestedDay = st.LastIngestedDay
	m.global = st.Global
	for _, e := range st.Bins {
		if e.HalfMonth < 0 || e.HalfMonth >= numHalfMonths || e.Hour < 0 || e.Hour >= numHours {
			continue
		}
		m.bins[e.HalfMonth*numHours+e.Hour] = e.Stats
	}
}

// persist writes the full state atomically (temp file + rename).
func (m *Model) persist() error {
	st := persistState{
		Version:         stateVersion,
		KWpRef:          m.kWpRef,
		LastIngestedDay: m.lastIngestedDay,
		Global:          m.global,
	}
	for i := range m.bins {
		b := m.bins[i]
		if b.NEff == 0 && b.SumPV == 0 && b.SumGTI == 0 {
			continue
		}
		st.Bins = append(st.Bins, binEntry{HalfMonth: i / numHours, Hour: i % numHours, Stats: b})
	}
	data, err := json.MarshalIndent(&st, "", "  ")
	if err != nil {
		return fmt.Errorf("pvmodel: encode state: %w", err)
	}
	return atomicWrite(m.statePath(), data)
}

// atomicWrite writes data to a temp file in the same directory and renames it
// over path, so a crash never leaves a half-written state file.
func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".pvmodel-*.tmp")
	if err != nil {
		return fmt.Errorf("pvmodel: temp file: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("pvmodel: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("pvmodel: sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("pvmodel: close temp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("pvmodel: rename state: %w", err)
	}
	return nil
}

// loadRaw reads the append-only raw log, skipping unparseable lines (a truncated
// tail from a crash mid-append must not stop the model loading).
func (m *Model) loadRaw() []RawRecord {
	f, err := os.Open(m.rawPath())
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			m.logColdStart("raw open", err)
		}
		return nil
	}
	defer func() { _ = f.Close() }()

	var out []RawRecord
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r RawRecord
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			continue
		}
		out = append(out, r)
	}
	return out
}

// appendRaw appends new raw records as JSON lines to the append-only log.
func (m *Model) appendRaw(records []RawRecord) error {
	if len(records) == 0 {
		return nil
	}
	f, err := os.OpenFile(m.rawPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("pvmodel: open raw log: %w", err)
	}
	defer func() { _ = f.Close() }()

	w := bufio.NewWriter(f)
	for i := range records {
		line, err := json.Marshal(&records[i])
		if err != nil {
			return fmt.Errorf("pvmodel: encode raw: %w", err)
		}
		if _, err := w.Write(line); err != nil {
			return fmt.Errorf("pvmodel: write raw: %w", err)
		}
		if err := w.WriteByte('\n'); err != nil {
			return fmt.Errorf("pvmodel: write raw: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("pvmodel: flush raw: %w", err)
	}
	return nil
}
