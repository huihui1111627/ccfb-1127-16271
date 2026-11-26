package acceptance

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"time"

	"skewexec/internal/data"
	"skewexec/internal/engine"
)

type DurationStats struct {
	RunsMS   []float64 `json:"runs_ms"`
	MinMS    float64   `json:"min_ms"`
	MedianMS float64   `json:"median_ms"`
	MeanMS   float64   `json:"mean_ms"`
	MaxMS    float64   `json:"max_ms"`
}

type CaseReport struct {
	Distribution    string        `json:"distribution"`
	QueryID         string        `json:"query_id"`
	QueryName       string        `json:"query_name"`
	Group           string        `json:"group"`
	Pass            bool          `json:"pass"`
	Correctness     Correctness   `json:"correctness"`
	Off             DurationStats `json:"off"`
	On              DurationStats `json:"on"`
	SpeedupMedian   float64       `json:"speedup_median"`
	Workers         int           `json:"workers"`
	Partitions      int           `json:"partition_count"`
	SkewPartitions  int           `json:"skew_partitions"`
	SplitPartitions int           `json:"split_partitions"`
	TotalTasksOn    int           `json:"total_tasks_on"`
	DetectMsOn      float64       `json:"detect_ms_on"`
	ExecuteMsOn     float64       `json:"execute_ms_on"`
	TotalMsOn       float64       `json:"total_ms_on"`
	TotalMsOff      float64       `json:"total_ms_off"`
	Error           string        `json:"error,omitempty"`
}

type Report struct {
	GeneratedAt time.Time    `json:"generated_at"`
	GoVersion   string       `json:"go_version"`
	OS          string       `json:"os"`
	Arch        string       `json:"arch"`
	CPUCount    int          `json:"cpu_count"`
	Workers     int          `json:"workers"`
	Partitions  int          `json:"partitions"`
	SkewRatio   float64      `json:"skew_ratio"`
	SkewMinRows int64        `json:"skew_min_rows"`
	Repeats     int          `json:"repeats"`
	Scale       string       `json:"scale"`
	AllPass     bool         `json:"all_pass"`
	PassedCount int          `json:"passed_count"`
	TotalCount  int          `json:"total_count"`
	Cases       []CaseReport `json:"cases"`
}

type Options struct {
	Scale   string
	Workers int
	Repeats int
	Base    engine.Config
}

func medianDuration(xs []float64) float64 {
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	n := len(cp)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return cp[n/2]
	}
	return (cp[n/2-1] + cp[n/2]) / 2
}

func statsFromRuns(xs []float64) DurationStats {
	if len(xs) == 0 {
		return DurationStats{}
	}
	var sum float64
	mn, mx := xs[0], xs[0]
	for _, x := range xs {
		sum += x
		mn = math.Min(mn, x)
		mx = math.Max(mx, x)
	}
	return DurationStats{
		RunsMS:   append([]float64(nil), xs...),
		MinMS:    mn,
		MedianMS: medianDuration(xs),
		MeanMS:   sum / float64(len(xs)),
		MaxMS:    mx,
	}
}

func Benchmark(store *data.Store, q QuerySpec, cfg engine.Config, repeats int) (DurationStats, *engine.Trace) {
	return benchmark(store, q, cfg, repeats)
}

func benchmark(store *data.Store, q QuerySpec, cfg engine.Config, repeats int) (DurationStats, *engine.Trace) {
	// Warm up maps/scheduler and code paths.
	_, _ = RunOptimized(store, q, cfg)
	runs := make([]float64, 0, repeats)
	var medianTrace *engine.Trace
	for i := 0; i < repeats; i++ {
		runtime.GC()
		start := time.Now()
		_, tr := RunOptimized(store, q, cfg)
		elapsed := float64(time.Since(start).Microseconds()) / 1000.0
		runs = append(runs, elapsed)
		medianTrace = tr
	}
	return statsFromRuns(runs), medianTrace
}

func RunAll(opts Options) Report {
	base := opts.Base
	base.Normalize()
	if opts.Workers > 0 {
		base.Workers = opts.Workers
	}
	repeats := opts.Repeats
	if repeats <= 0 {
		repeats = 5
	}
	scale := opts.Scale
	if scale == "" {
		scale = data.ScaleAcceptance
	}

	report := Report{
		GeneratedAt: time.Now(),
		GoVersion:   runtime.Version(),
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		CPUCount:    runtime.NumCPU(),
		Workers:     base.Workers,
		Partitions:  base.Partitions,
		SkewRatio:   base.SkewRatio,
		SkewMinRows: base.SkewMinRows,
		Repeats:     repeats,
		Scale:       scale,
		AllPass:     true,
	}

	distributions := []string{data.DistUniform, data.DistLongTail, data.DistExtreme}
	queries := Queries()
	for _, dist := range distributions {
		cfg := data.DefaultConfig(dist, scale)
		store := data.Generate(cfg)
		for _, q := range queries {
			cr := CaseReport{
				Distribution: dist,
				QueryID:      q.ID,
				QueryName:    q.Name,
				Group:        q.Group,
				Workers:      base.Workers,
				Partitions:   base.Partitions,
			}

			func() {
				defer func() {
					if rec := recover(); rec != nil {
						cr.Pass = false
						cr.Error = "panic: " + toString(rec)
					}
				}()

				cr.Correctness = Check(store, q, base)
				cr.Pass = cr.Correctness.Pass

				offCfg := base
				offCfg.AutoSplit = false
				offCfg.Collect = true
				offStats, offTrace := benchmark(store, q, offCfg, repeats)
				cr.Off = offStats
				if offTrace != nil {
					cr.TotalMsOff = offTrace.TotalElapsedMs
				}

				onCfg := base
				onCfg.AutoSplit = true
				onCfg.Collect = true
				onStats, onTrace := benchmark(store, q, onCfg, repeats)
				cr.On = onStats
				if onTrace != nil {
					cr.SkewPartitions = onTrace.SkewPartitions
					cr.SplitPartitions = onTrace.SplitPartitions
					cr.TotalTasksOn = onTrace.TotalTasks
					cr.DetectMsOn = onTrace.DetectMs
					cr.ExecuteMsOn = onTrace.ExecuteMs
					cr.TotalMsOn = onTrace.TotalElapsedMs
				}
				if cr.Off.MedianMS > 0 {
					cr.SpeedupMedian = cr.Off.MedianMS / cr.On.MedianMS
				}
			}()

			if cr.Pass {
				report.PassedCount++
			} else {
				report.AllPass = false
			}
			report.TotalCount++
			report.Cases = append(report.Cases, cr)
		}
	}
	return report
}

func toString(v any) string {
	return fmt.Sprint(v)
}
