package web

import (
	"encoding/json"
	"net/http"
	"runtime"
	"strconv"
	"sync"

	"skewexec/internal/acceptance"
	"skewexec/internal/data"
	"skewexec/internal/engine"
)

type Server struct {
	storeCache sync.Map
}

func New() *Server {
	return &Server{}
}

type runSide struct {
	Stats acceptance.DurationStats `json:"stats"`
	Trace *engine.Trace            `json:"trace"`
}

type compareResponse struct {
	Query           acceptance.QuerySpec    `json:"query"`
	Distribution    string                  `json:"distribution"`
	Scale           string                  `json:"scale"`
	Config          engine.Config           `json:"config"`
	Repeats         int                     `json:"repeats"`
	Off             runSide                 `json:"off"`
	On              runSide                 `json:"on"`
	SameRowCount    bool                    `json:"same_row_count"`
	SameFingerprint bool                    `json:"same_fingerprint"`
	SpeedupMedian   float64                 `json:"speedup_median"`
	Correctness     *acceptance.Correctness `json:"correctness,omitempty"`
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/queries", s.handleQueries)
	mux.HandleFunc("/api/run", s.handleRun)
	mux.HandleFunc("/api/compare", s.handleCompare)
	mux.HandleFunc("/api/acceptance/run", s.handleAcceptanceRun)
	mux.HandleFunc("/api/acceptance/report", s.handleAcceptanceReport)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func (s *Server) handleQueries(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"queries":       acceptance.Queries(),
		"distributions": []string{data.DistUniform, data.DistLongTail, data.DistExtreme},
		"scales":        []string{data.ScaleSmall, data.ScaleAcceptance, data.ScaleStandard, data.ScaleLarge},
		"cpu_count":     runtime.NumCPU(),
	})
}

func (s *Server) getStore(dist, scale string) *data.Store {
	if dist == "" {
		dist = data.DistExtreme
	}
	if scale == "" {
		scale = data.ScaleAcceptance
	}
	key := dist + "|" + scale
	if v, ok := s.storeCache.Load(key); ok {
		return v.(*data.Store)
	}
	store := data.Generate(data.DefaultConfig(dist, scale))
	actual, _ := s.storeCache.LoadOrStore(key, store)
	return actual.(*data.Store)
}

func parseConfig(r *http.Request) engine.Config {
	cfg := engine.DefaultConfig()
	q := r.URL.Query()
	if v := q.Get("workers"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Workers = n
		}
	}
	if v := q.Get("partitions"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Partitions = n
		}
	}
	if v := q.Get("ratio"); v != "" {
		if x, err := strconv.ParseFloat(v, 64); err == nil && x > 0 {
			cfg.SkewRatio = x
		}
	}
	if v := q.Get("min_rows"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			cfg.SkewMinRows = n
		}
	}
	if v := q.Get("target_agg_rows"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cfg.TargetAggRowsPerPiece = n
		}
	}
	// Materialize canonical rows for the fixed acceptance-sized data so web
	// timings match the CLI report. Larger scales stream/hash rows to avoid
	// retaining millions of display strings in the server process.
	scale := q.Get("scale")
	cfg.Collect = scale == "" || scale == data.ScaleSmall || scale == data.ScaleAcceptance
	cfg.AutoSplit = q.Get("split") != "0" && q.Get("auto_split") != "false"
	return cfg
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	spec := acceptance.QueryByID(q.Get("query"))
	dist := q.Get("dist")
	scale := q.Get("scale")
	store := s.getStore(dist, scale)
	cfg := parseConfig(r)
	cfg.AutoSplit = q.Get("split") != "0"
	res, trace := acceptance.RunOptimized(store, spec, cfg)
	writeJSON(w, http.StatusOK, map[string]any{
		"query":  spec,
		"config": cfg,
		"trace":  trace,
		"rows":   res.RowCount(),
	})
}

func (s *Server) handleCompare(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	spec := acceptance.QueryByID(q.Get("query"))
	dist := q.Get("dist")
	scale := q.Get("scale")
	if scale == "" {
		scale = data.ScaleAcceptance
	}
	store := s.getStore(dist, scale)
	cfg := parseConfig(r)

	repeats := 3
	if v := q.Get("repeats"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 20 {
			repeats = n
		}
	}

	offCfg := cfg
	offCfg.AutoSplit = false
	offStats, offTrace := acceptance.Benchmark(store, spec, offCfg, repeats)

	onCfg := cfg
	onCfg.AutoSplit = true
	onStats, onTrace := acceptance.Benchmark(store, spec, onCfg, repeats)

	resp := compareResponse{
		Query:        spec,
		Distribution: store.Config.Distribution,
		Scale:        scale,
		Config:       cfg,
		Repeats:      repeats,
		Off:          runSide{Stats: offStats, Trace: offTrace},
		On:           runSide{Stats: onStats, Trace: onTrace},
	}
	if offTrace != nil && onTrace != nil {
		resp.SameRowCount = offTrace.ResultRows == onTrace.ResultRows
		resp.SameFingerprint = offTrace.Fingerprint1 == onTrace.Fingerprint1 &&
			offTrace.Fingerprint2 == onTrace.Fingerprint2 &&
			offTrace.ResultRows == onTrace.ResultRows
	}
	if offStats.MedianMS > 0 && onStats.MedianMS > 0 {
		resp.SpeedupMedian = offStats.MedianMS / onStats.MedianMS
	}

	// Exact naive verification is fast enough on small/acceptance fixed data.
	if q.Get("verify") == "1" || ((scale == data.ScaleSmall || scale == data.ScaleAcceptance) && q.Get("verify") != "0") {
		verifyCfg := cfg
		verifyCfg.Collect = true
		corr := acceptance.Check(store, spec, verifyCfg)
		resp.Correctness = &corr
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAcceptanceRun(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	workers, _ := strconv.Atoi(q.Get("workers"))
	repeats := 1
	if v := q.Get("repeats"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 10 {
			repeats = n
		}
	}
	scale := q.Get("scale")
	if scale == "" {
		scale = data.ScaleSmall
	}
	base := engine.DefaultConfig()
	if workers > 0 {
		base.Workers = workers
	}
	if v := q.Get("ratio"); v != "" {
		if x, err := strconv.ParseFloat(v, 64); err == nil && x > 0 {
			base.SkewRatio = x
		}
	}
	if v := q.Get("min_rows"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			base.SkewMinRows = n
		}
	}
	report := acceptance.RunAll(acceptance.Options{Scale: scale, Workers: workers, Repeats: repeats, Base: base})
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleAcceptanceReport(w http.ResponseWriter, r *http.Request) {
	// Convenience endpoint for a report generated by cmd/acceptance.
	http.ServeFile(w, r, "acceptance_report.json")
}
