package engine

import (
	"runtime"
	"time"
)

type Config struct {
	Workers               int     `json:"workers"`
	Partitions            int     `json:"partitions"`
	SkewRatio             float64 `json:"skew_ratio"`
	SkewMinRows           int64   `json:"skew_min_rows"`
	AutoSplit             bool    `json:"auto_split"`
	DetectSkew            bool    `json:"detect_skew"`
	MinRowsPerPiece       int64   `json:"min_rows_per_piece"`
	TargetWorkPerPiece    int64   `json:"target_work_per_piece"`
	TargetAggRowsPerPiece int64   `json:"target_agg_rows_per_piece"`
	TaskOversubscribe     int     `json:"task_oversubscribe"`
	Collect               bool    `json:"collect"`
}

func DefaultConfig() Config {
	w := runtime.NumCPU()
	if w < 1 {
		w = 1
	}
	return Config{
		Workers:               w,
		Partitions:            16,
		SkewRatio:             3.0,
		SkewMinRows:           500,
		AutoSplit:             true,
		DetectSkew:            true,
		MinRowsPerPiece:       96,
		TargetWorkPerPiece:    90_000,
		TargetAggRowsPerPiece: 600,
		TaskOversubscribe:     2,
		Collect:               true,
	}
}

func (c *Config) Normalize() {
	def := DefaultConfig()
	if c.Workers <= 0 {
		c.Workers = def.Workers
	}
	if c.Partitions <= 0 {
		c.Partitions = def.Partitions
	}
	if c.SkewRatio <= 0 {
		c.SkewRatio = def.SkewRatio
	}
	if c.SkewMinRows <= 0 {
		c.SkewMinRows = def.SkewMinRows
	}
	if c.MinRowsPerPiece <= 0 {
		c.MinRowsPerPiece = def.MinRowsPerPiece
	}
	if c.TargetWorkPerPiece <= 0 {
		c.TargetWorkPerPiece = def.TargetWorkPerPiece
	}
	if c.TargetAggRowsPerPiece <= 0 {
		c.TargetAggRowsPerPiece = 600
	}
	if c.TaskOversubscribe <= 0 {
		c.TaskOversubscribe = def.TaskOversubscribe
	}
}

type Interval struct {
	TaskID    int     `json:"task_id"`
	StartMs   float64 `json:"start_ms"`
	EndMs     float64 `json:"end_ms"`
	Label     string  `json:"label"`
	Partition int     `json:"partition"`
}

type WorkerLoad struct {
	WorkerID  int        `json:"worker_id"`
	Tasks     int        `json:"tasks"`
	BusyMs    float64    `json:"busy_ms"`
	RowsIn    int64      `json:"rows_in"`
	WorkUnits int64      `json:"work_units"`
	RowsOut   int64      `json:"rows_out"`
	Intervals []Interval `json:"intervals"`
}

type TaskTrace struct {
	ID            int     `json:"id"`
	Partition     int     `json:"partition"`
	Scope         string  `json:"scope"`
	SplitIndex    int     `json:"split_index"`
	SplitCount    int     `json:"split_count"`
	WorkerID      int     `json:"worker_id"`
	StartMs       float64 `json:"start_ms"`
	DurationMs    float64 `json:"duration_ms"`
	RowsIn        int64   `json:"rows_in"`
	EstimatedWork int64   `json:"estimated_work"`
	MatchedPairs  int64   `json:"matched_pairs"`
	RowsOut       int64   `json:"rows_out"`
}

type PartitionTrace struct {
	Index            int          `json:"index"`
	LeftRows         int64        `json:"left_rows"`
	RightRows        int64        `json:"right_rows"`
	InputRows        int64        `json:"input_rows"`
	WorkUnits        int64        `json:"work_units"`
	PairWork         int64        `json:"pair_work"`
	MedianSize       float64      `json:"median_size"`
	RatioBoundary    float64      `json:"ratio_boundary"`
	AbsoluteBoundary int64        `json:"absolute_boundary"`
	Skewed           bool         `json:"skewed"`
	SplitEnabled     bool         `json:"split_enabled"`
	SuggestedSplits  int          `json:"suggested_splits"`
	RequestedSplits  int          `json:"requested_splits"`
	ActualTasks      int          `json:"actual_tasks"`
	Decision         string       `json:"decision"`
	SplitBasis       string       `json:"split_basis"`
	StartMs          float64      `json:"start_ms"`
	EndMs            float64      `json:"end_ms"`
	DurationMs       float64      `json:"duration_ms"`
	CpuMs            float64      `json:"cpu_ms"`
	Tasks            []*TaskTrace `json:"tasks"`
}

type Trace struct {
	QueryName        string            `json:"query_name"`
	QueryKind        string            `json:"query_kind"`
	Distribution     string            `json:"distribution,omitempty"`
	Scale            string            `json:"scale,omitempty"`
	Workers          int               `json:"workers"`
	RuntimeCPU       int               `json:"runtime_cpu"`
	Partitions       int               `json:"partition_count"`
	AutoSplit        bool              `json:"auto_split"`
	DetectSkew       bool              `json:"detect_skew"`
	SkewRatio        float64           `json:"skew_ratio"`
	SkewMinRows      int64             `json:"skew_min_rows"`
	TotalElapsedMs   float64           `json:"total_elapsed_ms"`
	PartitionMs      float64           `json:"partition_ms"`
	DetectMs         float64           `json:"detect_ms"`
	ExecuteMs        float64           `json:"execute_ms"`
	MergeMs          float64           `json:"merge_ms"`
	ResultRows       int               `json:"result_rows"`
	Fingerprint1     uint64            `json:"fingerprint1"`
	Fingerprint2     uint64            `json:"fingerprint2"`
	SkewPartitions   int               `json:"skew_partitions"`
	SplitPartitions  int               `json:"split_partitions"`
	TotalTasks       int               `json:"total_tasks"`
	MedianSize       float64           `json:"median_size"`
	RatioBoundary    float64           `json:"ratio_boundary"`
	AbsoluteBoundary int64             `json:"absolute_boundary"`
	PartitionsTrace  []*PartitionTrace `json:"partitions"`
	WorkersLoad      []*WorkerLoad     `json:"workers_load"`
	BaselineLoad     []*WorkerLoad     `json:"baseline_load"`
	GeneratedAt      time.Time         `json:"generated_at"`
}

type timed struct {
	start time.Time
}

func now() time.Time { return time.Now() }

func elapsedMs(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000.0
}
