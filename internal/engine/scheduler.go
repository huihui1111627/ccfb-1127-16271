package engine

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type Engine struct {
	cfg Config
}

func New(cfg Config) *Engine {
	cfg.Normalize()
	return &Engine{cfg: cfg}
}

func (e *Engine) Config() Config {
	return e.cfg
}

type taskJob struct {
	trace *TaskTrace
	fn    func(tt *TaskTrace)
}

func (e *Engine) runJobs(jobs []taskJob, tr *Trace, runStart time.Time) {
	w := e.cfg.Workers
	if w < 1 {
		w = 1
	}

	loads := make([]*WorkerLoad, w)
	for i := range loads {
		loads[i] = &WorkerLoad{WorkerID: i, Intervals: make([]Interval, 0)}
	}

	jobCh := make(chan int)
	var wg sync.WaitGroup
	for wid := 0; wid < w; wid++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			load := loads[wid]
			for idx := range jobCh {
				job := jobs[idx]
				tt := job.trace
				tt.ID = idx
				tt.WorkerID = wid
				start := time.Now()
				tt.StartMs = float64(start.Sub(runStart).Microseconds()) / 1000.0

				job.fn(tt)

				end := time.Now()
				tt.DurationMs = float64(end.Sub(start).Microseconds()) / 1000.0
				load.Tasks++
				load.BusyMs += tt.DurationMs
				load.RowsIn += tt.RowsIn
				load.WorkUnits += tt.EstimatedWork
				load.RowsOut += tt.RowsOut
				load.Intervals = append(load.Intervals, Interval{
					TaskID:    tt.ID,
					StartMs:   tt.StartMs,
					EndMs:     tt.StartMs + tt.DurationMs,
					Label:     tt.Scope,
					Partition: tt.Partition,
				})
			}
		}(wid)
	}

	for i := range jobs {
		jobCh <- i
	}
	close(jobCh)
	wg.Wait()

	tr.WorkersLoad = loads
	tr.TotalTasks = len(jobs)
}

func finishPartitionTraces(parts []*PartitionTrace) {
	for _, p := range parts {
		if len(p.Tasks) == 0 {
			continue
		}
		minStart := p.Tasks[0].StartMs
		maxEnd := 0.0
		cpu := 0.0
		for _, t := range p.Tasks {
			if t.StartMs < minStart {
				minStart = t.StartMs
			}
			end := t.StartMs + t.DurationMs
			if end > maxEnd {
				maxEnd = end
			}
			cpu += t.DurationMs
		}
		p.StartMs = minStart
		p.EndMs = maxEnd
		p.DurationMs = maxEnd - minStart
		p.CpuMs = cpu
		p.ActualTasks = len(p.Tasks)
	}
}

func buildBaselineLoad(parts []*PartitionTrace, workers int) []*WorkerLoad {
	if workers < 1 {
		workers = 1
	}
	loads := make([]*WorkerLoad, workers)
	for i := range loads {
		loads[i] = &WorkerLoad{WorkerID: i, Intervals: []Interval{}}
	}
	for _, p := range parts {
		wid := p.Index % workers
		unit := p.WorkUnits
		if unit < p.InputRows {
			unit = p.InputRows
		}
		l := loads[wid]
		l.Tasks++
		l.RowsIn += p.InputRows
		l.WorkUnits += unit
	}
	return loads
}

func medianInt64(values []int64) float64 {
	if len(values) == 0 {
		return 0
	}
	cp := append([]int64(nil), values...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	n := len(cp)
	if n%2 == 1 {
		return float64(cp[n/2])
	}
	return float64(cp[n/2-1]+cp[n/2]) / 2.0
}

func ceilDiv(a, b int64) int64 {
	if a <= 0 {
		return 0
	}
	if b <= 0 {
		return a
	}
	return (a + b - 1) / b
}

// decideSplits returns the number of parallel pieces for one skewed partition.
// The decision is deliberately explicit so the web UI can show its basis.
func decideSplits(maxSideRows, workUnits int64, workers int, cfg Config, skewed bool) (int, string) {
	cfg.Normalize()
	if workUnits < maxSideRows {
		workUnits = maxSideRows
	}
	byWork := ceilDiv(workUnits, cfg.TargetWorkPerPiece)
	if byWork < 1 {
		byWork = 1
	}
	byRows := maxSideRows / cfg.MinRowsPerPiece
	if byRows < 1 && maxSideRows > 0 {
		byRows = 1
	}
	threadCap := int64(workers * cfg.TaskOversubscribe)
	if threadCap < 1 {
		threadCap = 1
	}
	pieces := byWork
	if byRows > 0 && pieces > byRows {
		pieces = byRows
	}
	if pieces > threadCap {
		pieces = threadCap
	}
	if skewed && pieces < 2 && maxSideRows >= 2 {
		pieces = 2
	}
	if pieces < 1 {
		pieces = 1
	}

	basis := fmt.Sprintf(
		"最大输入侧=%d 行；估算工作单元=%d；按目标 %d 单元/片需 %d 片；按每片至少 %d 行最多 %d 片；%d 线程×%d 超订系数=%d 片上限；倾斜保底两片=%v；最终 %d 片",
		maxSideRows, workUnits, cfg.TargetWorkPerPiece, byWork,
		cfg.MinRowsPerPiece, byRows, workers, cfg.TaskOversubscribe, threadCap, skewed, pieces,
	)
	return int(pieces), basis
}
