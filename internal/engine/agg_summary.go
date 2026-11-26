package engine

import (
	"sort"
	"time"

	"skewexec/internal/model"
)

type summaryOutput struct {
	m map[string]*summaryAcc
}

func (e *Engine) runSummaryAgg(parts []*aggPartition, traces []*PartitionTrace, tr *Trace, req AggRequest, byWh bool, totalStart time.Time) *model.Result {
	execStart := time.Now()
	var jobs []taskJob
	var outs []*summaryOutput

	for _, p := range parts {
		if len(p.rows) == 0 {
			continue
		}
		ranges := e.partitionRanges(p, p.trace.RequestedSplits)
		for i, rg := range ranges {
			out := &summaryOutput{m: make(map[string]*summaryAcc)}
			outs = append(outs, out)
			tt := &TaskTrace{
				Partition:     p.trace.Index,
				Scope:         "summary-partial",
				SplitIndex:    i,
				SplitCount:    len(ranges),
				RowsIn:        int64(rg[1] - rg[0]),
				EstimatedWork: int64(rg[1] - rg[0]),
			}
			p.trace.Tasks = append(p.trace.Tasks, tt)
			rng := rg
			jobs = append(jobs, taskJob{trace: tt, fn: func(tt *TaskTrace) {
				for j := rng[0]; j < rng[1]; j++ {
					f := p.rows[j]
					k := flowGroupKey(f, byWh)
					a := out.m[k]
					if a == nil {
						a = &summaryAcc{}
						out.m[k] = a
					}
					a.add(f, byWh)
				}
				tt.RowsOut = int64(len(out.m))
			}})
		}
	}

	e.runJobs(jobs, tr, totalStart)
	tr.ExecuteMs = elapsedMs(execStart)
	finishPartitionTraces(traces)

	mergeStart := time.Now()
	merged := make(map[string]*summaryAcc)
	for _, out := range outs {
		for k, a := range out.m {
			if merged[k] == nil {
				merged[k] = &summaryAcc{}
			}
			merged[k].merge(a)
		}
	}
	lines := make([]string, 0, len(merged))
	for _, a := range merged {
		lines = append(lines, a.line())
	}
	sort.Strings(lines)

	var fp1, fp2 uint64
	for _, line := range lines {
		fp1 += hashLine1(line)
		fp2 += hashLine2(line)
	}
	tr.Fingerprint1 = fp1
	tr.Fingerprint2 = fp2
	tr.ResultRows = len(lines)
	tr.MergeMs = elapsedMs(mergeStart)
	tr.TotalElapsedMs = elapsedMs(totalStart)

	return &model.Result{
		Kind:      req.Kind,
		Columns:   model.SummaryColumns,
		Lines:     lines,
		RowsCount: len(lines),
	}
}
