package engine

import (
	"sort"
	"time"

	"skewexec/internal/model"
)

type recordOutput struct {
	m map[string][]model.FlowRecord
}

func (e *Engine) runTopRecordsAgg(parts []*aggPartition, traces []*PartitionTrace, tr *Trace, req AggRequest, byWh bool, totalStart time.Time) *model.Result {
	execStart := time.Now()
	var jobs []taskJob
	var outs []*recordOutput

	for _, p := range parts {
		if len(p.rows) == 0 {
			continue
		}
		ranges := e.partitionRanges(p, p.trace.RequestedSplits)
		for i, rg := range ranges {
			out := &recordOutput{m: make(map[string][]model.FlowRecord)}
			outs = append(outs, out)
			tt := &TaskTrace{
				Partition:     p.trace.Index,
				Scope:         "top-records-partial",
				SplitIndex:    i,
				SplitCount:    len(ranges),
				RowsIn:        int64(rg[1] - rg[0]),
				EstimatedWork: int64(rg[1]-rg[0]) * 4,
			}
			p.trace.Tasks = append(p.trace.Tasks, tt)
			rng := rg
			jobs = append(jobs, taskJob{trace: tt, fn: func(tt *TaskTrace) {
				for j := rng[0]; j < rng[1]; j++ {
					f := p.rows[j]
					k := flowGroupKey(f, byWh)
					out.m[k] = append(out.m[k], f)
				}
				groups := 0
				candidates := int64(0)
				for k, rs := range out.m {
					groups++
					kept := topNRecords(rs, req.TopN)
					out.m[k] = kept
					candidates += int64(len(kept))
				}
				tt.RowsOut = candidates
				_ = groups
			}})
		}
	}

	e.runJobs(jobs, tr, totalStart)
	tr.ExecuteMs = elapsedMs(execStart)
	finishPartitionTraces(traces)

	mergeStart := time.Now()
	merged := make(map[string][]model.FlowRecord)
	for _, out := range outs {
		for k, rs := range out.m {
			merged[k] = append(merged[k], rs...)
		}
	}

	lines := make([]string, 0)
	for _, rs := range merged {
		top := topNRecords(rs, req.TopN)
		for rank, f := range top {
			wh := "*"
			if byWh {
				wh = f.WhNo
			}
			lines = append(lines, model.TopRecordLine(
				f.ShipNo, wh, rank+1, f.Direction, f.UID, f.ID,
				f.Species, f.Weight, f.Boxes, f.Hour,
			))
		}
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

	return &model.Result{Kind: req.Kind, Columns: model.TopRecordColumns, Lines: lines, RowsCount: len(lines)}
}
