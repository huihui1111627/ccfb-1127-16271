package engine

import (
	"sort"
	"time"

	"skewexec/internal/model"
)

type speciesOutput struct {
	m map[string]map[string]*speciesAcc
}

func (e *Engine) runTopSpeciesAgg(parts []*aggPartition, traces []*PartitionTrace, tr *Trace, req AggRequest, totalStart time.Time) *model.Result {
	execStart := time.Now()
	var jobs []taskJob
	var outs []*speciesOutput

	for _, p := range parts {
		if len(p.rows) == 0 {
			continue
		}
		ranges := e.partitionRanges(p, p.trace.RequestedSplits)
		for i, rg := range ranges {
			out := &speciesOutput{m: make(map[string]map[string]*speciesAcc)}
			outs = append(outs, out)
			tt := &TaskTrace{
				Partition:     p.trace.Index,
				Scope:         "species-partial",
				SplitIndex:    i,
				SplitCount:    len(ranges),
				RowsIn:        int64(rg[1] - rg[0]),
				EstimatedWork: int64(rg[1]-rg[0]) * 2,
			}
			p.trace.Tasks = append(p.trace.Tasks, tt)
			rng := rg
			jobs = append(jobs, taskJob{trace: tt, fn: func(tt *TaskTrace) {
				for j := rng[0]; j < rng[1]; j++ {
					f := p.rows[j]
					bySpecies := out.m[f.ShipNo]
					if bySpecies == nil {
						bySpecies = make(map[string]*speciesAcc)
						out.m[f.ShipNo] = bySpecies
					}
					a := bySpecies[f.Species]
					if a == nil {
						a = &speciesAcc{}
						bySpecies[f.Species] = a
					}
					a.Count++
					a.Weight += f.Weight
					a.Boxes += f.Boxes
				}
				groups := 0
				for _, sm := range out.m {
					groups += len(sm)
				}
				tt.RowsOut = int64(groups)
			}})
		}
	}

	e.runJobs(jobs, tr, totalStart)
	tr.ExecuteMs = elapsedMs(execStart)
	finishPartitionTraces(traces)

	mergeStart := time.Now()
	merged := make(map[string]map[string]*speciesAcc)
	for _, out := range outs {
		for ship, sm := range out.m {
			target := merged[ship]
			if target == nil {
				target = make(map[string]*speciesAcc)
				merged[ship] = target
			}
			for species, a := range sm {
				b := target[species]
				if b == nil {
					b = &speciesAcc{}
					target[species] = b
				}
				b.Count += a.Count
				b.Weight += a.Weight
				b.Boxes += a.Boxes
			}
		}
	}

	lines := make([]string, 0)
	for ship, sm := range merged {
		type item struct {
			species string
			acc     *speciesAcc
		}
		items := make([]item, 0, len(sm))
		for sp, a := range sm {
			items = append(items, item{sp, a})
		}
		sort.Slice(items, func(i, j int) bool {
			if items[i].acc.Weight != items[j].acc.Weight {
				return items[i].acc.Weight > items[j].acc.Weight
			}
			return items[i].species < items[j].species
		})
		limit := req.TopN
		if limit > len(items) {
			limit = len(items)
		}
		for rank := 0; rank < limit; rank++ {
			it := items[rank]
			lines = append(lines, model.TopSpeciesLine(
				ship, rank+1, it.species, it.acc.Count, it.acc.Weight, it.acc.Boxes,
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

	return &model.Result{Kind: req.Kind, Columns: model.TopSpeciesColumns, Lines: lines, RowsCount: len(lines)}
}
