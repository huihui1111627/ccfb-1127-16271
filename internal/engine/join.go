package engine

import (
	"time"

	"skewexec/internal/model"
)

const (
	JoinInner = "inner"
	JoinLeft  = "left"
	JoinFull  = "full"
)

type JoinRequest struct {
	QueryName    string
	Distribution string
	Scale        string
	JoinType     string
	Left         []model.Detail
	Right        []model.Detail
}

type joinPartition struct {
	trace       *PartitionTrace
	left        []model.Detail
	right       []model.Detail
	leftGroups  map[string][]model.Detail
	rightGroups map[string][]model.Detail
	pairWork    int64
}

func (e *Engine) RunJoin(req JoinRequest) (*model.Result, *Trace) {
	cfg := e.cfg
	cfg.Normalize()
	totalStart := time.Now()
	tr := &Trace{
		QueryName:    req.QueryName,
		QueryKind:    "join_" + req.JoinType,
		Distribution: req.Distribution,
		Scale:        req.Scale,
		Workers:      cfg.Workers,
		RuntimeCPU:   runtimeNumCPU(),
		Partitions:   cfg.Partitions,
		AutoSplit:    cfg.AutoSplit,
		DetectSkew:   cfg.DetectSkew,
		SkewRatio:    cfg.SkewRatio,
		SkewMinRows:  cfg.SkewMinRows,
		GeneratedAt:  totalStart,
	}

	// Partition and build lookup indexes. Partitioning by ship number is a
	// valid prefix for the composite join key (ship_no, wh_no): equal composite
	// keys always land in the same partition.
	partitionStart := time.Now()
	parts := make([]*joinPartition, cfg.Partitions)
	traces := make([]*PartitionTrace, cfg.Partitions)
	for i := 0; i < cfg.Partitions; i++ {
		pt := &PartitionTrace{Index: i, Tasks: []*TaskTrace{}}
		traces[i] = pt
		parts[i] = &joinPartition{
			trace:       pt,
			leftGroups:  make(map[string][]model.Detail),
			rightGroups: make(map[string][]model.Detail),
		}
	}
	for _, d := range req.Left {
		p := shipPartition(d.ShipNo, cfg.Partitions)
		parts[p].left = append(parts[p].left, d)
	}
	for _, d := range req.Right {
		p := shipPartition(d.ShipNo, cfg.Partitions)
		parts[p].right = append(parts[p].right, d)
	}

	var sizes []int64
	for i, p := range parts {
		for _, d := range p.left {
			k := joinKey(d)
			p.leftGroups[k] = append(p.leftGroups[k], d)
		}
		for _, d := range p.right {
			k := joinKey(d)
			p.rightGroups[k] = append(p.rightGroups[k], d)
		}
		var pairWork int64
		for k, ls := range p.leftGroups {
			pairWork += int64(len(ls)) * int64(len(p.rightGroups[k]))
		}
		p.pairWork = pairWork
		pt := traces[i]
		pt.LeftRows = int64(len(p.left))
		pt.RightRows = int64(len(p.right))
		pt.InputRows = pt.LeftRows + pt.RightRows
		pt.PairWork = pairWork
		pt.WorkUnits = maxInt64(pairWork, pt.InputRows)
		sizes = append(sizes, pt.InputRows)
	}
	tr.PartitionMs = elapsedMs(partitionStart)

	// Skew detection is O(partitions log partitions); no second data scan.
	detectStart := time.Now()
	median := medianInt64(sizes)
	ratioBoundary := median * cfg.SkewRatio
	tr.MedianSize = median
	tr.RatioBoundary = ratioBoundary
	tr.AbsoluteBoundary = cfg.SkewMinRows
	for _, pt := range traces {
		pt.MedianSize = median
		pt.RatioBoundary = ratioBoundary
		pt.AbsoluteBoundary = cfg.SkewMinRows
		ratioSkew := float64(pt.InputRows) > ratioBoundary
		absoluteSkew := pt.InputRows > cfg.SkewMinRows
		pt.Skewed = cfg.DetectSkew && ratioSkew && absoluteSkew
		pt.Decision = joinDecisionText(pt, cfg, ratioSkew, absoluteSkew)

		suggested := 1
		if pt.Skewed {
			maxSide := maxInt64(pt.LeftRows, pt.RightRows)
			suggested, pt.SplitBasis = decideSplits(maxSide, pt.WorkUnits, cfg.Workers, cfg, true)
			tr.SkewPartitions++
		} else {
			pt.SplitBasis = "未达到倾斜条件，保持单任务，避免额外拆分/合并开销"
		}
		pt.SuggestedSplits = suggested
		if pt.Skewed && cfg.AutoSplit {
			pt.SplitEnabled = true
			pt.RequestedSplits = suggested
			tr.SplitPartitions++
		} else {
			pt.SplitEnabled = false
			pt.RequestedSplits = 1
			if pt.Skewed && !cfg.AutoSplit {
				pt.SplitBasis += "；但自动拆分开关关闭，实际保持 1 个任务"
			}
		}
	}
	tr.DetectMs = elapsedMs(detectStart)
	tr.BaselineLoad = buildBaselineLoad(traces, cfg.Workers)
	tr.PartitionsTrace = traces

	execStart := time.Now()
	var jobs []taskJob
	var sinks []*chunkSink
	addJob := func(pt *PartitionTrace, scope string, splitIndex, splitCount int, rowsIn, work int64, fn func(sink *chunkSink, tt *TaskTrace)) {
		tt := &TaskTrace{
			Partition:     pt.Index,
			Scope:         scope,
			SplitIndex:    splitIndex,
			SplitCount:    splitCount,
			RowsIn:        rowsIn,
			EstimatedWork: work,
		}
		pt.Tasks = append(pt.Tasks, tt)
		sink := newSink(cfg.Collect, int(work+rowsIn)+1)
		jobs = append(jobs, taskJob{trace: tt, fn: func(tt *TaskTrace) {
			fn(sink, tt)
			tt.RowsOut = sink.rows
			tt.MatchedPairs = sink.pairs
		}})
		sinks = append(sinks, sink)
	}

	for _, p := range parts {
		pt := p.trace
		if len(p.left) == 0 && len(p.right) == 0 {
			continue
		}
		if !pt.SplitEnabled {
			work := pt.WorkUnits
			addJob(pt, "join-whole", 0, 1, pt.InputRows, work, func(s *chunkSink, tt *TaskTrace) {
				runJoinWhole(p, req.JoinType, s)
			})
			continue
		}

		pieces := pt.RequestedSplits
		// Split the larger input side into disjoint contiguous ranges. Each
		// range probes the opposite side's complete key index. Every matched
		// pair is therefore emitted exactly once by the unique primary row.
		// The disjoint unmatched-row range is bundled into the same piece so a
		// split partition reports exactly RequestedSplits executable tasks.
		primaryLeft := len(p.left) >= len(p.right)
		if primaryLeft && len(p.left) > 0 {
			ranges := splitRanges(len(p.left), pieces)
			var rightNullRanges [][2]int
			if req.JoinType == JoinFull {
				rightNullRanges = splitRanges(len(p.right), len(ranges))
			}
			for i, rg := range ranges {
				lr := rg
				rr := [2]int{}
				hasRightNull := i < len(rightNullRanges)
				if hasRightNull {
					rr = rightNullRanges[i]
				}
				pairWork := estimateRangeWork(p.left[lr[0]:lr[1]], p.rightGroups)
				rowsIn := int64(lr[1] - lr[0])
				work := pairWork
				if hasRightNull {
					n := int64(rr[1] - rr[0])
					rowsIn += n
					work += n
				}
				addJob(pt, "split-L-primary", i, len(ranges), rowsIn, work, func(s *chunkSink, tt *TaskTrace) {
					runLeftPrimaryRange(p, req.JoinType, lr[0], lr[1], s)
					if hasRightNull {
						runRightUnmatchedRange(p, rr[0], rr[1], s)
					}
				})
			}
		} else if len(p.right) > 0 {
			ranges := splitRanges(len(p.right), pieces)
			var leftNullRanges [][2]int
			if req.JoinType == JoinLeft || req.JoinType == JoinFull {
				leftNullRanges = splitRanges(len(p.left), len(ranges))
			}
			for i, rg := range ranges {
				rr := rg
				lr := [2]int{}
				hasLeftNull := i < len(leftNullRanges)
				if hasLeftNull {
					lr = leftNullRanges[i]
				}
				pairWork := estimateRangeWorkRight(p.right[rr[0]:rr[1]], p.leftGroups)
				rowsIn := int64(rr[1] - rr[0])
				work := pairWork
				if hasLeftNull {
					n := int64(lr[1] - lr[0])
					rowsIn += n
					work += n
				}
				addJob(pt, "split-R-primary", i, len(ranges), rowsIn, work, func(s *chunkSink, tt *TaskTrace) {
					runRightPrimaryRange(p, req.JoinType, rr[0], rr[1], s)
					if hasLeftNull {
						runLeftUnmatchedRange(p, lr[0], lr[1], s)
					}
				})
			}
		} else if len(p.left) > 0 {
			// Right side is empty; left-primary branch would normally handle
			// this, but keep an explicit fallback for clarity.
			ranges := splitRanges(len(p.left), pieces)
			for i, rg := range ranges {
				lr := rg
				addJob(pt, "left-null", i, len(ranges), int64(lr[1]-lr[0]), int64(lr[1]-lr[0]), func(s *chunkSink, tt *TaskTrace) {
					runLeftPrimaryRange(p, req.JoinType, lr[0], lr[1], s)
				})
			}
		}
	}

	e.runJobs(jobs, tr, totalStart)
	tr.ExecuteMs = elapsedMs(execStart)
	finishPartitionTraces(traces)

	mergeStart := time.Now()
	result := &model.Result{Kind: "join_" + req.JoinType, Columns: model.JoinColumns}
	totalRows := 0
	for _, s := range sinks {
		totalRows += int(s.rows)
		tr.Fingerprint1 += s.fp1
		tr.Fingerprint2 += s.fp2
	}
	result.RowsCount = totalRows
	if cfg.Collect {
		result.Lines = make([]string, 0, totalRows)
		for _, s := range sinks {
			result.Lines = append(result.Lines, s.lines...)
		}
	}
	tr.ResultRows = totalRows
	tr.MergeMs = elapsedMs(mergeStart)
	tr.TotalElapsedMs = elapsedMs(totalStart)
	return result, tr
}

func joinDecisionText(pt *PartitionTrace, cfg Config, ratioSkew, absoluteSkew bool) string {
	return "真实统计 " + itoa(pt.InputRows) + " 行（左 " + itoa(pt.LeftRows) +
		" / 右 " + itoa(pt.RightRows) + "）；全部分区中位数 " + ftoa(pt.MedianSize) +
		"；" + ftoa(cfg.SkewRatio) + " 倍边界=" + ftoa(pt.RatioBoundary) +
		"，倍数条件=" + boolCN(ratioSkew) + "；绝对行数阈值 " + itoa(cfg.SkewMinRows) +
		"，绝对值条件=" + boolCN(absoluteSkew)
}

func runJoinWhole(p *joinPartition, joinType string, sink *chunkSink) {
	for _, l := range p.left {
		rs := p.rightGroups[joinKey(l)]
		if len(rs) > 0 {
			for _, r := range rs {
				sink.emitMatched(l, r)
			}
		} else if joinType != JoinInner {
			sink.emitLeftOnly(l)
		}
	}
	if joinType == JoinFull {
		for _, r := range p.right {
			if ls := p.leftGroups[joinKey(r)]; len(ls) == 0 {
				sink.emitRightOnly(r)
			}
		}
	}
}

func runLeftPrimaryRange(p *joinPartition, joinType string, start, end int, sink *chunkSink) {
	for i := start; i < end; i++ {
		l := p.left[i]
		rs := p.rightGroups[joinKey(l)]
		if len(rs) > 0 {
			for _, r := range rs {
				sink.emitMatched(l, r)
			}
		} else if joinType != JoinInner {
			sink.emitLeftOnly(l)
		}
	}
}

func runRightPrimaryRange(p *joinPartition, joinType string, start, end int, sink *chunkSink) {
	for i := start; i < end; i++ {
		r := p.right[i]
		ls := p.leftGroups[joinKey(r)]
		if len(ls) > 0 {
			for _, l := range ls {
				sink.emitMatched(l, r)
			}
		} else if joinType == JoinFull {
			sink.emitRightOnly(r)
		}
	}
}

func runLeftUnmatchedRange(p *joinPartition, start, end int, sink *chunkSink) {
	for i := start; i < end; i++ {
		l := p.left[i]
		if len(p.rightGroups[joinKey(l)]) == 0 {
			sink.emitLeftOnly(l)
		}
	}
}

func runRightUnmatchedRange(p *joinPartition, start, end int, sink *chunkSink) {
	for i := start; i < end; i++ {
		r := p.right[i]
		if len(p.leftGroups[joinKey(r)]) == 0 {
			sink.emitRightOnly(r)
		}
	}
}

func estimateRangeWork(rows []model.Detail, opposite map[string][]model.Detail) int64 {
	var w int64
	for _, d := range rows {
		w += int64(len(opposite[joinKey(d)]))
	}
	return w
}

func estimateRangeWorkRight(rows []model.Detail, opposite map[string][]model.Detail) int64 {
	return estimateRangeWork(rows, opposite)
}
