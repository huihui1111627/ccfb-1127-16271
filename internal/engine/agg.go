package engine

import (
	"sort"
	"time"

	"skewexec/internal/model"
)

const (
	AggShipSummary      = "ship_summary"
	AggShipWhSummary    = "ship_wh_summary"
	AggTopSpeciesShip   = "top_species_ship"
	AggTopRecordsShip   = "top_records_ship"
	AggTopRecordsShipWh = "top_records_ship_wh"
)

type AggRequest struct {
	QueryName    string
	Distribution string
	Scale        string
	Kind         string
	TopN         int
	Rows         []model.FlowRecord
}

type aggPartition struct {
	trace *PartitionTrace
	rows  []model.FlowRecord
}

type summaryAcc struct {
	Ship      string
	Wh        string
	Count     int64
	InCount   int64
	OutCount  int64
	Weight    int64
	InWeight  int64
	OutWeight int64
	Boxes     int64
	MinWeight int64
	MaxWeight int64
}

func (a *summaryAcc) add(f model.FlowRecord, byWh bool) {
	if a.Count == 0 {
		a.Ship = f.ShipNo
		if byWh {
			a.Wh = f.WhNo
		} else {
			a.Wh = "*"
		}
		a.MinWeight = f.Weight
		a.MaxWeight = f.Weight
	} else {
		if f.Weight < a.MinWeight {
			a.MinWeight = f.Weight
		}
		if f.Weight > a.MaxWeight {
			a.MaxWeight = f.Weight
		}
	}
	a.Count++
	a.Weight += f.Weight
	a.Boxes += f.Boxes
	switch f.Direction {
	case model.DirectionIn:
		a.InCount++
		a.InWeight += f.Weight
	case model.DirectionOut:
		a.OutCount++
		a.OutWeight += f.Weight
	}
}

func (a *summaryAcc) merge(b *summaryAcc) {
	if b == nil || b.Count == 0 {
		return
	}
	if a.Count == 0 {
		*a = *b
		return
	}
	a.Count += b.Count
	a.InCount += b.InCount
	a.OutCount += b.OutCount
	a.Weight += b.Weight
	a.InWeight += b.InWeight
	a.OutWeight += b.OutWeight
	a.Boxes += b.Boxes
	if b.MinWeight < a.MinWeight {
		a.MinWeight = b.MinWeight
	}
	if b.MaxWeight > a.MaxWeight {
		a.MaxWeight = b.MaxWeight
	}
}

func (a *summaryAcc) line() string {
	return model.SummaryLine(a.Ship, a.Wh, a.Count, a.InCount, a.OutCount,
		a.Weight, a.InWeight, a.OutWeight, a.Boxes, a.MinWeight, a.MaxWeight)
}

type speciesAcc struct {
	Count  int64
	Weight int64
	Boxes  int64
}

func (e *Engine) RunAgg(req AggRequest) (*model.Result, *Trace) {
	cfg := e.cfg
	cfg.Normalize()
	if req.TopN <= 0 {
		req.TopN = 3
	}
	totalStart := time.Now()
	tr := &Trace{
		QueryName:    req.QueryName,
		QueryKind:    "agg_" + req.Kind,
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

	partitionStart := time.Now()
	parts := make([]*aggPartition, cfg.Partitions)
	traces := make([]*PartitionTrace, cfg.Partitions)
	for i := range parts {
		pt := &PartitionTrace{Index: i, Tasks: []*TaskTrace{}}
		traces[i] = pt
		parts[i] = &aggPartition{trace: pt}
	}
	for _, f := range req.Rows {
		p := shipPartition(f.ShipNo, cfg.Partitions)
		parts[p].rows = append(parts[p].rows, f)
	}
	var sizes []int64
	for i, p := range parts {
		var inRows, outRows int64
		for _, f := range p.rows {
			if f.Direction == model.DirectionIn {
				inRows++
			} else {
				outRows++
			}
		}
		pt := traces[i]
		pt.LeftRows = inRows
		pt.RightRows = outRows
		pt.InputRows = int64(len(p.rows))
		pt.WorkUnits = pt.InputRows
		sizes = append(sizes, pt.InputRows)
	}
	tr.PartitionMs = elapsedMs(partitionStart)

	detectStart := time.Now()
	median := medianInt64(sizes)
	ratioBoundary := median * cfg.SkewRatio
	tr.MedianSize = median
	tr.RatioBoundary = ratioBoundary
	tr.AbsoluteBoundary = cfg.SkewMinRows
	aggCfg := cfg
	aggCfg.TargetWorkPerPiece = cfg.TargetAggRowsPerPiece
	for _, pt := range traces {
		pt.MedianSize = median
		pt.RatioBoundary = ratioBoundary
		pt.AbsoluteBoundary = cfg.SkewMinRows
		ratioSkew := float64(pt.InputRows) > ratioBoundary
		absoluteSkew := pt.InputRows > cfg.SkewMinRows
		pt.Skewed = cfg.DetectSkew && ratioSkew && absoluteSkew
		pt.Decision = "真实统计 " + itoa(pt.InputRows) + " 行；中位数 " + ftoa(median) +
			"；" + ftoa(cfg.SkewRatio) + " 倍边界=" + ftoa(ratioBoundary) +
			"，倍数条件=" + boolCN(ratioSkew) + "；绝对阈值 " + itoa(cfg.SkewMinRows) +
			"，绝对值条件=" + boolCN(absoluteSkew)
		suggested := 1
		if pt.Skewed {
			suggested, pt.SplitBasis = decideSplits(pt.InputRows, pt.InputRows, cfg.Workers, aggCfg, true)
			tr.SkewPartitions++
		} else {
			pt.SplitBasis = "未达到倾斜条件，使用单分区局部聚合映射，检测不产生第二次数据扫描"
		}
		pt.SuggestedSplits = suggested
		if pt.Skewed && cfg.AutoSplit {
			pt.SplitEnabled = true
			pt.RequestedSplits = suggested
			tr.SplitPartitions++
		} else {
			pt.RequestedSplits = 1
			if pt.Skewed && !cfg.AutoSplit {
				pt.SplitBasis += "；但自动拆分开关关闭，实际保持 1 个任务"
			}
		}
	}
	tr.DetectMs = elapsedMs(detectStart)
	tr.BaselineLoad = buildBaselineLoad(traces, cfg.Workers)
	tr.PartitionsTrace = traces

	var result *model.Result
	switch req.Kind {
	case AggShipSummary, AggShipWhSummary:
		byWh := req.Kind == AggShipWhSummary
		result = e.runSummaryAgg(parts, traces, tr, req, byWh, totalStart)
	case AggTopSpeciesShip:
		result = e.runTopSpeciesAgg(parts, traces, tr, req, totalStart)
	case AggTopRecordsShip, AggTopRecordsShipWh:
		byWh := req.Kind == AggTopRecordsShipWh
		result = e.runTopRecordsAgg(parts, traces, tr, req, byWh, totalStart)
	default:
		result = e.runSummaryAgg(parts, traces, tr, req, false, totalStart)
	}
	return result, tr
}

func (e *Engine) partitionRanges(p *aggPartition, pieces int) [][2]int {
	if !p.trace.SplitEnabled {
		return [][2]int{{0, len(p.rows)}}
	}
	return splitRanges(len(p.rows), pieces)
}

func compareTopRecord(a, b model.FlowRecord) bool {
	if a.Weight != b.Weight {
		return a.Weight > b.Weight
	}
	return a.UID < b.UID
}

func topNRecords(rows []model.FlowRecord, n int) []model.FlowRecord {
	sort.Slice(rows, func(i, j int) bool { return compareTopRecord(rows[i], rows[j]) })
	if n > 0 && len(rows) > n {
		rows = rows[:n]
	}
	return rows
}
