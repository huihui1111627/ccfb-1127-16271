package reference

import (
	"sort"

	"skewexec/internal/engine"
	"skewexec/internal/model"
)

func NaiveAgg(kind string, topN int, rows []model.FlowRecord) *model.Result {
	if topN <= 0 {
		topN = 3
	}
	switch kind {
	case engine.AggShipSummary, engine.AggShipWhSummary:
		return naiveSummary(kind, rows)
	case engine.AggTopSpeciesShip:
		return naiveTopSpecies(topN, rows)
	case engine.AggTopRecordsShip, engine.AggTopRecordsShipWh:
		return naiveTopRecords(kind == engine.AggTopRecordsShipWh, topN, rows)
	default:
		return naiveSummary(engine.AggShipSummary, rows)
	}
}

type refSummary struct {
	ship, wh                           string
	count, inCount, outCount           int64
	weight, inWeight, outWeight, boxes int64
	minWeight, maxWeight               int64
}

func naiveSummary(kind string, rows []model.FlowRecord) *model.Result {
	byWh := kind == engine.AggShipWhSummary
	m := make(map[string]*refSummary)
	for _, f := range rows {
		key := f.ShipNo
		wh := "*"
		if byWh {
			key += "\x00" + f.WhNo
			wh = f.WhNo
		}
		a := m[key]
		if a == nil {
			a = &refSummary{ship: f.ShipNo, wh: wh, minWeight: f.Weight, maxWeight: f.Weight}
			m[key] = a
		}
		a.count++
		a.weight += f.Weight
		a.boxes += f.Boxes
		if f.Weight < a.minWeight {
			a.minWeight = f.Weight
		}
		if f.Weight > a.maxWeight {
			a.maxWeight = f.Weight
		}
		if f.Direction == model.DirectionIn {
			a.inCount++
			a.inWeight += f.Weight
		} else {
			a.outCount++
			a.outWeight += f.Weight
		}
	}
	lines := make([]string, 0, len(m))
	for _, a := range m {
		lines = append(lines, model.SummaryLine(a.ship, a.wh, a.count, a.inCount, a.outCount,
			a.weight, a.inWeight, a.outWeight, a.boxes, a.minWeight, a.maxWeight))
	}
	sort.Strings(lines)
	return &model.Result{Kind: kind, Columns: model.SummaryColumns, Lines: lines, RowsCount: len(lines)}
}

type refSpecies struct {
	count, weight, boxes int64
}

func naiveTopSpecies(topN int, rows []model.FlowRecord) *model.Result {
	m := make(map[string]map[string]*refSpecies)
	for _, f := range rows {
		sm := m[f.ShipNo]
		if sm == nil {
			sm = make(map[string]*refSpecies)
			m[f.ShipNo] = sm
		}
		a := sm[f.Species]
		if a == nil {
			a = &refSpecies{}
			sm[f.Species] = a
		}
		a.count++
		a.weight += f.Weight
		a.boxes += f.Boxes
	}
	lines := make([]string, 0)
	for ship, sm := range m {
		type item struct {
			name string
			acc  *refSpecies
		}
		items := make([]item, 0, len(sm))
		for name, a := range sm {
			items = append(items, item{name, a})
		}
		sort.Slice(items, func(i, j int) bool {
			if items[i].acc.weight != items[j].acc.weight {
				return items[i].acc.weight > items[j].acc.weight
			}
			return items[i].name < items[j].name
		})
		n := topN
		if n > len(items) {
			n = len(items)
		}
		for i := 0; i < n; i++ {
			a := items[i].acc
			lines = append(lines, model.TopSpeciesLine(ship, i+1, items[i].name, a.count, a.weight, a.boxes))
		}
	}
	sort.Strings(lines)
	return &model.Result{Kind: engine.AggTopSpeciesShip, Columns: model.TopSpeciesColumns, Lines: lines, RowsCount: len(lines)}
}

func naiveTopRecords(byWh bool, topN int, rows []model.FlowRecord) *model.Result {
	m := make(map[string][]model.FlowRecord)
	for _, f := range rows {
		key := f.ShipNo
		if byWh {
			key += "\x00" + f.WhNo
		}
		m[key] = append(m[key], f)
	}
	lines := make([]string, 0)
	for _, rs := range m {
		sort.Slice(rs, func(i, j int) bool {
			if rs[i].Weight != rs[j].Weight {
				return rs[i].Weight > rs[j].Weight
			}
			return rs[i].UID < rs[j].UID
		})
		if topN < len(rs) {
			rs = rs[:topN]
		}
		for rank, f := range rs {
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
	return &model.Result{Kind: engine.AggTopRecordsShip, Columns: model.TopRecordColumns, Lines: lines, RowsCount: len(lines)}
}
