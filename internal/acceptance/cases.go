package acceptance

import (
	"sort"

	"skewexec/internal/data"
	"skewexec/internal/engine"
	"skewexec/internal/model"
	"skewexec/internal/reference"
)

type QuerySpec struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Group string `json:"group"`
	Kind  string `json:"kind"`
	TopN  int    `json:"top_n,omitempty"`
}

func Queries() []QuerySpec {
	return []QuerySpec{
		{ID: "join_inner", Name: "内连接（船号+库号）", Group: "关联", Kind: engine.JoinInner},
		{ID: "join_left", Name: "左外连接（入库 LEFT JOIN 出库）", Group: "关联", Kind: engine.JoinLeft},
		{ID: "join_full", Name: "全外连接（FULL OUTER JOIN）", Group: "关联", Kind: engine.JoinFull},
		{ID: "agg_ship", Name: "按船号汇总", Group: "聚合", Kind: engine.AggShipSummary},
		{ID: "agg_ship_wh", Name: "按船号+库号汇总", Group: "聚合", Kind: engine.AggShipWhSummary},
		{ID: "top_species", Name: "每船重量前3鱼种", Group: "聚合/TopN", Kind: engine.AggTopSpeciesShip, TopN: 3},
		{ID: "top_records_ship", Name: "每船前3条最大明细", Group: "聚合/TopN", Kind: engine.AggTopRecordsShip, TopN: 3},
		{ID: "top_records_ship_wh", Name: "每船每库前3条最大明细", Group: "聚合/TopN", Kind: engine.AggTopRecordsShipWh, TopN: 3},
	}
}

func QueryByID(id string) QuerySpec {
	for _, q := range Queries() {
		if q.ID == id {
			return q
		}
	}
	return Queries()[0]
}

func RunNaive(store *data.Store, q QuerySpec) *model.Result {
	if q.ID[:4] == "join" {
		return reference.NaiveJoin(store.Inbound, store.Outbound, q.Kind)
	}
	return reference.NaiveAgg(q.Kind, q.TopN, store.Flows)
}

func RunOptimized(store *data.Store, q QuerySpec, cfg engine.Config) (*model.Result, *engine.Trace) {
	eng := engine.New(cfg)
	if len(q.ID) >= 4 && q.ID[:4] == "join" {
		return eng.RunJoin(engine.JoinRequest{
			QueryName:    q.Name,
			Distribution: store.Config.Distribution,
			Scale:        store.Config.Scale,
			JoinType:     q.Kind,
			Left:         store.Inbound,
			Right:        store.Outbound,
		})
	}
	return eng.RunAgg(engine.AggRequest{
		QueryName:    q.Name,
		Distribution: store.Config.Distribution,
		Scale:        store.Config.Scale,
		Kind:         q.Kind,
		TopN:         q.TopN,
		Rows:         store.Flows,
	})
}

type Correctness struct {
	Pass           bool   `json:"pass"`
	NaiveRows      int    `json:"naive_rows"`
	OptimizedRows  int    `json:"optimized_rows"`
	OffRows        int    `json:"off_rows"`
	OnEqualsNaive  bool   `json:"on_equals_naive"`
	OffEqualsNaive bool   `json:"off_equals_naive"`
	OnEqualsOff    bool   `json:"on_equals_off"`
	Difference     string `json:"difference,omitempty"`
}

// Check runs the naive oracle plus optimized split-on and split-off paths and
// compares complete sorted canonical rows with multiplicity.
func Check(store *data.Store, q QuerySpec, base engine.Config) Correctness {
	naive := RunNaive(store, q)
	naiveSorted := model.SortedCanonical(naive.Lines)

	onCfg := base
	onCfg.AutoSplit = true
	onCfg.Collect = true
	onRes, _ := RunOptimized(store, q, onCfg)
	onSorted := model.SortedCanonical(onRes.Lines)

	offCfg := base
	offCfg.AutoSplit = false
	offCfg.Collect = true
	offRes, _ := RunOptimized(store, q, offCfg)
	offSorted := model.SortedCanonical(offRes.Lines)

	onOK := model.EqualSorted(naiveSorted, onSorted)
	offOK := model.EqualSorted(naiveSorted, offSorted)
	onOff := model.EqualSorted(onSorted, offSorted)
	diff := ""
	if !onOK {
		diff = model.FirstDifference(naive.Lines, onRes.Lines)
	} else if !offOK {
		diff = model.FirstDifference(naive.Lines, offRes.Lines)
	} else if !onOff {
		diff = "split-on and split-off results differ"
	}
	return Correctness{
		Pass:           onOK && offOK && onOff,
		NaiveRows:      len(naiveSorted),
		OptimizedRows:  len(onSorted),
		OffRows:        len(offSorted),
		OnEqualsNaive:  onOK,
		OffEqualsNaive: offOK,
		OnEqualsOff:    onOff,
		Difference:     diff,
	}
}

func sortedCopy(lines []string) []string {
	out := append([]string(nil), lines...)
	sort.Strings(out)
	return out
}
