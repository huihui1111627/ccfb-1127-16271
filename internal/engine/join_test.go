package engine_test

import (
	"fmt"
	"testing"

	"skewexec/internal/data"
	"skewexec/internal/engine"
	"skewexec/internal/model"
	"skewexec/internal/reference"
)

func hotRow(id int, ship, wh string, weight int64) model.Detail {
	return model.Detail{ID: id, ShipNo: ship, WhNo: wh, Species: "X", Weight: weight, Boxes: 1, Hour: 6}
}

func TestSyntheticBothSidesSameHotKeyOuterJoins(t *testing.T) {
	for _, joinType := range []string{engine.JoinInner, engine.JoinLeft, engine.JoinFull} {
		left := []model.Detail{
			hotRow(1, "HOT", "W1", 10),
			hotRow(2, "HOT", "W1", 20),
			hotRow(3, "HOT", "W2", 30),
			hotRow(4, "LONLY", "W1", 40),
		}
		right := []model.Detail{
			hotRow(101, "HOT", "W1", 11),
			hotRow(102, "HOT", "W1", 22),
			hotRow(103, "HOT", "W1", 33),
			hotRow(104, "HOT", "W2", 44),
			hotRow(105, "RONLY", "W9", 55),
		}
		// Expand the same key on both sides enough to force splitting.
		for i := 0; i < 240; i++ {
			left = append(left, hotRow(1000+i, "BIG", "K", int64(i)))
			right = append(right, hotRow(2000+i, "BIG", "K", int64(i)))
		}

		cfg := engine.DefaultConfig()
		cfg.Workers = 4
		cfg.Partitions = 4
		cfg.SkewRatio = 1.5
		cfg.SkewMinRows = 20
		cfg.MinRowsPerPiece = 16
		cfg.TargetWorkPerPiece = 1000
		cfg.AutoSplit = true
		cfg.Collect = true

		res, tr := engine.New(cfg).RunJoin(engine.JoinRequest{JoinType: joinType, Left: left, Right: right})
		naive := reference.NaiveJoin(left, right, joinType)
		a := model.SortedCanonical(res.Lines)
		b := model.SortedCanonical(naive.Lines)
		if !model.EqualSorted(a, b) {
			t.Fatalf("%s mismatch: %s", joinType, model.FirstDifference(naive.Lines, res.Lines))
		}
		if tr.SplitPartitions == 0 {
			t.Fatalf("%s expected hot partition to split; trace=%+v", joinType, tr)
		}
		for _, p := range tr.PartitionsTrace {
			if p.Skewed {
				t.Logf("%s p%d rows=%d work=%d suggested=%d actualTasks=%d basis=%s", joinType, p.Index, p.InputRows, p.WorkUnits, p.SuggestedSplits, p.ActualTasks, p.SplitBasis)
			}
		}
	}
}

func TestFixedAcceptanceScaleExtremeCorrectness(t *testing.T) {
	store := data.Generate(data.DefaultConfig(data.DistExtreme, data.ScaleAcceptance))
	cfg := engine.DefaultConfig()
	cfg.Workers = 8
	for _, qid := range []string{"join_inner", "join_left", "join_full"} {
		q := queryByID(qid)
		onCfg := cfg
		onCfg.AutoSplit = true
		on, _ := runOpt(store, q, onCfg)
		offCfg := cfg
		offCfg.AutoSplit = false
		off, _ := runOpt(store, q, offCfg)
		naive := runNaive(store, qid)
		if !model.EqualSorted(model.SortedCanonical(on.Lines), model.SortedCanonical(naive.Lines)) {
			t.Fatalf("%s on mismatch: %s", qid, model.FirstDifference(naive.Lines, on.Lines))
		}
		if !model.EqualSorted(model.SortedCanonical(off.Lines), model.SortedCanonical(naive.Lines)) {
			t.Fatalf("%s off mismatch", qid)
		}
	}
}

func queryByID(id string) (spec struct {
	ID, Kind string
	TopN     int
}) {
	// helper kept local to avoid import cycle in this smoke test declaration
	spec.ID = id
	switch id {
	case "join_inner":
		spec.Kind = engine.JoinInner
	case "join_left":
		spec.Kind = engine.JoinLeft
	case "join_full":
		spec.Kind = engine.JoinFull
	}
	return
}

func runOpt(store *data.Store, q struct {
	ID, Kind string
	TopN     int
}, cfg engine.Config) (*model.Result, *engine.Trace) {
	return engine.New(cfg).RunJoin(engine.JoinRequest{JoinType: q.Kind, Left: store.Inbound, Right: store.Outbound})
}

func runNaive(store *data.Store, id string) *model.Result {
	switch id {
	case "join_inner":
		return reference.NaiveJoin(store.Inbound, store.Outbound, engine.JoinInner)
	case "join_left":
		return reference.NaiveJoin(store.Inbound, store.Outbound, engine.JoinLeft)
	default:
		return reference.NaiveJoin(store.Inbound, store.Outbound, engine.JoinFull)
	}
}

func Example_decide() {
	fmt.Println("tests ready")
	// Output: tests ready
}
