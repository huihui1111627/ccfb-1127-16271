package reference

import (
	"skewexec/internal/engine"
	"skewexec/internal/model"
)

// NaiveJoin is intentionally single-threaded nested-loop execution. It is the
// correctness oracle, not a performance baseline.
func NaiveJoin(left, right []model.Detail, joinType string) *model.Result {
	res := &model.Result{Kind: "naive_join_" + joinType, Columns: model.JoinColumns}
	lines := make([]string, 0)
	rightMatched := make([]bool, len(right))

	for _, l := range left {
		matched := false
		for ri, r := range right {
			if l.ShipNo == r.ShipNo && l.WhNo == r.WhNo {
				rightMatched[ri] = true
				matched = true
				lines = append(lines, model.JoinedRow{
					Kind: model.KindMatched, Left: l, Right: r, HasLeft: true, HasRight: true,
				}.CanonicalLine())
			}
		}
		if !matched && joinType != engine.JoinInner {
			lines = append(lines, model.JoinedRow{
				Kind: model.KindLeftOnly, Left: l, HasLeft: true,
			}.CanonicalLine())
		}
	}

	if joinType == engine.JoinFull {
		for ri, r := range right {
			if !rightMatched[ri] {
				lines = append(lines, model.JoinedRow{
					Kind: model.KindRightOnly, Right: r, HasRight: true,
				}.CanonicalLine())
			}
		}
	}

	res.Lines = lines
	res.RowsCount = len(lines)
	return res
}
