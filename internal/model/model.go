package model

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Detail is one inbound or outbound detail row.
type Detail struct {
	ID      int    `json:"id"`
	ShipNo  string `json:"ship_no"`
	WhNo    string `json:"wh_no"`
	Species string `json:"species"`
	Weight  int64  `json:"weight_kg"`
	Boxes   int64  `json:"boxes"`
	Hour    int    `json:"hour"`
}

// FlowRecord is a row in the union of inbound and outbound details.
type FlowRecord struct {
	Detail
	Direction string `json:"direction"`
	UID       int    `json:"uid"`
}

const (
	KindMatched   = "M"
	KindLeftOnly  = "L"
	KindRightOnly = "R"
	DirectionIn   = "IN"
	DirectionOut  = "OUT"
)

// JoinedRow uses HasLeft/HasRight rather than nil pointers so hot join loops
// can keep values compact.
type JoinedRow struct {
	Kind     string
	Left     Detail
	Right    Detail
	HasLeft  bool
	HasRight bool
}

var JoinColumns = []string{
	"row_kind", "ship_no", "wh_no",
	"in_id", "out_id",
	"in_species", "out_species",
	"in_weight_kg", "out_weight_kg",
	"in_boxes", "out_boxes",
	"in_hour", "out_hour",
}

const fieldSep = "\x1f"

func nullString(valid bool, value string) string {
	if valid {
		return value
	}
	return "NULL"
}

func nullInt(valid bool, value int64) string {
	if valid {
		return strconv.FormatInt(value, 10)
	}
	return "NULL"
}

// CanonicalLine is the stable representation used by exact acceptance checks.
// All implementations must use this same formatter.
func (r JoinedRow) CanonicalLine() string {
	ship := ""
	wh := ""
	if r.HasLeft {
		ship, wh = r.Left.ShipNo, r.Left.WhNo
	} else if r.HasRight {
		ship, wh = r.Right.ShipNo, r.Right.WhNo
	}

	var b strings.Builder
	fields := []string{
		r.Kind,
		ship,
		wh,
		nullInt(r.HasLeft, int64(r.Left.ID)),
		nullInt(r.HasRight, int64(r.Right.ID)),
		nullString(r.HasLeft, r.Left.Species),
		nullString(r.HasRight, r.Right.Species),
		nullInt(r.HasLeft, r.Left.Weight),
		nullInt(r.HasRight, r.Right.Weight),
		nullInt(r.HasLeft, r.Left.Boxes),
		nullInt(r.HasRight, r.Right.Boxes),
		nullInt(r.HasLeft, int64(r.Left.Hour)),
		nullInt(r.HasRight, int64(r.Right.Hour)),
	}
	b.WriteString(strings.Join(fields, fieldSep))
	return b.String()
}

type Result struct {
	Kind      string   `json:"kind"`
	Columns   []string `json:"columns"`
	Lines     []string `json:"-"`
	RowsCount int      `json:"rows_count"`
}

func (r *Result) RowCount() int {
	if r == nil {
		return 0
	}
	if r.RowsCount > 0 || len(r.Lines) == 0 {
		return r.RowsCount
	}
	return len(r.Lines)
}

// SortedCanonical returns a sorted copy. Sorting lines preserves duplicate
// multiplicity, which is essential for join correctness checks.
func SortedCanonical(lines []string) []string {
	out := append([]string(nil), lines...)
	sort.Strings(out)
	return out
}

func EqualSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// FirstDifference is useful in failure reports.
func FirstDifference(a, b []string) string {
	sa := SortedCanonical(a)
	sb := SortedCanonical(b)
	if len(sa) != len(sb) {
		return fmt.Sprintf("row count differs: naive=%d optimized=%d", len(sa), len(sb))
	}
	for i := range sa {
		if sa[i] != sb[i] {
			return fmt.Sprintf("row %d differs:\nnaive: %q\nopt:   %q", i, sa[i], sb[i])
		}
	}
	return ""
}
