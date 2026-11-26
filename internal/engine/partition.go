package engine

import (
	"hash/fnv"

	"skewexec/internal/model"
)

func shipPartition(ship string, partitions int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(ship))
	return int(h.Sum32() % uint32(partitions))
}

func joinKey(d model.Detail) string {
	return d.ShipNo + "\x00" + d.WhNo
}

func flowGroupKey(f model.FlowRecord, byWarehouse bool) string {
	if byWarehouse {
		return f.ShipNo + "\x00" + f.WhNo
	}
	return f.ShipNo
}

// splitRanges returns [start,end) row ranges. It first honors the requested
// piece count when enough rows exist. Ranges are contiguous and disjoint,
// which makes outer-join unmatched handling easy to prove exactly once.
func splitRanges(n int, pieces int) [][2]int {
	if n <= 0 {
		return nil
	}
	if pieces <= 1 {
		return [][2]int{{0, n}}
	}
	if pieces > n {
		pieces = n
	}
	ranges := make([][2]int, 0, pieces)
	base := n / pieces
	extra := n % pieces
	start := 0
	for i := 0; i < pieces; i++ {
		size := base
		if i < extra {
			size++
		}
		end := start + size
		ranges = append(ranges, [2]int{start, end})
		start = end
	}
	return ranges
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
