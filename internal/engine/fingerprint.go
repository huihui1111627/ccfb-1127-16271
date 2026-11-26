package engine

import "skewexec/internal/model"

// hashLine1 is FNV-1a 64 inlined to avoid allocating a hasher per result row.
func hashLine1(s string) uint64 {
	const offset = 14695981039346656037
	const prime = 1099511628211
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}

// hashLine2 is FNV-1 64, providing a second checksum for order-independent web
// comparisons. Exact acceptance checks still sort and compare every row.
func hashLine2(s string) uint64 {
	const offset = 14695981039346656037
	const prime = 1099511628211
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h *= prime
		h ^= uint64(s[i])
	}
	return h
}

type chunkSink struct {
	collect bool
	lines   []string
	rows    int64
	pairs   int64
	fp1     uint64
	fp2     uint64
}

func newSink(collect bool, estimatedRows int) *chunkSink {
	s := &chunkSink{collect: collect}
	if collect && estimatedRows > 0 {
		s.lines = make([]string, 0, estimatedRows)
	}
	return s
}

func (s *chunkSink) emit(jr model.JoinedRow) {
	line := jr.CanonicalLine()
	s.rows++
	if jr.Kind == model.KindMatched {
		s.pairs++
	}
	s.fp1 += hashLine1(line)
	s.fp2 += hashLine2(line)
	if s.collect {
		s.lines = append(s.lines, line)
	}
}

func (s *chunkSink) emitMatched(l, r model.Detail) {
	s.emit(model.JoinedRow{
		Kind:     model.KindMatched,
		Left:     l,
		Right:    r,
		HasLeft:  true,
		HasRight: true,
	})
}

func (s *chunkSink) emitLeftOnly(l model.Detail) {
	s.emit(model.JoinedRow{Kind: model.KindLeftOnly, Left: l, HasLeft: true})
}

func (s *chunkSink) emitRightOnly(r model.Detail) {
	s.emit(model.JoinedRow{Kind: model.KindRightOnly, Right: r, HasRight: true})
}
