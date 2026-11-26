package acceptance

import (
	"testing"

	"skewexec/internal/data"
	"skewexec/internal/engine"
)

func TestAllQueriesAllDistributionsSmall(t *testing.T) {
	cfg := engine.DefaultConfig()
	cfg.Workers = 8
	cfg.Partitions = 16
	report := RunAll(Options{Scale: data.ScaleSmall, Workers: 8, Repeats: 1, Base: cfg})
	for _, c := range report.Cases {
		if !c.Pass {
			t.Fatalf("%s/%s failed: %s\n%s", c.Distribution, c.QueryID, c.Error, c.Correctness.Difference)
		}
	}
	if !report.AllPass || report.PassedCount != report.TotalCount {
		t.Fatalf("acceptance report not all pass: %+v", report)
	}
}

func TestAllQueriesForcedSplitsAndOneWorker(t *testing.T) {
	for _, workers := range []int{1, 2, 4} {
		cfg := engine.DefaultConfig()
		cfg.Workers = workers
		cfg.Partitions = 8
		cfg.SkewRatio = 0.0001
		cfg.SkewMinRows = 1
		cfg.MinRowsPerPiece = 1
		cfg.TargetWorkPerPiece = 1
		cfg.TargetAggRowsPerPiece = 1
		report := RunAll(Options{Scale: data.ScaleSmall, Workers: workers, Repeats: 1, Base: cfg})
		for _, c := range report.Cases {
			if !c.Pass {
				t.Fatalf("workers=%d %s/%s failed: %s\n%s", workers, c.Distribution, c.QueryID, c.Error, c.Correctness.Difference)
			}
			if c.SplitPartitions == 0 {
				t.Fatalf("workers=%d %s/%s expected forced split partitions", workers, c.Distribution, c.QueryID)
			}
		}
	}
}
