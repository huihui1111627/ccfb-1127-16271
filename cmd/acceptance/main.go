package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"skewexec/internal/acceptance"
	"skewexec/internal/data"
	"skewexec/internal/engine"
)

func main() {
	scale := flag.String("scale", data.ScaleAcceptance, "data scale: small|acceptance|standard|large")
	workers := flag.Int("workers", runtime.NumCPU(), "worker threads")
	repeats := flag.Int("repeats", 5, "timing repetitions after warmup")
	ratio := flag.Float64("skew-ratio", 3.0, "skew threshold as multiple of median partition size")
	minRows := flag.Int64("skew-min-rows", 500, "absolute skew row threshold")
	jsonPath := flag.String("json", "acceptance_report.json", "JSON report output path")
	mdPath := flag.String("markdown", "ACCEPTANCE_REPORT.md", "Markdown report output path")
	flag.Parse()

	base := engine.DefaultConfig()
	base.Workers = *workers
	base.SkewRatio = *ratio
	base.SkewMinRows = *minRows
	base.Collect = true

	report := acceptance.RunAll(acceptance.Options{
		Scale:   *scale,
		Workers: *workers,
		Repeats: *repeats,
		Base:    base,
	})

	if *jsonPath != "" {
		if err := acceptance.WriteJSON(*jsonPath, report); err != nil {
			fmt.Fprintf(os.Stderr, "write JSON report: %v\n", err)
			os.Exit(2)
		}
	}
	if *mdPath != "" {
		if err := acceptance.WriteMarkdown(*mdPath, report); err != nil {
			fmt.Fprintf(os.Stderr, "write Markdown report: %v\n", err)
			os.Exit(2)
		}
	}

	fmt.Printf("acceptance: %d/%d passed; report=%s json=%s\n",
		report.PassedCount, report.TotalCount, *mdPath, *jsonPath)
	if !report.AllPass {
		os.Exit(1)
	}
}
