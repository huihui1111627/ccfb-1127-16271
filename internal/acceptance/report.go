package acceptance

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func WriteJSON(path string, r Report) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func WriteMarkdown(path string, r Report) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# 自动拆分倾斜分区验收报告\n\n")
	fmt.Fprintf(&b, "- 生成时间：%s\n", r.GeneratedAt.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(&b, "- 环境：%s/%s，%s，CPU 逻辑核 %d；执行器线程 %d，Hash 分区 %d\n", r.OS, r.Arch, r.GoVersion, r.CPUCount, r.Workers, r.Partitions)
	fmt.Fprintf(&b, "- 倾斜判定：分区行数 > %.1f × 中位数，并且 > %d 行（两个条件同时满足）\n", r.SkewRatio, r.SkewMinRows)
	fmt.Fprintf(&b, "- 数据规模：%s；计时轮数：%d（表中为中位数，括号内为最小值）\n", r.Scale, r.Repeats)
	fmt.Fprintf(&b, "- 正确性：%d/%d 通过；验收方式为与单线程船号+库号嵌套循环朴素实现排序后逐行比较，保留重复行计数\n\n", r.PassedCount, r.TotalCount)

	b.WriteString("| 数据 | 查询 | 结果行数 | 正确性 | 倾斜/拆分分区 | 任务数(开) | 检测耗时 ms | 关拆分中位 ms (最小) | 开拆分中位 ms (最小) | 中位加速 |\n")
	b.WriteString("|---|---|---:|:---:|---:|---:|---:|---:|---:|---:|\n")
	for _, c := range r.Cases {
		pass := "✅"
		if !c.Pass {
			pass = "❌"
		}
		fmt.Fprintf(&b, "| %s | %s | %d | %s | %d/%d | %d | %.4f | %.2f (%.2f) | %.2f (%.2f) | %.2fx |\n",
			c.Distribution,
			c.QueryName,
			c.Correctness.NaiveRows,
			pass,
			c.SkewPartitions,
			c.SplitPartitions,
			c.TotalTasksOn,
			c.DetectMsOn,
			c.Off.MedianMS, c.Off.MinMS,
			c.On.MedianMS, c.On.MinMS,
			c.SpeedupMedian,
		)
	}

	b.WriteString("\n## 失败详情\n\n")
	failed := 0
	for _, c := range r.Cases {
		if c.Pass {
			continue
		}
		failed++
		fmt.Fprintf(&b, "### ❌ %s / %s\n\n", c.Distribution, c.QueryID)
		if c.Error != "" {
			fmt.Fprintf(&b, "执行错误：`%s`\n\n", c.Error)
		}
		if c.Correctness.Difference != "" {
			fmt.Fprintf(&b, "```text\n%s\n```\n\n", c.Correctness.Difference)
		}
	}
	if failed == 0 {
		b.WriteString("无。所有内连接、左外连接、全外连接和聚合/TopN 结果均与朴素实现完全一致。\n")
	}

	return os.WriteFile(path, []byte(b.String()), 0o644)
}
