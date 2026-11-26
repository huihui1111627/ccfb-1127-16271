# 渔港冷库倾斜分区查询执行器

一个无外部依赖的 Go 单机多线程查询执行器示例：程序内用固定种子生成渔船卸货/出入库明细，按 `船号` Hash 分区，支持 `(船号, 库号)` 关联和分组聚合；运行时统计分区真实行数，对超过“中位数倍数 + 绝对行数”双阈值的倾斜分区自动拆分并行处理，并提供网页观察分区、决策、线程负载和任务时间线。

## 快速开始

```bash
# 启动网页（默认 :8080）
go run .

# 自定义端口/线程数可在网页中调整，也可以：
go build -o skewexec . && ./skewexec -addr=:8090
```

打开 <http://localhost:8080>。

网页可以：

- 选择 `uniform` / `longtail` / `extreme` 固定种子数据；
- 选择内连接、左外连接、全外连接和多种聚合/TopN；
- 调整线程数、分区数、中位数倍数、绝对行数阈值；
- 对同一条查询分别计时自动拆分开/关；
- 展示每个分区左右行数、总行数、估算工作单元、处理墙钟/CPU 时间；
- 展示倾斜判定条件、建议片数、最终片数和完整决策依据；
- 展示关闭时实际线程负载、开启后实际线程负载、拆分前静态轮询估算；
- 展示每个 worker 的任务时间线和子任务输入/配对/输出计数；
- 在 small/acceptance 规模自动与朴素实现做完整排序逐行核对。

## 验收

```bash
# 跑 3 种分布 × 8 种查询，开/关各计时 5 轮，并生成报告
go run ./cmd/acceptance -repeats=5

# 快速测试
go test ./...

# 并发安全检查
go test -race ./...
```

生成：

- `ACCEPTANCE_REPORT.md`：人读表格；
- `acceptance_report.json`：完整机器可读结果和每轮耗时。

当前验收矩阵为：

- 分布：`uniform`、`longtail`、`extreme`，种子固定在 `internal/data/generate.go`；
- 查询：inner join、left outer join、full outer join、船号汇总、船号+库号汇总、每船 Top3 鱼种、每船 Top3 明细、每船每库 Top3 明细；
- 正确性：优化执行器拆分开启、拆分关闭两份结果，分别与单线程船号+库号嵌套循环朴素实现比较；所有 canonical row 排序后逐行比较，保留重复行计数；
- 计时：预热一次后，每侧配置运行多轮，报告最小值、中位数、均值、最大值和全部轮次。

最新一次本机 14 逻辑核、Go 1.26.1 的结果是 **24/24 全部一致**。极端倾斜关联的中位耗时约从 `104ms` 降到 `28ms`，约 `3.6–3.8×`；均匀数据不判定倾斜，检测耗时通常为几微秒，开/关耗时差异在噪声范围内。简单聚合本身是亚毫秒级，拆分收益可能小于调度和局部 map 合并开销，报告如实记录；TopN 等较重聚合在极端倾斜下可见收益。

## 倾斜判定与拆分片数

默认条件（两个条件同时满足）：

```text
partition_rows > skew_ratio × median_partition_rows
partition_rows > skew_min_rows
```

默认 `skew_ratio=3.0`、`skew_min_rows=500`，网页可配。

片数不是拍脑袋固定值。对倾斜分区，执行器计算：

```text
by_work   = ceil(estimated_work_units / target_work_per_piece)
by_rows   = max_side_rows / min_rows_per_piece
thread_cap = workers × task_oversubscribe
pieces     = max(2, min(by_work, by_rows, thread_cap))
```

关联的 `estimated_work_units` 是按 join key 分组后精确估算的配对数 `sum(len(left_group[k]) * len(right_group[k]))`，并与输入行数取最大值；聚合使用输入行数和较小的聚合目标片大小。网页每个倾斜分区都会显示这些输入数字和最终依据。

## 为什么拆分后不重、不漏，外连接 NULL 行也只出现一次

分区键是 `船号`，它是复合关联键 `(船号, 库号)` 的前缀，所以相同复合键一定在同一分区。

对于一个倾斜关联分区：

1. 按行数选择较大的一侧作为 primary side；
2. 将 primary side 切成互不相交的连续区间；
3. 每个分片用自己区间内的每一行探测对侧完整 key index，并输出所有匹配对。

因此每个匹配对由唯一的 primary row 输出一次：不会重复；所有 primary row 区间的并集是整侧，所以不会漏。

外连接未匹配行也按互不相交区间处理：

- primary 为左侧时，左未匹配行在 primary 分片中输出；FULL JOIN 的右未匹配行由对应的、互不相交的右侧区间检查并输出；
- primary 为右侧时对称；
- 每个未匹配行只属于一个区间，且仅当对侧 key group 为空时输出一次。

当两侧同一个热点 key 都很大时，上述方法仍然成立：primary side 被切开，每个分片处理一部分 primary rows，并探测同一个完整对侧 group；所有 `L_hot × R_hot` 配对被这些不相交 primary 区间完整覆盖。测试中包含 240×240 同键热点和外连接合成用例。

## 聚合热点键拆分

聚合分区同样按行切分。每个分片先做局部聚合：

- count/sum/min/max/avg：局部 accumulator 最后合并，min/max 可交换，sum/count 可加；
- 鱼种 TopN：每个分片维护局部“船 → 鱼种聚合”，合并后再排序取 N；
- 原始明细 TopN：每个分组在每个分片只保留局部 TopN，合并所有局部候选后再取全局 TopN。

局部 TopN 截断是安全的：如果一行不在某个分片的前 N 名内，该分片内已有 N 个排序键不弱于它且 tie-break 更稳定的行，因此它不可能进入全局前 N。所有排序都用 `weight DESC, UID ASC` 作为确定性 tie-break。

## 均匀数据的检测开销

分区行数在 Hash shuffle 时已经产生；倾斜检测只对 `P` 个计数排序取中位数并生成判定字符串，复杂度为 `O(P log P)`，不会第二次扫描数据。`Trace.detect_ms` 和报告中都记录了这一阶段的实际耗时（通常为微秒级）。均匀分区不会创建额外分片，执行路径与关闭自动拆分相同。

## HTTP API

```bash
# 查询元信息
curl 'http://localhost:8080/api/queries'

# 单次执行并返回完整 trace（不返回大结果行）
curl 'http://localhost:8080/api/run?dist=extreme&scale=acceptance&query=join_inner&workers=8&ratio=3&min_rows=500&split=1'

# 开/关对比；small/acceptance 默认附带朴素逐行正确性检查
curl 'http://localhost:8080/api/compare?dist=extreme&scale=acceptance&query=join_full&workers=8&repeats=3'

# 网页内快速跑验收矩阵
curl 'http://localhost:8080/api/acceptance/run?scale=small&repeats=1'
```

## 代码结构

```text
main.go                         网页服务入口，embed web/index.html
cmd/acceptance                  验收/计时报告 CLI
internal/data                   固定种子和 uniform/longtail/extreme 数据生成
internal/model                  行模型、canonical 输出格式
internal/engine                 分区、倾斜判定、线程池、关联/聚合拆分和 trace
internal/reference              单线程嵌套循环 join 与朴素聚合 oracle
internal/acceptance             矩阵、正确性检查、计时和 Markdown/JSON 报告
internal/web                    HTTP API
web/index.html                  无 CDN/无外部依赖的可视化页面
```

## 说明

这是一个用于演示执行器机制和可验证正确性的单机程序，不是完整 SQL 数据库。为了让验收可执行，默认数据规模刻意控制在朴素嵌套循环和逐行排序可以快速跑完的范围内；网页提供 standard/large 规模，此时主要比较开/关行数和双指纹，完整朴素核对仍以固定验收矩阵为准。
