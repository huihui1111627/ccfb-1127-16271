package data

import (
	"math"
	"math/rand"

	"skewexec/internal/model"
)

const (
	DistUniform  = "uniform"
	DistLongTail = "longtail"
	DistExtreme  = "extreme"

	ScaleSmall      = "small"
	ScaleAcceptance = "acceptance"
	ScaleStandard   = "standard"
	ScaleLarge      = "large"
)

type Config struct {
	Seed         int64  `json:"seed"`
	Distribution string `json:"distribution"`
	Scale        string `json:"scale"`
	InboundRows  int    `json:"inbound_rows"`
	OutboundRows int    `json:"outbound_rows"`
	Ships        int    `json:"ships"`
	Warehouses   int    `json:"warehouses"`
}

type Store struct {
	Config   Config             `json:"config"`
	Inbound  []model.Detail     `json:"-"`
	Outbound []model.Detail     `json:"-"`
	Flows    []model.FlowRecord `json:"-"`
}

func SeedFor(distribution string) int64 {
	switch distribution {
	case DistUniform:
		return 20261101
	case DistLongTail:
		return 20261102
	case DistExtreme:
		return 20261103
	default:
		return 20261100
	}
}

func DefaultConfig(distribution, scale string) Config {
	if distribution == "" {
		distribution = DistExtreme
	}
	if scale == "" {
		scale = ScaleAcceptance
	}
	c := Config{
		Seed:         SeedFor(distribution),
		Distribution: distribution,
		Scale:        scale,
		Ships:        28,
		Warehouses:   6,
	}
	switch scale {
	case ScaleSmall:
		c.InboundRows, c.OutboundRows = 700, 700
	case ScaleAcceptance:
		c.InboundRows, c.OutboundRows = 2400, 2400
	case ScaleStandard:
		c.InboundRows, c.OutboundRows = 6000, 6000
	case ScaleLarge:
		c.InboundRows, c.OutboundRows = 18000, 18000
	default:
		c.Scale = ScaleAcceptance
		c.InboundRows, c.OutboundRows = 2400, 2400
	}
	return c
}

var species = []string{
	"带鱼", "大黄鱼", "小黄鱼", "鲳鱼", "鲅鱼", "鱿鱼", "虾类", "蟹类",
}

func distributionWeights(distribution string, ships int) []float64 {
	w := make([]float64, ships)
	switch distribution {
	case DistUniform:
		for i := range w {
			w[i] = 1
		}
	case DistLongTail:
		for i := range w {
			w[i] = 1 / math.Pow(float64(i+1), 1.35)
		}
	case DistExtreme:
		// The largest three vessels receive about 62%, 22%, 11%.
		// The remaining vessels share only 5%, making the largest
		// vessel hundreds of times larger than a typical small boat.
		for i := range w {
			switch i {
			case 0:
				w[i] = 0.62
			case 1:
				w[i] = 0.22
			case 2:
				w[i] = 0.11
			default:
				w[i] = 0.05 / float64(ships-3)
			}
		}
	default:
		for i := range w {
			w[i] = 1
		}
	}
	return w
}

func warehouseWeights(n int) []float64 {
	w := make([]float64, n)
	for i := range w {
		w[i] = 1 / math.Pow(float64(i+1), 0.65)
	}
	return w
}

func pickWeighted(r *rand.Rand, weights []float64, total float64) int {
	v := r.Float64() * total
	for i, w := range weights {
		v -= w
		if v <= 0 {
			return i
		}
	}
	return len(weights) - 1
}

func shipNo(i int) string {
	return "浙渔" + string(rune('A'+i/26)) + formatInt(i%26+1, 3)
}

func formatInt(v, width int) string {
	s := ""
	for v > 0 {
		s = string(rune('0'+v%10)) + s
		v /= 10
	}
	for len(s) < width {
		s = "0" + s
	}
	return s
}

func warehouseNo(i int) string {
	return "WH-" + formatInt(i+1, 2)
}

func Generate(cfg Config) *Store {
	r := rand.New(rand.NewSource(cfg.Seed))
	shipW := distributionWeights(cfg.Distribution, cfg.Ships)
	var shipTotal float64
	for _, w := range shipW {
		shipTotal += w
	}
	whW := warehouseWeights(cfg.Warehouses)
	var whTotal float64
	for _, w := range whW {
		whTotal += w
	}

	makeRows := func(n int, side string) []model.Detail {
		rows := make([]model.Detail, 0, n+64)
		for id := 1; id <= n; id++ {
			var ship, wh string

			// Reserve 5% rows for deterministic outer-join unmatched cases.
			// Some use rare ships and some use a normal/hot ship with a
			// warehouse number absent from the opposite side.
			if id > n*95/100 {
				k := id - n*95/100
				if k%2 == 0 {
					if side == model.DirectionIn {
						ship = "INONLY-" + formatInt(k%8+1, 2)
						wh = warehouseNo(r.Intn(cfg.Warehouses))
					} else {
						ship = "OUTONLY-" + formatInt(k%8+1, 2)
						wh = warehouseNo(r.Intn(cfg.Warehouses))
					}
				} else {
					ship = shipNo(k % 3) // includes the hottest vessel
					if side == model.DirectionIn {
						wh = "WH-I9"
					} else {
						wh = "WH-O9"
					}
				}
			} else {
				si := pickWeighted(r, shipW, shipTotal)
				wi := pickWeighted(r, whW, whTotal)
				ship = shipNo(si)
				wh = warehouseNo(wi)
			}

			boxes := int64(2 + r.Intn(78))
			unit := int64(8 + r.Intn(26))
			rows = append(rows, model.Detail{
				ID:      id,
				ShipNo:  ship,
				WhNo:    wh,
				Species: species[r.Intn(len(species))],
				Weight:  boxes * unit,
				Boxes:   boxes,
				Hour:    5 + r.Intn(18),
			})
		}
		return rows
	}

	in := makeRows(cfg.InboundRows, model.DirectionIn)
	out := makeRows(cfg.OutboundRows, model.DirectionOut)

	flows := make([]model.FlowRecord, 0, len(in)+len(out))
	uid := 1
	for _, d := range in {
		flows = append(flows, model.FlowRecord{Detail: d, Direction: model.DirectionIn, UID: uid})
		uid++
	}
	for _, d := range out {
		flows = append(flows, model.FlowRecord{Detail: d, Direction: model.DirectionOut, UID: uid})
		uid++
	}

	return &Store{Config: cfg, Inbound: in, Outbound: out, Flows: flows}
}
