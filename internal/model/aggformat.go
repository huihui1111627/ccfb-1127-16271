package model

import "strconv"

var SummaryColumns = []string{
	"ship_no", "wh_no", "row_count", "in_count", "out_count",
	"total_weight_kg", "in_weight_kg", "out_weight_kg", "total_boxes",
	"avg_weight_kg", "min_weight_kg", "max_weight_kg",
}

func SummaryLine(ship, wh string, count, inCount, outCount, weight, inWeight, outWeight, boxes, minWeight, maxWeight int64) string {
	avg := 0.0
	if count > 0 {
		avg = float64(weight) / float64(count)
	}
	return joinCanonical(
		ship,
		wh,
		strconv.FormatInt(count, 10),
		strconv.FormatInt(inCount, 10),
		strconv.FormatInt(outCount, 10),
		strconv.FormatInt(weight, 10),
		strconv.FormatInt(inWeight, 10),
		strconv.FormatInt(outWeight, 10),
		strconv.FormatInt(boxes, 10),
		strconv.FormatFloat(avg, 'f', 4, 64),
		strconv.FormatInt(minWeight, 10),
		strconv.FormatInt(maxWeight, 10),
	)
}

var TopSpeciesColumns = []string{
	"ship_no", "rank", "species", "row_count", "total_weight_kg", "total_boxes",
}

func TopSpeciesLine(ship string, rank int, species string, count, weight, boxes int64) string {
	return joinCanonical(
		ship,
		strconv.Itoa(rank),
		species,
		strconv.FormatInt(count, 10),
		strconv.FormatInt(weight, 10),
		strconv.FormatInt(boxes, 10),
	)
}

var TopRecordColumns = []string{
	"ship_no", "wh_no", "rank", "direction", "uid", "detail_id",
	"species", "weight_kg", "boxes", "hour",
}

func TopRecordLine(ship, wh string, rank int, direction string, uid, id int, species string, weight, boxes int64, hour int) string {
	return joinCanonical(
		ship,
		wh,
		strconv.Itoa(rank),
		direction,
		strconv.Itoa(uid),
		strconv.Itoa(id),
		species,
		strconv.FormatInt(weight, 10),
		strconv.FormatInt(boxes, 10),
		strconv.Itoa(hour),
	)
}

func joinCanonical(fields ...string) string {
	out := ""
	for i, f := range fields {
		if i > 0 {
			out += fieldSep
		}
		out += f
	}
	return out
}
