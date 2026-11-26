package engine

import (
	"runtime"
	"strconv"
)

func runtimeNumCPU() int { return runtime.NumCPU() }

func itoa[T ~int | ~int64](v T) string {
	return strconv.FormatInt(int64(v), 10)
}

func ftoa(v float64) string {
	return strconv.FormatFloat(v, 'f', 1, 64)
}

func boolCN(v bool) string {
	if v {
		return "满足"
	}
	return "不满足"
}
