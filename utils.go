package main

import (
	"math"
	"os"
)

func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num * output)) / output
}

func byteToInt(b []byte) (c uint, ok bool) {
	l := len(b)
	if l == 0 {
		return
	}
	var v uint
	for i := 0; i < l; i++ {
		if b[i] < 48 || b[i] > 57 {
			return
		}
		v = uint(b[i] - '0')
		c *= 10
		c += v
	}
	ok = true
	return
}