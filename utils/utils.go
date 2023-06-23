package utils

import "math"

func Contains[T comparable](arr []T, elem T) bool {
	for _, v := range arr {
		if v == elem {
			return true
		}
	}
	return false
}

func IsInteger(n float64) bool {
	return math.Mod(n, 1.0) == 0
}
