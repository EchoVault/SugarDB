package main

import (
	"math/big"
)

func Contains[T comparable](arr []T, elem T) bool {
	for _, v := range arr {
		if v == elem {
			return true
		}
	}
	return false
}

func Filter[T comparable](arr []T, test func(elem T) bool) (res []T) {
	for _, e := range arr {
		if test(e) {
			res = append(res, e)
		}
	}
	return
}

func AdaptType(s string) interface{} {
	// Adapt the type of the parameter to string, float64 or int
	n, _, err := big.ParseFloat(s, 10, 256, big.RoundingMode(big.Exact))

	if err != nil {
		return s
	}

	if n.IsInt() {
		i, _ := n.Int64()
		return i
	}

	return n
}
