package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"

	"github.com/tidwall/resp"
)

func Contains[T comparable](arr []T, elem T) bool {
	for _, v := range arr {
		if v == elem {
			return true
		}
	}
	return false
}

func ContainsMutual[T comparable](arr1 []T, arr2 []T) (bool, T) {
	for _, a := range arr1 {
		for _, b := range arr2 {
			if a == b {
				return true, a
			}
		}
	}
	return false, arr1[0]
}

func IsInteger(n float64) bool {
	return math.Mod(n, 1.0) == 0
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

func IncrBy(num interface{}, by interface{}) (interface{}, error) {
	if !Contains[string]([]string{"int", "float64"}, reflect.TypeOf(num).String()) {
		return nil, errors.New("can only increment number")
	}
	if !Contains[string]([]string{"int", "float64"}, reflect.TypeOf(by).String()) {
		return nil, errors.New("can only increment by number")
	}

	n, _ := num.(float64)
	b, _ := by.(float64)
	res := n + b

	if IsInteger(res) {
		return int(res), nil
	}

	return res, nil
}

func Filter[T comparable](arr []T, test func(elem T) bool) (res []T) {
	for _, e := range arr {
		if test(e) {
			res = append(res, e)
		}
	}
	return
}

func tokenize(comm string) ([]string, error) {
	r := csv.NewReader(strings.NewReader(comm))
	r.Comma = ' '
	return r.Read()
}

func Encode(comm string) (string, error) {
	tokens, err := tokenize(comm)

	if err != nil {
		return "", errors.New("could not parse command")
	}

	str := fmt.Sprintf("*%d\r\n", len(tokens))

	for i, token := range tokens {
		if i == 0 {
			str += fmt.Sprintf("$%d\r\n%s\r\n", len(token), strings.ToUpper(token))
		} else {
			str += fmt.Sprintf("$%d\r\n%s\r\n", len(token), token)
		}
	}

	str += "\n"

	return str, nil
}

func Decode(raw string) ([]string, error) {
	rd := resp.NewReader(bytes.NewBufferString(raw))
	res := []string{}

	v, _, err := rd.ReadValue()

	if err != nil {
		return nil, err
	}

	if Contains[string]([]string{"SimpleString", "BulkString", "Integer", "Error"}, v.Type().String()) {
		return []string{v.String()}, nil
	}

	if v.Type().String() == "Array" {
		for _, elem := range v.Array() {
			res = append(res, elem.String())
		}
	}

	return res, nil
}

func ReadMessage(r *bufio.ReadWriter) (message string, err error) {
	var line [][]byte

	for {
		b, _, err := r.ReadLine()

		if err != nil {
			return "", err
		}

		if bytes.Equal(b, []byte("")) {
			// End of message
			break
		}

		line = append(line, b)
	}

	return fmt.Sprintf("%s\r\n", string(bytes.Join(line, []byte("\r\n")))), nil
}
