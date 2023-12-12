package utils

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"strings"
	"time"

	"github.com/sethvargo/go-retry"
	"github.com/tidwall/resp"
)

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

func Filter[T any](arr []T, test func(elem T) bool) (res []T) {
	for _, e := range arr {
		if test(e) {
			res = append(res, e)
		}
	}
	return
}

func Decode(raw string) ([]string, error) {
	rd := resp.NewReader(bytes.NewBufferString(raw))
	res := []string{}

	v, _, err := rd.ReadValue()

	if err != nil {
		return nil, err
	}

	if Contains[string]([]string{"SimpleString", "Integer", "Error"}, v.Type().String()) {
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

func RetryBackoff(b retry.Backoff, maxRetries uint64, jitter, cappedDuration, maxDuration time.Duration) retry.Backoff {
	backoff := b

	if maxRetries > 0 {
		backoff = retry.WithMaxRetries(maxRetries, backoff)
	}

	if jitter > 0 {
		backoff = retry.WithJitter(jitter, backoff)
	}

	if cappedDuration > 0 {
		backoff = retry.WithCappedDuration(cappedDuration, backoff)
	}

	if maxDuration > 0 {
		backoff = retry.WithMaxDuration(maxDuration, backoff)
	}

	return backoff
}

func UnmarshalCommandsJSON(b []byte) ([]Command, error) {
	commands := []Command{}
	if err := json.Unmarshal(b, &commands); err != nil {
		return nil, err
	}
	return commands, nil
}

func GetIPAddress() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")

	if err != nil {
		return "", err
	}

	defer conn.Close()

	localAddr := strings.Split(conn.LocalAddr().String(), ":")[0]

	return localAddr, nil
}
