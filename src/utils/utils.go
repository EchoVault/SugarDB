package utils

import (
	"bytes"
	"io"
	"math/big"
	"net"
	"slices"
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
		return int(i)
	}

	f, _ := n.Float64()

	return f
}

func Filter[T any](arr []T, test func(elem T) bool) (res []T) {
	for _, e := range arr {
		if test(e) {
			res = append(res, e)
		}
	}
	return
}

func Decode(raw []byte) ([]string, error) {
	rd := resp.NewReader(bytes.NewBuffer(raw))
	var res []string

	v, _, err := rd.ReadValue()

	if err != nil {
		return nil, err
	}

	if slices.Contains([]string{"SimpleString", "Integer", "Error"}, v.Type().String()) {
		return []string{v.String()}, nil
	}

	if v.Type().String() == "Array" {
		for _, elem := range v.Array() {
			res = append(res, elem.String())
		}
	}

	return res, nil
}

func ReadMessage(r io.Reader) ([]byte, error) {
	delim := []byte{'\r', '\n', '\r', '\n'}
	buffSize := 8
	buff := make([]byte, buffSize)

	var n int
	var err error
	var res []byte

	for {
		n, err = r.Read(buff)
		res = append(res, buff...)
		if n < buffSize || err != nil {
			break
		}
		if bytes.Equal(buff[len(buff)-4:], delim) {
			break
		}
		clear(buff)
	}

	return res, err
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

func GetIPAddress() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")

	if err != nil {
		return "", err
	}

	defer conn.Close()

	localAddr := strings.Split(conn.LocalAddr().String(), ":")[0]

	return localAddr, nil
}

func GetSubCommand(command Command, cmd []string) interface{} {
	if len(command.SubCommands) == 0 || len(cmd) < 2 {
		return nil
	}
	for _, subCommand := range command.SubCommands {
		if strings.EqualFold(subCommand.Command, cmd[1]) {
			return subCommand
		}
	}
	return nil
}

func AbsInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
