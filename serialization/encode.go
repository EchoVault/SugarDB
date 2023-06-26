package serialization

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/kelvinmwinuka/memstore/utils"
	"github.com/tidwall/resp"
)

const (
	wrong_args_error = "wrong number of arguments for %s command"
	wrong_type_error = "wrong data type for %s command"
)

func tokenize(comm string) ([]string, error) {
	r := csv.NewReader(strings.NewReader(comm))
	r.Comma = ' '
	return r.Read()
}

func encodeError(wr *resp.Writer, tokens []string) {
	wr.WriteError(errors.New(tokens[1]))
}

func encodeSimpleString(wr *resp.Writer, tokens []string) error {
	fmt.Println(tokens, len(tokens))
	switch len(tokens) {
	default:
		return fmt.Errorf(wrong_args_error, strings.ToUpper(tokens[0]))
	case 2:
		fmt.Println(tokens[0], tokens[1])
		wr.WriteSimpleString(tokens[1])
		return nil
	}
}

func encodePingPong(wr *resp.Writer, tokens []string) error {
	switch len(tokens) {
	default:
		return fmt.Errorf(wrong_args_error, strings.ToUpper(tokens[0]))
	case 1:
		wr.WriteSimpleString(strings.ToUpper(tokens[0]))
		return nil
	case 2:
		wr.WriteArray([]resp.Value{
			resp.StringValue(strings.ToUpper(tokens[0])),
			resp.StringValue(tokens[1]),
		})
		return nil
	}
}

func encodeSet(wr *resp.Writer, tokens []string) error {
	switch len(tokens) {
	default:
		return fmt.Errorf(wrong_args_error, strings.ToUpper(tokens[0]))
	case 3:
		arr := []resp.Value{
			resp.StringValue(strings.ToUpper(tokens[0])),
			resp.StringValue(tokens[1]),
		}

		if n, err := strconv.ParseFloat(tokens[2], 32); err != nil {
			arr = append(arr, resp.StringValue(tokens[2]))
		} else if math.Mod(n, 1.0) == 0 {
			arr = append(arr, resp.IntegerValue(int(n)))
		} else {
			arr = append(arr, resp.FloatValue(n))
		}

		wr.WriteArray(arr)
		return nil
	}
}

func encodeGet(wr *resp.Writer, tokens []string) error {
	switch len(tokens) {
	default:
		return fmt.Errorf(wrong_args_error, strings.ToUpper(tokens[0]))
	case 2:
		wr.WriteArray([]resp.Value{
			resp.StringValue(strings.ToUpper(tokens[0])),
			resp.StringValue(tokens[1]),
		})
		return nil
	}
}

func encodeMGet(wr *resp.Writer, tokens []string) error {
	switch len(tokens) {
	default:
		arr := []resp.Value{resp.StringValue(strings.ToUpper(tokens[0]))}
		for _, token := range tokens[1:] {
			arr = append(arr, resp.StringValue(token))
		}
		wr.WriteArray(arr)
		return nil
	case 1:
		return fmt.Errorf(wrong_args_error, strings.ToUpper(tokens[0]))
	}
}

func encodeIncr(wr *resp.Writer, tokens []string) error {
	switch len(tokens) {

	default:
		return fmt.Errorf(wrong_args_error, strings.ToUpper(tokens[0]))

	case 2:
		if utils.Contains[string]([]string{"incrby", "incrbyfloat"}, strings.ToLower(tokens[0])) {
			return fmt.Errorf(wrong_args_error, strings.ToUpper(tokens[0]))
		}
		wr.WriteArray([]resp.Value{
			resp.StringValue(strings.ToUpper(tokens[0])),
			resp.StringValue(tokens[1]),
		})
		return nil

	case 3:
		if strings.ToLower(tokens[0]) == "incr" {
			return fmt.Errorf(wrong_args_error, strings.ToUpper(tokens[0]))
		}

		arr := []resp.Value{
			resp.StringValue(strings.ToUpper(tokens[0])),
			resp.StringValue(tokens[1]),
		}

		if n, err := strconv.ParseFloat(tokens[2], 32); err != nil {
			return fmt.Errorf(wrong_type_error, strings.ToUpper(tokens[0]))
		} else if !utils.IsInteger(n) || strings.ToLower(tokens[0]) == "incrbyfloat" {
			arr = append(arr, resp.FloatValue(n))
		} else {
			arr = append(arr, resp.IntegerValue(int(n)))
		}

		wr.WriteArray(arr)
		return nil
	}
}

func Encode(buf io.ReadWriter, comm string) error {
	var err error = nil

	tokens, err := tokenize(comm)

	if err != nil {
		return errors.New("could not parse command")
	}

	wr := resp.NewWriter(buf)

	switch string(strings.ToLower(tokens[0])) {
	default:
		err = errors.New("unknown command")
	case "ping", "pong":
		err = encodePingPong(wr, tokens)
	case "set", "setnx":
		err = encodeSet(wr, tokens)
	case "get":
		err = encodeGet(wr, tokens)
	case "mget":
		err = encodeMGet(wr, tokens)
	case "incr", "incrby", "incrbyfloat":
		err = encodeIncr(wr, tokens)
	case "simplestring":
		err = encodeSimpleString(wr, tokens)
	case "Error":
		encodeError(wr, tokens)
		err = errors.New("failed to parse command")
	}

	return err
}
