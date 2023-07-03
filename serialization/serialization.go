package serialization

import (
	"bytes"

	"encoding/csv"
	"errors"
	"fmt"
	"strings"

	"github.com/kelvinmwinuka/memstore/utils"
	"github.com/tidwall/resp"
)

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

	if utils.Contains[string]([]string{"SimpleString", "Integer", "Error"}, v.Type().String()) {
		return []string{v.String()}, nil
	}

	if v.Type().String() == "Array" {
		for _, elem := range v.Array() {
			res = append(res, elem.String())
		}
	}

	return res, nil
}
