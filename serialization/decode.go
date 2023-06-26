package serialization

import (
	"bytes"
	"fmt"

	"github.com/tidwall/resp"
)

func Decode(raw string) ([]string, error) {
	rd := resp.NewReader(bytes.NewBufferString(raw))

	v, _, err := rd.ReadValue()

	if err != nil {
		return nil, err
	}

	res := []string{}

	if v.Type().String() == "SimpleString" {
		return []string{v.String()}, nil
	}

	if v.Type().String() == "Array" {
		for _, elem := range v.Array() {
			res = append(res, elem.String())
		}
	}

	fmt.Println(res)
	return res, nil
}
