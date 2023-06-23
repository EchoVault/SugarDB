package serialization

import (
	"bytes"
	"fmt"
	"io"

	"github.com/tidwall/resp"
)

func Decode(raw string) {
	rd := resp.NewReader(bytes.NewBufferString(raw))

	for {
		v, _, err := rd.ReadValue()

		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Println(err)
		}

		fmt.Println(v)
		if v.Type().String() == "Array" {
			for _, elem := range v.Array() {
				fmt.Printf("%s: %v\n", elem.Type().String(), elem)
			}
		}
	}
}
