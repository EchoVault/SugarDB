package serialization

import (
	"bytes"
	"errors"
	"fmt"
	"log"
)

func tokenize(b []byte) ([][]byte, error) {
	qOpen := false
	transformed := []byte("")

	for _, c := range b {

		if c != ' ' && c != '"' {
			transformed = append(transformed, c)
			continue
		}

		if c == '"' {
			qOpen = !qOpen

			if qOpen && !bytes.HasSuffix(transformed, []byte(" ")) {
				transformed = append(transformed, ' ')
			}

			transformed = append(transformed, c)
			continue
		}

		if c == ' ' && qOpen {
			transformed = append(transformed, []byte("*-*")...)
			continue
		}

		if c == ' ' && !qOpen {
			transformed = append(transformed, c)
			continue
		}
	}

	if qOpen {
		return nil, errors.New("open quote in command")
	}

	tokens := bytes.Split(transformed, []byte(" "))

	for i := 0; i < len(tokens); i++ {
		tokens[i] = bytes.Trim(tokens[i], "\"")
		tokens[i] = bytes.ReplaceAll(tokens[i], []byte("*-*"), []byte(" "))
	}

	return tokens, nil
}

func Encode(b []byte) []byte {
	tokens, err := tokenize(b)

	if err != nil {
		log.Fatal(err)
	}

	if len(tokens) <= 0 {
		return b
	}

	if len(tokens) == 1 && bytes.Equal(bytes.ToLower(tokens[0]), []byte("ping")) {
		return []byte(fmt.Sprintf("+%s\r\n", string(bytes.ToUpper(tokens[0]))))
	}

	if len(tokens) > 1 && bytes.Equal(bytes.ToLower(tokens[0]), []byte("ping")) {
		enc := []byte(fmt.Sprintf("*%d\r\n$%d\r\n%s\r\n",
			len(tokens), len(tokens[0]), string(bytes.ToUpper(tokens[0]))))
		for i := 1; i < len(tokens); i++ {
			token := tokens[i]
			enc = append(enc, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(token), token))...)
		}
		return enc
	}

	return b
}
