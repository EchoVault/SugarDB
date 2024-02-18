package get

import (
	"errors"
	"github.com/echovault/echovault/src/utils"
)

func getKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func mgetKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return cmd[1:], nil
}
