package list

import (
	"errors"
	"github.com/echovault/echovault/src/utils"
)

func lpushKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func popKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func llenKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func lrangeKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func lindexKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func lsetKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func ltrimKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func lremKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func rpushKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func lmoveKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 5 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1], cmd[2]}, nil
}
