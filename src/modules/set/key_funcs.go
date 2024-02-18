package set

import (
	"errors"
	"github.com/echovault/echovault/src/utils"
	"slices"
	"strings"
)

func saddKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func scardKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func sdiffKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return cmd[1:], nil
}

func sdiffstoreKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return cmd[1:], nil
}

func sinterKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return cmd[1:], nil
}

func sintercardKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	limitIdx := slices.IndexFunc(cmd, func(s string) bool {
		return strings.EqualFold(s, "limit")
	})

	if limitIdx == -1 {
		return cmd[1:], nil
	}

	return cmd[1:limitIdx], nil
}

func sinterstoreKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return cmd[1:], nil
}

func sismemberKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return cmd[1:], nil
}

func smembersKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func smismemberKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func smoveKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return cmd[1:3], nil
}

func spopKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 || len(cmd) > 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return cmd[1:2], nil
}

func srandmemberKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 || len(cmd) > 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return cmd[1:2], nil
}

func sremKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return []string{cmd[1]}, nil
}

func sunionKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return cmd[1:], nil
}

func sunionstoreKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	return cmd[1:], nil
}
