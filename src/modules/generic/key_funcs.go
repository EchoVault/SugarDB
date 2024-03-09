package generic

import (
	"errors"
	"github.com/echovault/echovault/src/utils"
)

func setKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 || len(cmd) > 7 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return []string{cmd[1]}, nil
}

func msetKeyFunc(cmd []string) ([]string, error) {
	if len(cmd[1:])%2 != 0 {
		return nil, errors.New("each key must be paired with a value")
	}
	var keys []string
	for i, key := range cmd[1:] {
		if i%2 == 0 {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func getKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return []string{cmd[1]}, nil
}

func mgetKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:], nil
}

func delKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:], nil
}

func persistKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:], nil
}

func expireTimeKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:], nil
}

func ttlKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:], nil
}

func expireKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 || len(cmd) > 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return []string{cmd[1]}, nil
}

func expireAtKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 || len(cmd) > 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return []string{cmd[1]}, nil
}
