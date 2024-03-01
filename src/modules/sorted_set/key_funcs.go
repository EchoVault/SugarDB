package sorted_set

import (
	"errors"
	"github.com/echovault/echovault/src/utils"
	"slices"
	"strings"
)

func zaddKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zcardKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:], nil
}

func zcountKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zdiffKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}

	withscoresIndex := slices.IndexFunc(cmd, func(s string) bool {
		return strings.EqualFold(s, "withscores")
	})

	if withscoresIndex == -1 {
		return cmd[1:], nil
	}

	return cmd[1:withscoresIndex], nil
}

func zdiffstoreKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[2:], nil
}

func zincrbyKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zinterKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	endIdx := slices.IndexFunc(cmd[1:], func(s string) bool {
		if strings.EqualFold(s, "WEIGHTS") ||
			strings.EqualFold(s, "AGGREGATE") ||
			strings.EqualFold(s, "WITHSCORES") {
			return true
		}
		return false
	})
	if endIdx == -1 {
		return cmd[1:], nil
	}
	if endIdx >= 1 {
		return cmd[1:endIdx], nil
	}
	return nil, errors.New(utils.WrongArgsResponse)
}

func zinterstoreKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	endIdx := slices.IndexFunc(cmd[1:], func(s string) bool {
		if strings.EqualFold(s, "WEIGHTS") ||
			strings.EqualFold(s, "AGGREGATE") ||
			strings.EqualFold(s, "WITHSCORES") {
			return true
		}
		return false
	})
	if endIdx == -1 {
		return cmd[1:], nil
	}
	if endIdx >= 2 {
		return cmd[1:endIdx], nil
	}
	return nil, errors.New(utils.WrongArgsResponse)
}

func zmpopKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	endIdx := slices.IndexFunc(cmd, func(s string) bool {
		return slices.Contains([]string{"MIN", "MAX", "COUNT"}, strings.ToUpper(s))
	})
	if endIdx == -1 {
		return cmd[1:], nil
	}
	if endIdx >= 2 {
		return cmd[1:endIdx], nil
	}
	return nil, errors.New(utils.WrongArgsResponse)
}

func zmscoreKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zpopKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 || len(cmd) > 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zrandmemberKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 || len(cmd) > 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zrankKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 || len(cmd) > 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zremKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zrevrankKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zscoreKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 3 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zremrangebylexKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zremrangebyrankKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zremrangebyscoreKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zlexcountKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zrangeKeyCount(cmd []string) ([]string, error) {
	if len(cmd) < 4 || len(cmd) > 10 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:2], nil
}

func zrangeStoreKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 5 || len(cmd) > 11 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	return cmd[1:3], nil
}

func zunionKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	endIdx := slices.IndexFunc(cmd[1:], func(s string) bool {
		if strings.EqualFold(s, "WEIGHTS") ||
			strings.EqualFold(s, "AGGREGATE") ||
			strings.EqualFold(s, "WITHSCORES") {
			return true
		}
		return false
	})
	if endIdx == -1 {
		return cmd[1:], nil
	}
	if endIdx >= 1 {
		return cmd[1:endIdx], nil
	}
	return nil, errors.New(utils.WrongArgsResponse)
}

func zunionstoreKeyFunc(cmd []string) ([]string, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WrongArgsResponse)
	}
	endIdx := slices.IndexFunc(cmd[1:], func(s string) bool {
		if strings.EqualFold(s, "WEIGHTS") ||
			strings.EqualFold(s, "AGGREGATE") ||
			strings.EqualFold(s, "WITHSCORES") {
			return true
		}
		return false
	})
	if endIdx == -1 {
		return cmd[1:], nil
	}
	if endIdx >= 1 {
		return cmd[1:endIdx], nil
	}
	return nil, errors.New(utils.WrongArgsResponse)
}
