package generic

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type SetParams struct {
	exists   string
	get      bool
	expireAt interface{} // Exact expireAt time un unix milliseconds
}

func getSetCommandParams(cmd []string, params SetParams) (SetParams, error) {
	if len(cmd) == 0 {
		return params, nil
	}
	switch strings.ToLower(cmd[0]) {
	case "get":
		params.get = true
		return getSetCommandParams(cmd[1:], params)

	case "nx":
		if params.exists != "" {
			return SetParams{}, fmt.Errorf("cannot specify NX when %s is already specified", strings.ToUpper(params.exists))
		}
		params.exists = "NX"
		return getSetCommandParams(cmd[1:], params)

	case "xx":
		if params.exists != "" {
			return SetParams{}, fmt.Errorf("cannot specify XX when %s is already specified", strings.ToUpper(params.exists))
		}
		params.exists = "XX"
		return getSetCommandParams(cmd[1:], params)

	case "ex":
		if len(cmd) < 2 {
			return SetParams{}, errors.New("seconds value required after EX")
		}
		if params.expireAt != nil {
			return SetParams{}, errors.New("cannot specify EX when expiry time is already set")
		}
		secondsStr := cmd[1]
		seconds, err := strconv.ParseInt(secondsStr, 10, 64)
		if err != nil {
			return SetParams{}, err
		}
		params.expireAt = time.Now().Add(time.Duration(seconds) * time.Second)
		return getSetCommandParams(cmd[2:], params)

	case "px":
		if len(cmd) < 2 {
			return SetParams{}, errors.New("seconds value required after PX")
		}
		if params.expireAt != nil {
			return SetParams{}, errors.New("cannot specify PX when expiry time is already set")
		}
		millisecondsStr := cmd[1]
		milliseconds, err := strconv.ParseInt(millisecondsStr, 10, 64)
		if err != nil {
			return SetParams{}, err
		}
		params.expireAt = time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
		return getSetCommandParams(cmd[2:], params)

	case "exat":
		if len(cmd) < 2 {
			return SetParams{}, errors.New("seconds value required after EXAT")
		}
		if params.expireAt != nil {
			return SetParams{}, errors.New("cannot specify EXAT when expiry time is already set")
		}
		secondsStr := cmd[1]
		seconds, err := strconv.ParseInt(secondsStr, 10, 64)
		if err != nil {
			return SetParams{}, err
		}
		params.expireAt = time.Unix(seconds, 0)
		return getSetCommandParams(cmd[2:], params)

	case "pxat":
		if len(cmd) < 2 {
			return SetParams{}, errors.New("seconds value required after PXAT")
		}
		if params.expireAt != nil {
			return SetParams{}, errors.New("cannot specify PXAT when expiry time is already set")
		}
		millisecondsStr := cmd[1]
		milliseconds, err := strconv.ParseInt(millisecondsStr, 10, 64)
		if err != nil {
			return SetParams{}, err
		}
		params.expireAt = time.UnixMilli(milliseconds)
		return getSetCommandParams(cmd[2:], params)

	default:
		return SetParams{}, fmt.Errorf("unknown option %s for set command", strings.ToUpper(cmd[0]))
	}
}
