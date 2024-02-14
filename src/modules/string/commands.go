package str

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"net"
)

type Plugin struct {
	name        string
	commands    []utils.Command
	description string
}

func (p Plugin) Name() string {
	return p.name
}

func (p Plugin) Commands() []utils.Command {
	return p.commands
}

func (p Plugin) Description() string {
	return p.description
}

func handleSetRange(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd[1:]) != 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]

	offset, ok := utils.AdaptType(cmd[2]).(int)
	if !ok {
		return nil, errors.New("offset must be an integer")
	}

	newStr := cmd[3]

	if !server.KeyExists(key) {
		if _, err := server.CreateKeyAndLock(ctx, key); err != nil {
			return nil, err
		}
		server.SetValue(ctx, key, newStr)
		server.KeyUnlock(key)
		return []byte(fmt.Sprintf(":%d\r\n\r\n", len(newStr))), nil
	}

	if _, err := server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	str, ok := server.GetValue(key).(string)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a string", key)
	}

	// If the offset  >= length of the string, append the new string to the old one.
	if offset >= len(str) {
		newStr = str + newStr
		server.SetValue(ctx, key, newStr)
		return []byte(fmt.Sprintf(":%d\r\n\r\n", len(newStr))), nil
	}

	// If the offset is < 0, prepend the new string to the old one.
	if offset < 0 {
		newStr = newStr + str
		server.SetValue(ctx, key, newStr)
		return []byte(fmt.Sprintf(":%d\r\n\r\n", len(newStr))), nil
	}

	strRunes := []rune(str)

	for i := 0; i < len(newStr); i++ {
		// If we're still withing the length of the original string, replace the rune in strRunes
		if offset < len(str) {
			strRunes[offset] = rune(newStr[i])
			offset += 1
			continue
		}
		// We are past the length of the original string, append the remainder of newStr to strRunes
		strRunes = append(strRunes, []rune(newStr)[i:]...)
		break
	}

	server.SetValue(ctx, key, string(strRunes))

	return []byte(fmt.Sprintf(":%d\r\n\r\n", len(strRunes))), nil
}

func handleStrLen(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd[1:]) != 1 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]

	if !server.KeyExists(key) {
		return []byte(":0\r\n\r\n"), nil
	}

	if _, err := server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	value, ok := server.GetValue(key).(string)

	if !ok {
		return nil, fmt.Errorf("value at key %s is not a string", key)
	}

	return []byte(fmt.Sprintf(":%d\r\n\r\n", len(value))), nil
}

func handleSubStr(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd[1:]) != 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]

	start, startOk := utils.AdaptType(cmd[2]).(int)
	end, endOk := utils.AdaptType(cmd[3]).(int)
	reversed := false

	if !startOk || !endOk {
		return nil, errors.New("start and end indices must be integers")
	}

	if !server.KeyExists(key) {
		return nil, fmt.Errorf("key %s does not exist", key)
	}

	if _, err := server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	value, ok := server.GetValue(key).(string)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a string", key)
	}

	if start < 0 {
		start = len(value) - utils.AbsInt(start)
	}
	if end < 0 {
		end = len(value) - utils.AbsInt(end)
	}

	if end >= 0 && end >= start {
		end += 1
	}

	if end > len(value) {
		end = len(value)
	}

	if start > end {
		reversed = true
		start, end = end, start
	}

	str := value[start:end]

	if reversed {
		res := ""
		for i := len(str) - 1; i >= 0; i-- {
			res = res + string(str[i])
		}
		str = res
	}

	return []byte(fmt.Sprintf("$%d\r\n%s\r\n\r\n", len(str), str)), nil
}

func Commands() []utils.Command {
	return []utils.Command{
		{
			Command:     "setrange",
			Categories:  []string{utils.StringCategory, utils.WriteCategory, utils.SlowCategory},
			Description: "(SETRANGE key offset value) Overwrites part of a string value with another by offset. Creates the key if it doesn't exist.",
			Sync:        true,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				if len(cmd) != 4 {
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
				}
				return []string{cmd[1]}, nil
			},
			HandlerFunc: handleSetRange,
		},
		{
			Command:     "strlen",
			Categories:  []string{utils.StringCategory, utils.ReadCategory, utils.FastCategory},
			Description: "(STRLEN key) Returns length of the key's value if it's a string.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				if len(cmd) != 2 {
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
				}
				return []string{cmd[1]}, nil
			},
			HandlerFunc: handleStrLen,
		},
		{
			Command:     "substr",
			Categories:  []string{utils.StringCategory, utils.ReadCategory, utils.SlowCategory},
			Description: "(SUBSTR key start end) Returns a substring from the string value.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				if len(cmd) != 4 {
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
				}
				return []string{cmd[1]}, nil
			},
			HandlerFunc: handleSubStr,
		},
		{
			Command:     "getrange",
			Categories:  []string{utils.StringCategory, utils.ReadCategory, utils.SlowCategory},
			Description: "(GETRANGE key start end) Returns a substring from the string value.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				if len(cmd) != 4 {
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
				}
				return []string{cmd[1]}, nil
			},
			HandlerFunc: handleSubStr,
		},
	}
}
