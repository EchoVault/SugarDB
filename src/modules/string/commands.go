package str

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"net"
)

func handleSetRange(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := setRangeKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	offset, ok := utils.AdaptType(cmd[2]).(int)
	if !ok {
		return nil, errors.New("offset must be an integer")
	}

	newStr := cmd[3]

	if !server.KeyExists(ctx, key) {
		if _, err = server.CreateKeyAndLock(ctx, key); err != nil {
			return nil, err
		}
		if err = server.SetValue(ctx, key, newStr); err != nil {
			return nil, err
		}
		server.KeyUnlock(ctx, key)
		return []byte(fmt.Sprintf(":%d\r\n", len(newStr))), nil
	}

	if _, err := server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, key)

	str, ok := server.GetValue(ctx, key).(string)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a string", key)
	}

	// If the offset  >= length of the string, append the new string to the old one.
	if offset >= len(str) {
		newStr = str + newStr
		if err = server.SetValue(ctx, key, newStr); err != nil {
			return nil, err
		}
		return []byte(fmt.Sprintf(":%d\r\n", len(newStr))), nil
	}

	// If the offset is < 0, prepend the new string to the old one.
	if offset < 0 {
		newStr = newStr + str
		if err = server.SetValue(ctx, key, newStr); err != nil {
			return nil, err
		}
		return []byte(fmt.Sprintf(":%d\r\n", len(newStr))), nil
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

	if err = server.SetValue(ctx, key, string(strRunes)); err != nil {
		return nil, err
	}

	return []byte(fmt.Sprintf(":%d\r\n", len(strRunes))), nil
}

func handleStrLen(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := strLenKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	if !server.KeyExists(ctx, key) {
		return []byte(":0\r\n"), nil
	}

	if _, err := server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(ctx, key)

	value, ok := server.GetValue(ctx, key).(string)

	if !ok {
		return nil, fmt.Errorf("value at key %s is not a string", key)
	}

	return []byte(fmt.Sprintf(":%d\r\n", len(value))), nil
}

func handleSubStr(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := subStrKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	start, startOk := utils.AdaptType(cmd[2]).(int)
	end, endOk := utils.AdaptType(cmd[3]).(int)
	reversed := false

	if !startOk || !endOk {
		return nil, errors.New("start and end indices must be integers")
	}

	if !server.KeyExists(ctx, key) {
		return nil, fmt.Errorf("key %s does not exist", key)
	}

	if _, err = server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(ctx, key)

	value, ok := server.GetValue(ctx, key).(string)
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

	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)), nil
}

func Commands() []utils.Command {
	return []utils.Command{
		{
			Command:           "setrange",
			Categories:        []string{utils.StringCategory, utils.WriteCategory, utils.SlowCategory},
			Description:       "(SETRANGE key offset value) Overwrites part of a string value with another by offset. Creates the key if it doesn't exist.",
			Sync:              true,
			KeyExtractionFunc: setRangeKeyFunc,
			HandlerFunc:       handleSetRange,
		},
		{
			Command:           "strlen",
			Categories:        []string{utils.StringCategory, utils.ReadCategory, utils.FastCategory},
			Description:       "(STRLEN key) Returns length of the key's value if it's a string.",
			Sync:              false,
			KeyExtractionFunc: strLenKeyFunc,
			HandlerFunc:       handleStrLen,
		},
		{
			Command:           "substr",
			Categories:        []string{utils.StringCategory, utils.ReadCategory, utils.SlowCategory},
			Description:       "(SUBSTR key start end) Returns a substring from the string value.",
			Sync:              false,
			KeyExtractionFunc: subStrKeyFunc,
			HandlerFunc:       handleSubStr,
		},
		{
			Command:           "getrange",
			Categories:        []string{utils.StringCategory, utils.ReadCategory, utils.SlowCategory},
			Description:       "(GETRANGE key start end) Returns a substring from the string value.",
			Sync:              false,
			KeyExtractionFunc: subStrKeyFunc,
			HandlerFunc:       handleSubStr,
		},
	}
}
