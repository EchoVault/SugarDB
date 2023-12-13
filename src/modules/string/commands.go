package str

import (
	"context"
	"errors"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
	"strings"
)

type Plugin struct {
	name        string
	commands    []utils.Command
	description string
}

var StringModule Plugin

func (p Plugin) Name() string {
	return p.name
}

func (p Plugin) Commands() []utils.Command {
	return p.commands
}

func (p Plugin) Description() string {
	return p.description
}

func (p Plugin) HandleCommand(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	switch strings.ToLower(cmd[0]) {
	default:
		return nil, errors.New("command unknown")
	case "setrange":
		return handleSetRange(ctx, cmd, server)
	case "strlen":
		return handleStrLen(ctx, cmd, server)
	case "substr":
		return handleSubStr(ctx, cmd, server)
	case "getrange":
		return handleSubStr(ctx, cmd, server)
	}
}

func handleSetRange(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if len(cmd[1:]) != 3 {
		return nil, errors.New("wrong number of args for SETRANGE command")
	}

	key := cmd[1]

	offset, ok := utils.AdaptType(cmd[2]).(int64)
	if !ok {
		return nil, errors.New("offset must be integer")
	}

	newStr := cmd[3]

	if !server.KeyExists(key) {
		if _, err := server.CreateKeyAndLock(ctx, key); err != nil {
			return nil, err
		}
		server.SetValue(ctx, key, newStr)
		server.KeyUnlock(key)
		return []byte(fmt.Sprintf(":%d\r\n\n", len(newStr))), nil
	}

	str, ok := server.GetValue(key).(string)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a string", key)
	}

	if _, err := server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	if offset >= int64(len(str)) {
		newStr = str + newStr
		server.SetValue(ctx, key, newStr)
		return []byte(fmt.Sprintf(":%d\r\n\n", len(newStr))), nil
	}

	if offset < 0 {
		newStr = newStr + str
		server.SetValue(ctx, key, newStr)
		return []byte(fmt.Sprintf(":%d\r\n\n", len(newStr))), nil
	}

	if offset == 0 {
		newStr = newStr + strings.Join(strings.Split(str, "")[1:], "")
		server.SetValue(ctx, key, newStr)
		return []byte(fmt.Sprintf(":%d\r\n\n", len(newStr))), nil
	}

	if offset == int64(len(str))-1 {
		newStr = strings.Join(strings.Split(str, "")[0:len(str)-1], "") + newStr
		server.SetValue(ctx, key, newStr)
		return []byte(fmt.Sprintf(":%d\r\n\n", len(newStr))), nil
	}

	strArr := strings.Split(str, "")
	newStrArr := append(strArr[0:offset], append(strings.Split(newStr, ""), strArr[offset+1:]...)...)

	newStr = strings.Join(newStrArr, "")
	server.SetValue(ctx, key, newStr)

	return []byte(fmt.Sprintf(":%d\r\n\n", len(newStr))), nil
}

func handleStrLen(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if len(cmd[1:]) != 1 {
		return nil, errors.New("wrong number of args for STRLEN command")
	}

	key := cmd[1]

	if !server.KeyExists(key) {
		return []byte(":0\r\n\n"), nil
	}

	if _, err := server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	value, ok := server.GetValue(key).(string)

	if !ok {
		return nil, fmt.Errorf("value at key %s is not a string", key)
	}

	return []byte(fmt.Sprintf(":%d\r\n\n", len(value))), nil
}

func handleSubStr(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if len(cmd[1:]) != 3 {
		return nil, errors.New("wrong number of args for SUBSTR command")
	}

	key := cmd[1]

	start, startOk := utils.AdaptType(cmd[2]).(int64)
	end, endOk := utils.AdaptType(cmd[3]).(int64)
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

	if end >= 0 {
		end += 1
	}

	if start < 0 {
		start = int64(len(value)) + start
	}
	if end < 0 {
		end = int64(len(value)) + end
	}

	if end > int64(len(value)) {
		end = int64(len(value))
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

	return []byte(fmt.Sprintf("$%d\r\n%s\r\n\n", len(str), str)), nil
}

func NewModule() Plugin {
	StringModule := Plugin{
		name: "StringCommands",
		commands: []utils.Command{
			{
				Command:     "setrange",
				Categories:  []string{},
				Description: "(SETRANGE key offset value) Overwrites part of a string value with another by offset. Creates the key if it doesn't exist.",
				Sync:        true,
			},
			{
				Command:     "strlen",
				Categories:  []string{},
				Description: "(STRLEN key) Returns length of the key's value if it's a string.",
				Sync:        false,
			},
			{
				Command:     "substr",
				Categories:  []string{},
				Description: "(SUBSTR key start end) Returns a substring from the string value.",
				Sync:        false,
			},
			{
				Command:     "getrange",
				Categories:  []string{},
				Description: "(GETRANGE key start end) Returns a substring from the string value.",
				Sync:        false,
			},
		},
		description: "Handle basic STRING commands",
	}
	return StringModule
}
