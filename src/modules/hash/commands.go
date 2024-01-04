package hash

import (
	"context"
	"errors"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
	"strconv"
	"strings"
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

func handleHSET(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 4 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]
	entries := make(map[string]interface{})

	if len(cmd[2:])%2 != 0 {
		return nil, errors.New("each field must have a corresponding value")
	}

	for i := 2; i <= len(cmd)-2; i += 2 {
		entries[cmd[i]] = utils.AdaptType(cmd[i+1])
	}

	if !server.KeyExists(key) {
		_, err := server.CreateKeyAndLock(ctx, key)
		if err != nil {
			return nil, err
		}
		defer server.KeyUnlock(key)
		server.SetValue(ctx, key, entries)
		return []byte(fmt.Sprintf(":%d\r\n\r\n", len(entries))), nil
	}

	_, err := server.KeyLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	hash, ok := server.GetValue(key).(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	count := 0
	for field, value := range entries {
		if strings.EqualFold(cmd[0], "hsetnx") {
			if hash[field] == nil {
				hash[field] = value
				count += 1
			}
			continue
		}
		hash[field] = value
		count += 1
	}
	server.SetValue(ctx, key, hash)

	return []byte(fmt.Sprintf(":%d\r\n\r\n", count)), nil
}

func handleHGET(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]
	fields := cmd[2:]

	if !server.KeyExists(key) {
		return []byte("_\r\n\r\n"), nil
	}

	_, err := server.KeyRLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	hash, ok := server.GetValue(key).(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	var value interface{}

	res := fmt.Sprintf("*%d\r\n", len(fields))
	for _, field := range fields {
		value = hash[field]
		if value == nil {
			res += "+(nil)\r\n"
			continue
		}
		if s, ok := value.(string); ok {
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
			continue
		}
		if d, ok := value.(int); ok {
			res += fmt.Sprintf(":%d\r\n", d)
			continue
		}
		if f, ok := value.(float64); ok {
			fs := strconv.FormatFloat(f, 'f', -1, 64)
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(fs), fs)
			continue
		}
		res += fmt.Sprintf("+(nil)\r\n")
	}
	res += "\r\n"

	return []byte(res), nil
}

func handleHSTRLEN(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]
	fields := cmd[2:]

	if !server.KeyExists(key) {
		return []byte("+(nil)\r\n\r\n"), nil
	}

	_, err := server.KeyRLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	hash, ok := server.GetValue(key).(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	var value interface{}

	res := fmt.Sprintf("*%d\r\n", len(fields))
	for _, field := range fields {
		value = hash[field]
		if value == nil {
			res += ":0\r\n"
			continue
		}
		if s, ok := value.(string); ok {
			res += fmt.Sprintf(":%d\r\n", len(s))
			continue
		}
		if f, ok := value.(float64); ok {
			fs := strconv.FormatFloat(f, 'f', -1, 64)
			res += fmt.Sprintf(":%d\r\n", len(fs))
			continue
		}
		if d, ok := value.(int); ok {
			res += fmt.Sprintf(":%d\r\n", d)
			continue
		}
		res += ":0\r\n"
	}
	res += "\r\n"

	return []byte(res), nil
}

func handleHVALS(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	key := cmd[1]

	if !server.KeyExists(key) {
		return []byte("*0\r\n\r\n"), nil
	}

	_, err := server.KeyRLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	hash, ok := server.GetValue(key).(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	res := fmt.Sprintf("*%d\r\n", len(hash))
	for _, val := range hash {
		if s, ok := val.(string); ok {
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
			continue
		}
		if f, ok := val.(float64); ok {
			fs := strconv.FormatFloat(f, 'f', -1, 64)
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(fs), fs)
			continue
		}
		if d, ok := val.(int); ok {
			res += fmt.Sprintf(":%d\r\n", d)
		}
	}
	res += "\r\n"

	return []byte(res), nil
}

func handleHRANDFIELD(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	return nil, errors.New("hrandfield command not implemented")
}

func handleHLEN(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) != 2 {
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

	hash, ok := server.GetValue(key).(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	return []byte(fmt.Sprintf(":%d\r\n\r\n", len(hash))), nil
}

func handleHKEYS(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]

	if !server.KeyExists(key) {
		return []byte("*0\r\n\r\n"), nil
	}

	if _, err := server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	hash, ok := server.GetValue(key).(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	res := fmt.Sprintf("*%d\r\n", len(hash))
	for field, _ := range hash {
		res += fmt.Sprintf("$%d\r\n%s\r\n", len(field), field)
	}
	res += "\r\n"

	return []byte(res), nil
}

func handleINCRBYFLOAT(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	return nil, errors.New("hincrbyfloat command not implemented")
}

func handleINCRBY(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	return nil, errors.New("hincrby command not implemented")
}

func handleHGETALL(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	return nil, errors.New("hgetall command not implemented")
}

func handleHEXISTS(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	return nil, errors.New("hexists command not implemented")
}

func handleHDEL(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	return nil, errors.New("hdel command not implemented")
}

func NewModule() Plugin {
	SetModule := Plugin{
		name: "HashCommands",
		commands: []utils.Command{
			{
				Command:     "hset",
				Categories:  []string{utils.HashCategory, utils.WriteCategory, utils.FastCategory},
				Description: `(HSET key field value [field value ...]) Set update each field of the hash with the corresponding value`,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: handleHSET,
			},
			{
				Command:     "hsetnx",
				Categories:  []string{utils.HashCategory, utils.WriteCategory, utils.FastCategory},
				Description: `(HSETNX key field value [field value ...]) Set hash field value only if the field does not exist`,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: handleHSET,
			},
			{
				Command:     "hget",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.FastCategory},
				Description: `(HGET key field [field ...]) Retrieve the of each of the listed fields from the hash`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: handleHGET,
			},
			{
				Command:    "hstrlen",
				Categories: []string{utils.HashCategory, utils.ReadCategory, utils.FastCategory},
				Description: `(HSTRLEN key field [field ...]) 
Return the string length of the values stored at the specified fields. 0 if the value does not exist`,
				Sync: false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: handleHSTRLEN,
			},
			{
				Command:     "hvals",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.SlowCategory},
				Description: `(HVALS key) Returns all the values of the hash at key.`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleHVALS,
			},
			{
				Command:     "hrandfield",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.SlowCategory},
				Description: `(HRANDFIELD key [count] [WITHVALUES]) Returns one or more random fields from the hash`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 || len(cmd) > 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleHRANDFIELD,
			},
			{
				Command:     "hlen",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.FastCategory},
				Description: `(HLEN key) Returns the number of fields in the hash`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleHLEN,
			},
			{
				Command:     "hkeys",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.SlowCategory},
				Description: `(HKEYS key) Returns all the fields in a hash`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleHKEYS,
			},
			{
				Command:     "hincrbyfloat",
				Categories:  []string{utils.HashCategory, utils.WriteCategory, utils.FastCategory},
				Description: `(HINCRBYFLOAT key field increment) Increment the hash value by the float increment`,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: handleINCRBYFLOAT,
			},
			{
				Command:     "hincrby",
				Categories:  []string{utils.HashCategory, utils.WriteCategory, utils.FastCategory},
				Description: `(HINCRBY key field increment) Increment the hash value by the integer increment`,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: handleINCRBY,
			},
			{
				Command:     "hgetall",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.SlowCategory},
				Description: `(HGETALL key) Get all fields and values of a hash`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleHGETALL,
			},
			{
				Command:     "hexists",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.FastCategory},
				Description: `(HEXISTS key field) Returns if field is an existing field in the hash`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: handleHEXISTS,
			},
			{
				Command:     "hdel",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.FastCategory},
				Description: `(HDEL key field [field ...]) Deletes the specified fields from the hash`,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: handleHDEL,
			},
		},
		description: "Handle HASH commands",
	}

	return SetModule
}
