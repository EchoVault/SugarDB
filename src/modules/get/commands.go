package get

import (
	"context"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"net"
)

func handleGet(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := getKeyFunc(cmd)
	if err != nil {
		return nil, err
	}
	key := keys[0]

	if !server.KeyExists(key) {
		return []byte("+nil\r\n\r\n"), nil
	}

	_, err = server.KeyRLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	value := server.GetValue(key)

	return []byte(fmt.Sprintf("+%v\r\n\r\n", value)), nil
}

func handleMGet(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := mgetKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	values := make(map[string]string)

	locks := make(map[string]bool)
	for _, key := range keys {
		if _, ok := values[key]; ok {
			// Skip if we have already locked this key
			continue
		}
		if server.KeyExists(key) {
			_, err = server.KeyRLock(ctx, key)
			if err != nil {
				return nil, fmt.Errorf("could not obtain lock for %s key", key)
			}
			locks[key] = true
			continue
		}
		values[key] = "nil"
	}
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
				locks[key] = false
			}
		}
	}()

	for key, _ := range locks {
		values[key] = fmt.Sprintf("%v", server.GetValue(key))
	}

	bytes := []byte(fmt.Sprintf("*%d\r\n", len(cmd[1:])))

	for _, key := range cmd[1:] {
		bytes = append(bytes, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(values[key]), values[key]))...)
	}

	bytes = append(bytes, []byte("\r\n")...)

	return bytes, nil
}

func Commands() []utils.Command {
	return []utils.Command{
		{
			Command:           "get",
			Categories:        []string{utils.ReadCategory, utils.FastCategory},
			Description:       "(GET key) Get the value at the specified key.",
			Sync:              false,
			KeyExtractionFunc: getKeyFunc,
			HandlerFunc:       handleGet,
		},
		{
			Command:           "mget",
			Categories:        []string{utils.ReadCategory, utils.FastCategory},
			Description:       "(MGET key1 [key2]) Get multiple values from the specified keys.",
			Sync:              false,
			KeyExtractionFunc: mgetKeyFunc,
			HandlerFunc:       handleMGet,
		},
	}
}
