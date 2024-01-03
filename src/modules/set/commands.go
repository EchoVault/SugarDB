package set

import (
	"context"
	"errors"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
	"slices"
	"strings"
)

type Plugin struct {
	name        string
	commands    []utils.Command
	categories  []string
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

func handleSADD(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]

	var set *Set

	if !server.KeyExists(key) {
		set = NewSet(cmd[2:])
		if ok, err := server.CreateKeyAndLock(ctx, key); !ok && err != nil {
			return nil, err
		}
		server.SetValue(ctx, key, set)
		server.KeyUnlock(key)
		return []byte(fmt.Sprintf(":%d\r\n\r\n", len(cmd[2:]))), nil
	}

	_, err := server.KeyLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	count := set.Add(cmd[2:])

	return []byte(fmt.Sprintf(":%d\r\n\n", count)), nil
}

func handleSCARD(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) != 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]

	if !server.KeyExists(key) {
		return []byte(fmt.Sprintf(":0\r\n\r\n")), nil
	}

	_, err := server.KeyRLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	cardinality := set.Cardinality()

	return []byte(fmt.Sprintf(":%d\r\n\r\n", cardinality)), nil
}

func handleSDIFF(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range cmd[1:] {
		if !server.KeyExists(key) {
			continue
		}
		_, err := server.KeyRLock(ctx, key)
		if err != nil {
			continue
		}
		locks[key] = true
	}

	var sets []*Set
	for key, _ := range locks {
		set, ok := server.GetValue(key).(*Set)
		if !ok {
			continue
		}
		sets = append(sets, set)
	}

	if len(sets) <= 0 {
		return nil, errors.New("not enough sets in the keys provided")
	}

	diff := sets[0].Subtract(sets[1:])
	elems := diff.GetAll()

	res := fmt.Sprintf("*%d", len(elems))
	for i, e := range elems {
		res = fmt.Sprintf("%s\r\n$%d\r\n%s", res, len(e), e)
		if i == len(elems)-1 {
			res += "\r\n\r\n"
		}
	}

	return []byte(res), nil
}

func handleSDIFFSTORE(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	destination := cmd[1]

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range cmd[2:] {
		if !server.KeyExists(key) {
			continue
		}
		_, err := server.KeyRLock(ctx, key)
		if err != nil {
			continue
		}
		locks[key] = true
	}

	var sets []*Set
	for key, _ := range locks {
		set, ok := server.GetValue(key).(*Set)
		if !ok {
			continue
		}
		sets = append(sets, set)
	}

	if len(sets) <= 0 {
		return nil, errors.New("not enough sets in the keys provided")
	}

	diff := sets[0].Subtract(sets[1:])
	elems := diff.GetAll()

	res := fmt.Sprintf(":%d\r\n\r\n", len(elems))

	if server.KeyExists(destination) {
		if _, err := server.KeyLock(ctx, destination); err != nil {
			return nil, err
		}
		server.SetValue(ctx, destination, diff)
		server.KeyUnlock(destination)
		return []byte(res), nil
	}

	if _, err := server.CreateKeyAndLock(ctx, destination); err != nil {
		return nil, err
	}
	server.SetValue(ctx, destination, diff)
	server.KeyUnlock(destination)

	return []byte(res), nil
}

func handleSINTER(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range cmd[1:] {
		if !server.KeyExists(key) {
			// If key does not exist, then there is no intersection
			return []byte("*0\r\n\r\n"), nil
		}
		_, err := server.KeyRLock(ctx, key)
		if err != nil {
			return nil, err
		}
		locks[key] = true
	}

	var sets []*Set

	for key, _ := range locks {
		set, ok := server.GetValue(key).(*Set)
		if !ok {
			// If the value at the key is not a set, return error
			return nil, fmt.Errorf("value at key %s is not a set", key)
		}
		sets = append(sets, set)
	}

	if len(sets) <= 0 {
		return nil, fmt.Errorf("not enough sets in the keys provided")
	}

	intersect := sets[0].Intersection(sets[1:], 0)
	elems := intersect.GetAll()

	res := fmt.Sprintf("*%d", len(elems))
	for i, e := range elems {
		res = fmt.Sprintf("%s\r\n$%d\r\n%s", res, len(e), e)
		if i == len(elems)-1 {
			res += "\r\n\r\n"
		}
	}

	return []byte(res), nil
}

func handleSINTERCARD(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	// Extract the limit from the command
	var limit int
	limitIdx := slices.IndexFunc(cmd, func(s string) bool {
		return strings.EqualFold(s, "limit")
	})
	if limitIdx >= 0 && limitIdx < 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}
	if limitIdx != -1 {
		limitIdx += 1
		if limitIdx >= len(cmd) {
			return nil, errors.New("provide limit after LIMIT keyword")
		}

		if l, ok := utils.AdaptType(cmd[limitIdx]).(int); !ok {
			return nil, errors.New("limit must be an integer")
		} else {
			limit = l
		}
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	var keySlice []string
	if limitIdx == -1 {
		keySlice = cmd[1:]
	} else {
		keySlice = cmd[1 : limitIdx-1]
	}

	for _, key := range keySlice {
		if !server.KeyExists(key) {
			// If key does not exist, then there is no intersection
			return []byte("*0\r\n\r\n"), nil
		}
		_, err := server.KeyRLock(ctx, key)
		if err != nil {
			return nil, err
		}
		locks[key] = true
	}

	var sets []*Set

	for key, _ := range locks {
		set, ok := server.GetValue(key).(*Set)
		if !ok {
			// If the value at the key is not a set, return error
			return nil, fmt.Errorf("value at key %s is not a set", key)
		}
		sets = append(sets, set)
	}

	if len(sets) <= 0 {
		return nil, fmt.Errorf("not enough sets in the keys provided")
	}

	intersect := sets[0].Intersection(sets[1:], limit)

	return []byte(fmt.Sprintf(":%d\r\n\r\n", intersect.Cardinality())), nil
}

func handleSINTERSTORE(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range cmd[2:] {
		if !server.KeyExists(key) {
			// If key does not exist, then there is no intersection
			return []byte("*0\r\n\r\n"), nil
		}
		_, err := server.KeyRLock(ctx, key)
		if err != nil {
			return nil, err
		}
		locks[key] = true
	}

	var sets []*Set

	for key, _ := range locks {
		set, ok := server.GetValue(key).(*Set)
		if !ok {
			// If the value at the key is not a set, return error
			return nil, fmt.Errorf("value at key %s is not a set", key)
		}
		sets = append(sets, set)
	}

	if len(sets) <= 0 {
		return nil, fmt.Errorf("not enough sets in the keys provided")
	}

	intersect := sets[0].Intersection(sets[1:], 0)
	destination := cmd[1]

	if server.KeyExists(destination) {
		if _, err := server.KeyLock(ctx, destination); err != nil {
			return nil, err
		}
	} else {
		if _, err := server.CreateKeyAndLock(ctx, destination); err != nil {
			return nil, err
		}
	}

	server.SetValue(ctx, destination, intersect)
	server.KeyUnlock(destination)

	return []byte(fmt.Sprintf(":%d\r\n\r\n", intersect.Cardinality())), nil
}

func handleSISMEMBER(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) != 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]

	if !server.KeyExists(key) {
		return []byte(":0\r\n\r\n"), nil
	}

	_, err := server.KeyRLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	if !set.Contains(cmd[2]) {
		return []byte(":0\r\n\r\n"), nil
	}

	return []byte(":1\r\n\r\n"), nil
}

func handleSMEMBERS(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
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

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	elems := set.GetAll()

	res := fmt.Sprintf("*%d", len(elems))
	for i, e := range elems {
		res = fmt.Sprintf("%s\r\n$%d\r\n%s", res, len(e), e)
		if i == len(elems)-1 {
			res += "\r\n\r\n"
		}
	}

	return []byte(res), nil
}

func handleSMISMEMBER(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]
	members := cmd[2:]

	if !server.KeyExists(key) {
		res := fmt.Sprintf("*%d", len(members))
		for i, _ := range members {
			res = fmt.Sprintf("%s\r\n:0", res)
			if i == len(members)-1 {
				res += "\r\n\r\n"
			}
		}
		return []byte(res), nil
	}

	_, err := server.KeyRLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	res := fmt.Sprintf("*%d", len(members))
	for i, m := range members {
		if set.Contains(m) {
			res += "\r\n:1"
		} else {
			res += "\r\n:0"
		}
		if i == len(members)-1 {
			res += "\r\n\r\n"
		}
	}

	return []byte(res), nil
}

func handleSMOVE(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	source := cmd[1]
	destination := cmd[2]
	member := cmd[3]

	if !server.KeyExists(source) {
		return []byte(":0\r\n\r\n"), nil
	}

	_, err := server.KeyLock(ctx, source)
	if err != nil {
		return nil, err
	}
	defer server.KeyUnlock(source)

	sourceSet, ok := server.GetValue(source).(*Set)
	if !ok {
		return nil, errors.New("source is not a set")
	}

	var destinationSet *Set

	if !server.KeyExists(destination) {
		// Destination key does not exist
		_, err := server.CreateKeyAndLock(ctx, destination)
		if err != nil {
			return nil, err
		}
		defer server.KeyUnlock(destination)
		destinationSet = NewSet([]string{})
		server.SetValue(ctx, destination, destinationSet)
	} else {
		// Destination key exists
		_, err := server.KeyLock(ctx, destination)
		if err != nil {
			return nil, err
		}
		defer server.KeyUnlock(destination)
		ds, ok := server.GetValue(destination).(*Set)
		if !ok {
			return nil, errors.New("destination is not a set")
		}
		destinationSet = ds
	}

	res := sourceSet.Move(destinationSet, member)

	return []byte(fmt.Sprintf(":%d\r\n\r\n", res)), nil
}

func handleSPOP(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 2 || len(cmd) > 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]
	count := 1

	if len(cmd) == 3 {
		c, ok := utils.AdaptType(cmd[2]).(int)
		if !ok {
			return nil, errors.New("count must be an integer")
		}
		count = c
	}

	if !server.KeyExists(key) {
		return []byte("*-1\r\n\r\n"), nil
	}

	_, err := server.KeyLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a set", key)
	}

	members := set.Pop(count)

	res := fmt.Sprintf("*%d", len(members))
	for i, m := range members {
		res = fmt.Sprintf("%s\r\n$%d\r\n%s", res, len(m), m)
		if i == len(members)-1 {
			res += "\r\n\r\n"
		}
	}

	return []byte(res), nil
}

func handleSRANDMEMBER(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 2 || len(cmd) > 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]
	count := 1

	if len(cmd) == 3 {
		c, ok := utils.AdaptType(cmd[2]).(int)
		if !ok {
			return nil, errors.New("count must be an integer")
		}
		count = c
	}

	if !server.KeyExists(key) {
		return []byte("*-1\r\n\r\n"), nil
	}

	_, err := server.KeyLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a set", key)
	}

	members := set.GetRandom(count)

	res := fmt.Sprintf("*%d", len(members))
	for i, m := range members {
		res = fmt.Sprintf("%s\r\n$%d\r\n%s", res, len(m), m)
		if i == len(members)-1 {
			res += "\r\n\r\n"
		}
	}

	return []byte(res), nil
}

func handleSREM(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]
	members := cmd[2:]

	if !server.KeyExists(key) {
		return []byte(":0\r\n\r\n"), nil
	}

	_, err := server.KeyLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	count := set.Remove(members)

	return []byte(fmt.Sprintf(":%d\r\n\r\n", count)), nil
}

func handleSUNION(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 2 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range cmd[1:] {
		if !server.KeyExists(key) {
			continue
		}
		_, err := server.KeyRLock(ctx, key)
		if err != nil {
			return nil, err
		}
		locks[key] = true
	}

	var sets []*Set

	for key, locked := range locks {
		if !locked {
			continue
		}
		set, ok := server.GetValue(key).(*Set)
		if !ok {
			return nil, fmt.Errorf("value at key %s is not a set", key)
		}
		sets = append(sets, set)
	}

	union := sets[0].Union(sets[1:])

	res := fmt.Sprintf("*%d", union.Cardinality())
	for i, e := range union.GetAll() {
		res = fmt.Sprintf("%s\r\n$%d\r\n%s", res, len(e), e)
		if i == len(union.GetAll())-1 {
			res += "\r\n\r\n"
		}
	}

	return []byte(res), nil
}

func handleSUNIONSTORE(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range cmd[2:] {
		if !server.KeyExists(key) {
			continue
		}
		_, err := server.KeyRLock(ctx, key)
		if err != nil {
			return nil, err
		}
		locks[key] = true
	}

	var sets []*Set

	for key, locked := range locks {
		if !locked {
			continue
		}
		set, ok := server.GetValue(key).(*Set)
		if !ok {
			return nil, fmt.Errorf("value at key %s is not a set", key)
		}
		sets = append(sets, set)
	}

	union := sets[0].Union(sets[1:])

	destination := cmd[1]

	if server.KeyExists(destination) {
		_, err := server.KeyLock(ctx, destination)
		if err != nil {
			return nil, err
		}
	} else {
		_, err := server.CreateKeyAndLock(ctx, destination)
		if err != nil {
			return nil, err
		}
	}
	defer server.KeyUnlock(destination)

	server.SetValue(ctx, destination, union)
	return []byte(fmt.Sprintf(":%d\r\n\r\n", union.Cardinality())), nil
}

func NewModule() Plugin {
	return Plugin{
		name: "SetCommands",
		commands: []utils.Command{
			{
				Command:     "sadd",
				Categories:  []string{utils.SetCategory, utils.WriteCategory, utils.FastCategory},
				Description: "(SADD key member [member...]) Add one or more members to the set.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
				HandlerFunc: handleSADD,
			},
			{
				Command:     "scard",
				Categories:  []string{utils.SetCategory, utils.WriteCategory, utils.FastCategory},
				Description: "(SCARD key) Returns the cardinality of the set.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
				HandlerFunc: handleSCARD,
			},
			{
				Command:     "sdiff",
				Categories:  []string{utils.SetCategory, utils.ReadCategory, utils.SlowCategory},
				Description: "(SDIFF key [key...]) Returns the difference between all the sets in the given keys.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleSDIFF,
			},
			{
				Command:     "sdiffstore",
				Categories:  []string{utils.SetCategory, utils.WriteCategory, utils.SlowCategory},
				Description: "(SDIFFSTORE destination key [key...]) Stores the difference between all the sets at the destination key.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleSDIFFSTORE,
			},
			{
				Command:     "sinter",
				Categories:  []string{utils.SetCategory, utils.WriteCategory, utils.SlowCategory},
				Description: "(SINTER key [key...]) Returns the intersection of multiple sets.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleSINTER,
			},
			{
				Command:     "sintercard",
				Categories:  []string{utils.SetCategory, utils.ReadCategory, utils.SlowCategory},
				Description: "(SINTERCARD key [key...] [LIMIT limit]) Returns the cardinality of the intersection between multiple sets.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleSINTERCARD,
			},
			{
				Command:     "sinterstore",
				Categories:  []string{utils.SetCategory, utils.WriteCategory, utils.SlowCategory},
				Description: "(SINTERSTORE destination key [key...]) Stores the intersection of multiple sets at the destination key.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleSINTERSTORE,
			},
			{
				Command:     "sismember",
				Categories:  []string{utils.SetCategory, utils.ReadCategory, utils.FastCategory},
				Description: "(SISMEMBER key member) Returns if member is contained in the set.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
				HandlerFunc: handleSISMEMBER,
			},
			{
				Command:     "smembers",
				Categories:  []string{utils.SetCategory, utils.ReadCategory, utils.SlowCategory},
				Description: "(SMEMBERS key) Returns all members of a set.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
				HandlerFunc: handleSMEMBERS,
			},
			{
				Command:     "smismember",
				Categories:  []string{utils.SetCategory, utils.ReadCategory, utils.FastCategory},
				Description: "(SMISMEMBER key member [member...]) Returns if multiple members are in the set.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
				HandlerFunc: handleSMISMEMBER,
			},

			{
				Command:     "smove",
				Categories:  []string{utils.SetCategory, utils.WriteCategory, utils.FastCategory},
				Description: "(SMOVE source destination member) Moves a member from source set to destination set.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:3], nil
				},
				HandlerFunc: handleSMOVE,
			},
			{
				Command:     "spop",
				Categories:  []string{utils.SetCategory, utils.WriteCategory, utils.SlowCategory},
				Description: "(SPOP key [count]) Returns and removes one or more random members from the set.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
				HandlerFunc: handleSPOP,
			},
			{
				Command:     "srandmember",
				Categories:  []string{utils.SetCategory, utils.ReadCategory, utils.SlowCategory},
				Description: "(SRANDMEMBER key [count]) Returns one or more random members from the set without removing them.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
				HandlerFunc: handleSRANDMEMBER,
			},
			{
				Command:     "srem",
				Categories:  []string{utils.SetCategory, utils.WriteCategory, utils.FastCategory},
				Description: "(SREM key member [member...]) Remove one or more members from a set.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
				HandlerFunc: handleSREM,
			},
			{
				Command:     "sunion",
				Categories:  []string{utils.SetCategory, utils.ReadCategory, utils.SlowCategory},
				Description: "(SUNION key [key...]) Returns the members of the set resulting from the union of the provided sets.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleSUNION,
			},
			{
				Command:     "sunionstore",
				Categories:  []string{utils.SetCategory, utils.WriteCategory, utils.SlowCategory},
				Description: "(SUNIONSTORE destination key [key...]) Stores the union of the given sets into destination.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: handleSUNIONSTORE,
			},
		},
		description: "Handle commands for sets",
	}
}
