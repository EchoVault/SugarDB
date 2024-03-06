package set

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/src/utils"
	"net"
	"slices"
	"strings"
)

func handleSADD(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := saddKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	var set *Set

	if !server.KeyExists(key) {
		set = NewSet(cmd[2:])
		if ok, err := server.CreateKeyAndLock(ctx, key); !ok && err != nil {
			return nil, err
		}
		if err = server.SetValue(ctx, key, set); err != nil {
			return nil, err
		}
		server.KeyUnlock(key)
		return []byte(fmt.Sprintf(":%d\r\n", len(cmd[2:]))), nil
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	count := set.Add(cmd[2:])

	return []byte(fmt.Sprintf(":%d\r\n", count)), nil
}

func handleSCARD(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := scardKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	if !server.KeyExists(key) {
		return []byte(fmt.Sprintf(":0\r\n")), nil
	}

	if _, err = server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	cardinality := set.Cardinality()

	return []byte(fmt.Sprintf(":%d\r\n", cardinality)), nil
}

func handleSDIFF(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := sdiffKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	// Extract base set first
	if !server.KeyExists(keys[0]) {
		return nil, fmt.Errorf("key for base set \"%s\" does not exist", keys[0])
	}
	if _, err = server.KeyRLock(ctx, keys[0]); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(keys[0])
	baseSet, ok := server.GetValue(keys[0]).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", keys[0])
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range keys[1:] {
		if !server.KeyExists(key) {
			continue
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
			continue
		}
		locks[key] = true
	}

	var sets []*Set
	for _, key := range cmd[2:] {
		set, ok := server.GetValue(key).(*Set)
		if !ok {
			continue
		}
		sets = append(sets, set)
	}

	diff := baseSet.Subtract(sets)
	elems := diff.GetAll()

	res := fmt.Sprintf("*%d", len(elems))
	for i, e := range elems {
		res = fmt.Sprintf("%s\r\n$%d\r\n%s", res, len(e), e)
		if i == len(elems)-1 {
			res += "\r\n"
		}
	}

	return []byte(res), nil
}

func handleSDIFFSTORE(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := sdiffstoreKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	destination := keys[0]

	// Extract base set first
	if !server.KeyExists(keys[1]) {
		return nil, fmt.Errorf("key for base set \"%s\" does not exist", keys[1])
	}
	if _, err := server.KeyRLock(ctx, keys[1]); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(keys[1])
	baseSet, ok := server.GetValue(keys[1]).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", keys[1])
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range keys[2:] {
		if !server.KeyExists(key) {
			continue
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
			continue
		}
		locks[key] = true
	}

	var sets []*Set
	for _, key := range keys[2:] {
		set, ok := server.GetValue(key).(*Set)
		if !ok {
			continue
		}
		sets = append(sets, set)
	}

	diff := baseSet.Subtract(sets)
	elems := diff.GetAll()

	res := fmt.Sprintf(":%d\r\n", len(elems))

	if server.KeyExists(destination) {
		if _, err = server.KeyLock(ctx, destination); err != nil {
			return nil, err
		}
		if err = server.SetValue(ctx, destination, diff); err != nil {
			return nil, err
		}
		server.KeyUnlock(destination)
		return []byte(res), nil
	}

	if _, err = server.CreateKeyAndLock(ctx, destination); err != nil {
		return nil, err
	}
	if err = server.SetValue(ctx, destination, diff); err != nil {
		return nil, err
	}
	server.KeyUnlock(destination)

	return []byte(res), nil
}

func handleSINTER(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := sinterKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range keys[0:] {
		if !server.KeyExists(key) {
			// If key does not exist, then there is no intersection
			return []byte("*0\r\n"), nil
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
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

	intersect, _ := Intersection(0, sets...)
	elems := intersect.GetAll()

	res := fmt.Sprintf("*%d", len(elems))
	for i, e := range elems {
		res = fmt.Sprintf("%s\r\n$%d\r\n%s", res, len(e), e)
		if i == len(elems)-1 {
			res += "\r\n"
		}
	}

	return []byte(res), nil
}

func handleSINTERCARD(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := sintercardKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	// Extract the limit from the command
	var limit int
	limitIdx := slices.IndexFunc(cmd, func(s string) bool {
		return strings.EqualFold(s, "limit")
	})
	if limitIdx >= 0 && limitIdx < 2 {
		return nil, errors.New(utils.WrongArgsResponse)
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

	for _, key := range keys {
		if !server.KeyExists(key) {
			// If key does not exist, then there is no intersection
			return []byte(":0\r\n"), nil
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
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

	intersect, _ := Intersection(limit, sets...)

	return []byte(fmt.Sprintf(":%d\r\n", intersect.Cardinality())), nil
}

func handleSINTERSTORE(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := sinterstoreKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range keys[1:] {
		if !server.KeyExists(key) {
			// If key does not exist, then there is no intersection
			return []byte(":0\r\n"), nil
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
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

	intersect, _ := Intersection(0, sets...)
	destination := keys[0]

	if server.KeyExists(destination) {
		if _, err = server.KeyLock(ctx, destination); err != nil {
			return nil, err
		}
	} else {
		if _, err = server.CreateKeyAndLock(ctx, destination); err != nil {
			return nil, err
		}
	}

	if err = server.SetValue(ctx, destination, intersect); err != nil {
		return nil, err
	}
	server.KeyUnlock(destination)

	return []byte(fmt.Sprintf(":%d\r\n", intersect.Cardinality())), nil
}

func handleSISMEMBER(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := sismemberKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	if !server.KeyExists(key) {
		return []byte(":0\r\n"), nil
	}

	if _, err = server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	if !set.Contains(cmd[2]) {
		return []byte(":0\r\n"), nil
	}

	return []byte(":1\r\n"), nil
}

func handleSMEMBERS(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := smembersKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	if !server.KeyExists(key) {
		return []byte("*0\r\n"), nil
	}

	if _, err = server.KeyRLock(ctx, key); err != nil {
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
			res += "\r\n"
		}
	}

	return []byte(res), nil
}

func handleSMISMEMBER(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := smismemberKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]
	members := cmd[2:]

	if !server.KeyExists(key) {
		res := fmt.Sprintf("*%d", len(members))
		for i, _ := range members {
			res = fmt.Sprintf("%s\r\n:0", res)
			if i == len(members)-1 {
				res += "\r\n"
			}
		}
		return []byte(res), nil
	}

	if _, err = server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	res := fmt.Sprintf("*%d", len(members))
	for i := 0; i < len(members); i++ {
		if set.Contains(members[i]) {
			res += "\r\n:1"
		} else {
			res += "\r\n:0"
		}
	}
	res += "\r\n"

	return []byte(res), nil
}

func handleSMOVE(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := smoveKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	source := keys[0]
	destination := keys[1]
	member := cmd[3]

	if !server.KeyExists(source) {
		return []byte(":0\r\n"), nil
	}

	if _, err = server.KeyLock(ctx, source); err != nil {
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
		if _, err = server.CreateKeyAndLock(ctx, destination); err != nil {
			return nil, err
		}
		defer server.KeyUnlock(destination)
		destinationSet = NewSet([]string{})
		if err = server.SetValue(ctx, destination, destinationSet); err != nil {
			return nil, err
		}
	} else {
		// Destination key exists
		if _, err := server.KeyLock(ctx, destination); err != nil {
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

	return []byte(fmt.Sprintf(":%d\r\n", res)), nil
}

func handleSPOP(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := spopKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]
	count := 1

	if len(cmd) == 3 {
		c, ok := utils.AdaptType(cmd[2]).(int)
		if !ok {
			return nil, errors.New("count must be an integer")
		}
		count = c
	}

	if !server.KeyExists(key) {
		return []byte("*-1\r\n"), nil
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
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
			res += "\r\n"
		}
	}

	return []byte(res), nil
}

func handleSRANDMEMBER(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := srandmemberKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]
	count := 1

	if len(cmd) == 3 {
		c, ok := utils.AdaptType(cmd[2]).(int)
		if !ok {
			return nil, errors.New("count must be an integer")
		}
		count = c
	}

	if !server.KeyExists(key) {
		return []byte("*-1\r\n"), nil
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
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
			res += "\r\n"
		}
	}

	return []byte(res), nil
}

func handleSREM(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := sremKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]
	members := cmd[2:]

	if !server.KeyExists(key) {
		return []byte(":0\r\n"), nil
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	set, ok := server.GetValue(key).(*Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	count := set.Remove(members)

	return []byte(fmt.Sprintf(":%d\r\n", count)), nil
}

func handleSUNION(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := sunionKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range keys {
		if !server.KeyExists(key) {
			continue
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
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

	union := Union(sets...)

	res := fmt.Sprintf("*%d", union.Cardinality())
	for i, e := range union.GetAll() {
		res = fmt.Sprintf("%s\r\n$%d\r\n%s", res, len(e), e)
		if i == len(union.GetAll())-1 {
			res += "\r\n"
		}
	}

	return []byte(res), nil
}

func handleSUNIONSTORE(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	keys, err := sunionstoreKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(key)
			}
		}
	}()

	for _, key := range keys[1:] {
		if !server.KeyExists(key) {
			continue
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
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

	union := Union(sets...)

	destination := cmd[1]

	if server.KeyExists(destination) {
		if _, err = server.KeyLock(ctx, destination); err != nil {
			return nil, err
		}
	} else {
		if _, err = server.CreateKeyAndLock(ctx, destination); err != nil {
			return nil, err
		}
	}
	defer server.KeyUnlock(destination)

	if err = server.SetValue(ctx, destination, union); err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf(":%d\r\n", union.Cardinality())), nil
}

func Commands() []utils.Command {
	return []utils.Command{
		{
			Command:           "sadd",
			Categories:        []string{utils.SetCategory, utils.WriteCategory, utils.FastCategory},
			Description:       "(SADD key member [member...]) Add one or more members to the set. If the set does not exist, it's created.",
			Sync:              true,
			KeyExtractionFunc: saddKeyFunc,
			HandlerFunc:       handleSADD,
		},
		{
			Command:           "scard",
			Categories:        []string{utils.SetCategory, utils.WriteCategory, utils.FastCategory},
			Description:       "(SCARD key) Returns the cardinality of the set.",
			Sync:              false,
			KeyExtractionFunc: scardKeyFunc,
			HandlerFunc:       handleSCARD,
		},
		{
			Command:    "sdiff",
			Categories: []string{utils.SetCategory, utils.ReadCategory, utils.SlowCategory},
			Description: `(SDIFF key [key...]) Returns the difference between all the sets in the given keys.
If the first key provided is the only valid set, then this key's set will be returned as the result.
All keys that are non-existed or hold values that are not sets will be skipped.`,
			Sync:              false,
			KeyExtractionFunc: sdiffKeyFunc,
			HandlerFunc:       handleSDIFF,
		},
		{
			Command:    "sdiffstore",
			Categories: []string{utils.SetCategory, utils.WriteCategory, utils.SlowCategory},
			Description: `(SDIFFSTORE destination key [key...]) Works the same as SDIFF but also stores the result at 'destination'.
Returns the cardinality of the new set`,
			Sync:              true,
			KeyExtractionFunc: sdiffstoreKeyFunc,
			HandlerFunc:       handleSDIFFSTORE,
		},
		{
			Command:           "sinter",
			Categories:        []string{utils.SetCategory, utils.WriteCategory, utils.SlowCategory},
			Description:       "(SINTER key [key...]) Returns the intersection of multiple sets.",
			Sync:              false,
			KeyExtractionFunc: sinterKeyFunc,
			HandlerFunc:       handleSINTER,
		},
		{
			Command:           "sintercard",
			Categories:        []string{utils.SetCategory, utils.ReadCategory, utils.SlowCategory},
			Description:       "(SINTERCARD key [key...] [LIMIT limit]) Returns the cardinality of the intersection between multiple sets.",
			Sync:              false,
			KeyExtractionFunc: sintercardKeyFunc,
			HandlerFunc:       handleSINTERCARD,
		},
		{
			Command:           "sinterstore",
			Categories:        []string{utils.SetCategory, utils.WriteCategory, utils.SlowCategory},
			Description:       "(SINTERSTORE destination key [key...]) Stores the intersection of multiple sets at the destination key.",
			Sync:              true,
			KeyExtractionFunc: sinterstoreKeyFunc,
			HandlerFunc:       handleSINTERSTORE,
		},
		{
			Command:           "sismember",
			Categories:        []string{utils.SetCategory, utils.ReadCategory, utils.FastCategory},
			Description:       "(SISMEMBER key member) Returns if member is contained in the set.",
			Sync:              false,
			KeyExtractionFunc: sismemberKeyFunc,
			HandlerFunc:       handleSISMEMBER,
		},
		{
			Command:           "smembers",
			Categories:        []string{utils.SetCategory, utils.ReadCategory, utils.SlowCategory},
			Description:       "(SMEMBERS key) Returns all members of a set.",
			Sync:              false,
			KeyExtractionFunc: smembersKeyFunc,
			HandlerFunc:       handleSMEMBERS,
		},
		{
			Command:           "smismember",
			Categories:        []string{utils.SetCategory, utils.ReadCategory, utils.FastCategory},
			Description:       "(SMISMEMBER key member [member...]) Returns if multiple members are in the set.",
			Sync:              false,
			KeyExtractionFunc: smismemberKeyFunc,
			HandlerFunc:       handleSMISMEMBER,
		},

		{
			Command:           "smove",
			Categories:        []string{utils.SetCategory, utils.WriteCategory, utils.FastCategory},
			Description:       "(SMOVE source destination member) Moves a member from source set to destination set.",
			Sync:              true,
			KeyExtractionFunc: smoveKeyFunc,
			HandlerFunc:       handleSMOVE,
		},
		{
			Command:           "spop",
			Categories:        []string{utils.SetCategory, utils.WriteCategory, utils.SlowCategory},
			Description:       "(SPOP key [count]) Returns and removes one or more random members from the set.",
			Sync:              true,
			KeyExtractionFunc: spopKeyFunc,
			HandlerFunc:       handleSPOP,
		},
		{
			Command:           "srandmember",
			Categories:        []string{utils.SetCategory, utils.ReadCategory, utils.SlowCategory},
			Description:       "(SRANDMEMBER key [count]) Returns one or more random members from the set without removing them.",
			Sync:              false,
			KeyExtractionFunc: srandmemberKeyFunc,
			HandlerFunc:       handleSRANDMEMBER,
		},
		{
			Command:           "srem",
			Categories:        []string{utils.SetCategory, utils.WriteCategory, utils.FastCategory},
			Description:       "(SREM key member [member...]) Remove one or more members from a set.",
			Sync:              true,
			KeyExtractionFunc: sremKeyFunc,
			HandlerFunc:       handleSREM,
		},
		{
			Command:           "sunion",
			Categories:        []string{utils.SetCategory, utils.ReadCategory, utils.SlowCategory},
			Description:       "(SUNION key [key...]) Returns the members of the set resulting from the union of the provided sets.",
			Sync:              false,
			KeyExtractionFunc: sunionKeyFunc,
			HandlerFunc:       handleSUNION,
		},
		{
			Command:           "sunionstore",
			Categories:        []string{utils.SetCategory, utils.WriteCategory, utils.SlowCategory},
			Description:       "(SUNIONSTORE destination key [key...]) Stores the union of the given sets into destination.",
			Sync:              true,
			KeyExtractionFunc: sunionstoreKeyFunc,
			HandlerFunc:       handleSUNIONSTORE,
		},
	}
}
