// Copyright 2024 Kelvin Clement Mwinuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package set

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal"
	internal_set "github.com/echovault/echovault/internal/set"
	"github.com/echovault/echovault/pkg/constants"
	"github.com/echovault/echovault/pkg/types"
	"net"
	"slices"
	"strings"
)

func handleSADD(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := saddKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]

	var set *internal_set.Set

	if !server.KeyExists(ctx, key) {
		set = internal_set.NewSet(cmd[2:])
		if ok, err := server.CreateKeyAndLock(ctx, key); !ok && err != nil {
			return nil, err
		}
		if err = server.SetValue(ctx, key, set); err != nil {
			return nil, err
		}
		server.KeyUnlock(ctx, key)
		return []byte(fmt.Sprintf(":%d\r\n", len(cmd[2:]))), nil
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, key)

	set, ok := server.GetValue(ctx, key).(*internal_set.Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	count := set.Add(cmd[2:])

	return []byte(fmt.Sprintf(":%d\r\n", count)), nil
}

func handleSCARD(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := scardKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]

	if !server.KeyExists(ctx, key) {
		return []byte(fmt.Sprintf(":0\r\n")), nil
	}

	if _, err = server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(ctx, key)

	set, ok := server.GetValue(ctx, key).(*internal_set.Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	cardinality := set.Cardinality()

	return []byte(fmt.Sprintf(":%d\r\n", cardinality)), nil
}

func handleSDIFF(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := sdiffKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	// Extract base set first
	if !server.KeyExists(ctx, keys.ReadKeys[0]) {
		return nil, fmt.Errorf("key for base set \"%s\" does not exist", keys.ReadKeys[0])
	}
	if _, err = server.KeyRLock(ctx, keys.ReadKeys[0]); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(ctx, keys.ReadKeys[0])
	baseSet, ok := server.GetValue(ctx, keys.ReadKeys[0]).(*internal_set.Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", keys.ReadKeys[0])
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(ctx, key)
			}
		}
	}()

	for _, key := range keys.ReadKeys[1:] {
		if !server.KeyExists(ctx, key) {
			continue
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
			continue
		}
		locks[key] = true
	}

	var sets []*internal_set.Set
	for _, key := range cmd[2:] {
		set, ok := server.GetValue(ctx, key).(*internal_set.Set)
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

func handleSDIFFSTORE(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := sdiffstoreKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	destination := keys.WriteKeys[0]

	// Extract base set first
	if !server.KeyExists(ctx, keys.ReadKeys[0]) {
		return nil, fmt.Errorf("key for base set \"%s\" does not exist", keys.ReadKeys[0])
	}
	if _, err := server.KeyRLock(ctx, keys.ReadKeys[0]); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(ctx, keys.ReadKeys[0])
	baseSet, ok := server.GetValue(ctx, keys.ReadKeys[0]).(*internal_set.Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", keys.ReadKeys[0])
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(ctx, key)
			}
		}
	}()

	for _, key := range keys.ReadKeys[1:] {
		if !server.KeyExists(ctx, key) {
			continue
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
			continue
		}
		locks[key] = true
	}

	var sets []*internal_set.Set
	for _, key := range keys.ReadKeys[1:] {
		set, ok := server.GetValue(ctx, key).(*internal_set.Set)
		if !ok {
			continue
		}
		sets = append(sets, set)
	}

	diff := baseSet.Subtract(sets)
	elems := diff.GetAll()

	res := fmt.Sprintf(":%d\r\n", len(elems))

	if server.KeyExists(ctx, destination) {
		if _, err = server.KeyLock(ctx, destination); err != nil {
			return nil, err
		}
		if err = server.SetValue(ctx, destination, diff); err != nil {
			return nil, err
		}
		server.KeyUnlock(ctx, destination)
		return []byte(res), nil
	}

	if _, err = server.CreateKeyAndLock(ctx, destination); err != nil {
		return nil, err
	}
	if err = server.SetValue(ctx, destination, diff); err != nil {
		return nil, err
	}
	server.KeyUnlock(ctx, destination)

	return []byte(res), nil
}

func handleSINTER(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := sinterKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(ctx, key)
			}
		}
	}()

	for _, key := range keys.ReadKeys {
		if !server.KeyExists(ctx, key) {
			// If key does not exist, then there is no intersection
			return []byte("*0\r\n"), nil
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
			return nil, err
		}
		locks[key] = true
	}

	var sets []*internal_set.Set

	for key, _ := range locks {
		set, ok := server.GetValue(ctx, key).(*internal_set.Set)
		if !ok {
			// If the value at the key is not a set, return error
			return nil, fmt.Errorf("value at key %s is not a set", key)
		}
		sets = append(sets, set)
	}

	if len(sets) <= 0 {
		return nil, fmt.Errorf("not enough sets in the keys provided")
	}

	intersect, _ := internal_set.Intersection(0, sets...)
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

func handleSINTERCARD(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
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
		return nil, errors.New(constants.WrongArgsResponse)
	}
	if limitIdx != -1 {
		limitIdx += 1
		if limitIdx >= len(cmd) {
			return nil, errors.New("provide limit after LIMIT keyword")
		}

		if l, ok := internal.AdaptType(cmd[limitIdx]).(int); !ok {
			return nil, errors.New("limit must be an integer")
		} else {
			limit = l
		}
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(ctx, key)
			}
		}
	}()

	for _, key := range keys.ReadKeys {
		if !server.KeyExists(ctx, key) {
			// If key does not exist, then there is no intersection
			return []byte(":0\r\n"), nil
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
			return nil, err
		}
		locks[key] = true
	}

	var sets []*internal_set.Set

	for key, _ := range locks {
		set, ok := server.GetValue(ctx, key).(*internal_set.Set)
		if !ok {
			// If the value at the key is not a set, return error
			return nil, fmt.Errorf("value at key %s is not a set", key)
		}
		sets = append(sets, set)
	}

	if len(sets) <= 0 {
		return nil, fmt.Errorf("not enough sets in the keys provided")
	}

	intersect, _ := internal_set.Intersection(limit, sets...)

	return []byte(fmt.Sprintf(":%d\r\n", intersect.Cardinality())), nil
}

func handleSINTERSTORE(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := sinterstoreKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(ctx, key)
			}
		}
	}()

	for _, key := range keys.ReadKeys {
		if !server.KeyExists(ctx, key) {
			// If key does not exist, then there is no intersection
			return []byte(":0\r\n"), nil
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
			return nil, err
		}
		locks[key] = true
	}

	var sets []*internal_set.Set

	for key, _ := range locks {
		set, ok := server.GetValue(ctx, key).(*internal_set.Set)
		if !ok {
			// If the value at the key is not a set, return error
			return nil, fmt.Errorf("value at key %s is not a set", key)
		}
		sets = append(sets, set)
	}

	intersect, _ := internal_set.Intersection(0, sets...)
	destination := keys.WriteKeys[0]

	if server.KeyExists(ctx, destination) {
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
	server.KeyUnlock(ctx, destination)

	return []byte(fmt.Sprintf(":%d\r\n", intersect.Cardinality())), nil
}

func handleSISMEMBER(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := sismemberKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]

	if !server.KeyExists(ctx, key) {
		return []byte(":0\r\n"), nil
	}

	if _, err = server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(ctx, key)

	set, ok := server.GetValue(ctx, key).(*internal_set.Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	if !set.Contains(cmd[2]) {
		return []byte(":0\r\n"), nil
	}

	return []byte(":1\r\n"), nil
}

func handleSMEMBERS(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := smembersKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]

	if !server.KeyExists(ctx, key) {
		return []byte("*0\r\n"), nil
	}

	if _, err = server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(ctx, key)

	set, ok := server.GetValue(ctx, key).(*internal_set.Set)
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

func handleSMISMEMBER(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := smismemberKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	members := cmd[2:]

	if !server.KeyExists(ctx, key) {
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
	defer server.KeyRUnlock(ctx, key)

	set, ok := server.GetValue(ctx, key).(*internal_set.Set)
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

func handleSMOVE(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := smoveKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	source, destination := keys.WriteKeys[0], keys.WriteKeys[1]
	member := cmd[3]

	if !server.KeyExists(ctx, source) {
		return []byte(":0\r\n"), nil
	}

	if _, err = server.KeyLock(ctx, source); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, source)

	sourceSet, ok := server.GetValue(ctx, source).(*internal_set.Set)
	if !ok {
		return nil, errors.New("source is not a set")
	}

	var destinationSet *internal_set.Set

	if !server.KeyExists(ctx, destination) {
		// Destination key does not exist
		if _, err = server.CreateKeyAndLock(ctx, destination); err != nil {
			return nil, err
		}
		defer server.KeyUnlock(ctx, destination)
		destinationSet = internal_set.NewSet([]string{})
		if err = server.SetValue(ctx, destination, destinationSet); err != nil {
			return nil, err
		}
	} else {
		// Destination key exists
		if _, err := server.KeyLock(ctx, destination); err != nil {
			return nil, err
		}
		defer server.KeyUnlock(ctx, destination)
		ds, ok := server.GetValue(ctx, destination).(*internal_set.Set)
		if !ok {
			return nil, errors.New("destination is not a set")
		}
		destinationSet = ds
	}

	res := sourceSet.Move(destinationSet, member)

	return []byte(fmt.Sprintf(":%d\r\n", res)), nil
}

func handleSPOP(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := spopKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]
	count := 1

	if len(cmd) == 3 {
		c, ok := internal.AdaptType(cmd[2]).(int)
		if !ok {
			return nil, errors.New("count must be an integer")
		}
		count = c
	}

	if !server.KeyExists(ctx, key) {
		return []byte("*-1\r\n"), nil
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, key)

	set, ok := server.GetValue(ctx, key).(*internal_set.Set)
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

func handleSRANDMEMBER(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := srandmemberKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	count := 1

	if len(cmd) == 3 {
		c, ok := internal.AdaptType(cmd[2]).(int)
		if !ok {
			return nil, errors.New("count must be an integer")
		}
		count = c
	}

	if !server.KeyExists(ctx, key) {
		return []byte("*-1\r\n"), nil
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, key)

	set, ok := server.GetValue(ctx, key).(*internal_set.Set)
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

func handleSREM(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := sremKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]
	members := cmd[2:]

	if !server.KeyExists(ctx, key) {
		return []byte(":0\r\n"), nil
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, key)

	set, ok := server.GetValue(ctx, key).(*internal_set.Set)
	if !ok {
		return nil, fmt.Errorf("value at key %s is not a set", key)
	}

	count := set.Remove(members)

	return []byte(fmt.Sprintf(":%d\r\n", count)), nil
}

func handleSUNION(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := sunionKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(ctx, key)
			}
		}
	}()

	for _, key := range keys.ReadKeys {
		if !server.KeyExists(ctx, key) {
			continue
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
			return nil, err
		}
		locks[key] = true
	}

	var sets []*internal_set.Set

	for key, locked := range locks {
		if !locked {
			continue
		}
		set, ok := server.GetValue(ctx, key).(*internal_set.Set)
		if !ok {
			return nil, fmt.Errorf("value at key %s is not a set", key)
		}
		sets = append(sets, set)
	}

	union := internal_set.Union(sets...)

	res := fmt.Sprintf("*%d", union.Cardinality())
	for i, e := range union.GetAll() {
		res = fmt.Sprintf("%s\r\n$%d\r\n%s", res, len(e), e)
		if i == len(union.GetAll())-1 {
			res += "\r\n"
		}
	}

	return []byte(res), nil
}

func handleSUNIONSTORE(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := sunionstoreKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	locks := make(map[string]bool)
	defer func() {
		for key, locked := range locks {
			if locked {
				server.KeyRUnlock(ctx, key)
			}
		}
	}()

	for _, key := range keys.ReadKeys {
		if !server.KeyExists(ctx, key) {
			continue
		}
		if _, err = server.KeyRLock(ctx, key); err != nil {
			return nil, err
		}
		locks[key] = true
	}

	var sets []*internal_set.Set

	for key, locked := range locks {
		if !locked {
			continue
		}
		set, ok := server.GetValue(ctx, key).(*internal_set.Set)
		if !ok {
			return nil, fmt.Errorf("value at key %s is not a set", key)
		}
		sets = append(sets, set)
	}

	union := internal_set.Union(sets...)

	destination := keys.WriteKeys[0]

	if server.KeyExists(ctx, destination) {
		if _, err = server.KeyLock(ctx, destination); err != nil {
			return nil, err
		}
	} else {
		if _, err = server.CreateKeyAndLock(ctx, destination); err != nil {
			return nil, err
		}
	}
	defer server.KeyUnlock(ctx, destination)

	if err = server.SetValue(ctx, destination, union); err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf(":%d\r\n", union.Cardinality())), nil
}

func Commands() []types.Command {
	return []types.Command{
		{
			Command:           "sadd",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.WriteCategory, constants.FastCategory},
			Description:       "(SADD key member [member...]) Add one or more members to the set. If the set does not exist, it's created.",
			Sync:              true,
			KeyExtractionFunc: saddKeyFunc,
			HandlerFunc:       handleSADD,
		},
		{
			Command:           "scard",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.WriteCategory, constants.FastCategory},
			Description:       "(SCARD key) Returns the cardinality of the set.",
			Sync:              false,
			KeyExtractionFunc: scardKeyFunc,
			HandlerFunc:       handleSCARD,
		},
		{
			Command:    "sdiff",
			Module:     constants.SetModule,
			Categories: []string{constants.SetCategory, constants.ReadCategory, constants.SlowCategory},
			Description: `(SDIFF key [key...]) Returns the difference between all the sets in the given keys.
If the first key provided is the only valid set, then this key's set will be returned as the result.
All keys that are non-existed or hold values that are not sets will be skipped.`,
			Sync:              false,
			KeyExtractionFunc: sdiffKeyFunc,
			HandlerFunc:       handleSDIFF,
		},
		{
			Command:    "sdiffstore",
			Module:     constants.SetModule,
			Categories: []string{constants.SetCategory, constants.WriteCategory, constants.SlowCategory},
			Description: `(SDIFFSTORE destination key [key...]) Works the same as SDIFF but also stores the result at 'destination'.
Returns the cardinality of the new set`,
			Sync:              true,
			KeyExtractionFunc: sdiffstoreKeyFunc,
			HandlerFunc:       handleSDIFFSTORE,
		},
		{
			Command:           "sinter",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.WriteCategory, constants.SlowCategory},
			Description:       "(SINTER key [key...]) Returns the intersection of multiple sets.",
			Sync:              false,
			KeyExtractionFunc: sinterKeyFunc,
			HandlerFunc:       handleSINTER,
		},
		{
			Command:           "sintercard",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.ReadCategory, constants.SlowCategory},
			Description:       "(SINTERCARD key [key...] [LIMIT limit]) Returns the cardinality of the intersection between multiple sets.",
			Sync:              false,
			KeyExtractionFunc: sintercardKeyFunc,
			HandlerFunc:       handleSINTERCARD,
		},
		{
			Command:           "sinterstore",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.WriteCategory, constants.SlowCategory},
			Description:       "(SINTERSTORE destination key [key...]) Stores the intersection of multiple sets at the destination key.",
			Sync:              true,
			KeyExtractionFunc: sinterstoreKeyFunc,
			HandlerFunc:       handleSINTERSTORE,
		},
		{
			Command:           "sismember",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.ReadCategory, constants.FastCategory},
			Description:       "(SISMEMBER key member) Returns if member is contained in the set.",
			Sync:              false,
			KeyExtractionFunc: sismemberKeyFunc,
			HandlerFunc:       handleSISMEMBER,
		},
		{
			Command:           "smembers",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.ReadCategory, constants.SlowCategory},
			Description:       "(SMEMBERS key) Returns all members of a set.",
			Sync:              false,
			KeyExtractionFunc: smembersKeyFunc,
			HandlerFunc:       handleSMEMBERS,
		},
		{
			Command:           "smismember",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.ReadCategory, constants.FastCategory},
			Description:       "(SMISMEMBER key member [member...]) Returns if multiple members are in the set.",
			Sync:              false,
			KeyExtractionFunc: smismemberKeyFunc,
			HandlerFunc:       handleSMISMEMBER,
		},

		{
			Command:           "smove",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.WriteCategory, constants.FastCategory},
			Description:       "(SMOVE source destination member) Moves a member from source set to destination set.",
			Sync:              true,
			KeyExtractionFunc: smoveKeyFunc,
			HandlerFunc:       handleSMOVE,
		},
		{
			Command:           "spop",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.WriteCategory, constants.SlowCategory},
			Description:       "(SPOP key [count]) Returns and removes one or more random members from the set.",
			Sync:              true,
			KeyExtractionFunc: spopKeyFunc,
			HandlerFunc:       handleSPOP,
		},
		{
			Command:           "srandmember",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.ReadCategory, constants.SlowCategory},
			Description:       "(SRANDMEMBER key [count]) Returns one or more random members from the set without removing them.",
			Sync:              false,
			KeyExtractionFunc: srandmemberKeyFunc,
			HandlerFunc:       handleSRANDMEMBER,
		},
		{
			Command:           "srem",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.WriteCategory, constants.FastCategory},
			Description:       "(SREM key member [member...]) Remove one or more members from a set.",
			Sync:              true,
			KeyExtractionFunc: sremKeyFunc,
			HandlerFunc:       handleSREM,
		},
		{
			Command:           "sunion",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.ReadCategory, constants.SlowCategory},
			Description:       "(SUNION key [key...]) Returns the members of the set resulting from the union of the provided sets.",
			Sync:              false,
			KeyExtractionFunc: sunionKeyFunc,
			HandlerFunc:       handleSUNION,
		},
		{
			Command:           "sunionstore",
			Module:            constants.SetModule,
			Categories:        []string{constants.SetCategory, constants.WriteCategory, constants.SlowCategory},
			Description:       "(SUNIONSTORE destination key [key...]) Stores the union of the given sets into destination.",
			Sync:              true,
			KeyExtractionFunc: sunionstoreKeyFunc,
			HandlerFunc:       handleSUNIONSTORE,
		},
	}
}
