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

package list

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/pkg/utils"
	"math"
	"net"
	"slices"
	"strings"
)

func handleLLen(ctx context.Context, cmd []string, server utils.EchoVault, _ *net.Conn) ([]byte, error) {
	keys, err := llenKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	if !server.KeyExists(ctx, key) {
		// If key does not exist, return 0
		return []byte(":0\r\n"), nil
	}

	if _, err = server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(ctx, key)

	if list, ok := server.GetValue(ctx, key).([]interface{}); ok {
		return []byte(fmt.Sprintf(":%d\r\n", len(list))), nil
	}

	return nil, errors.New("LLEN command on non-list item")
}

func handleLIndex(ctx context.Context, cmd []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	keys, err := lindexKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]
	index, ok := utils.AdaptType(cmd[2]).(int)

	if !ok {
		return nil, errors.New("index must be an integer")
	}

	if !server.KeyExists(ctx, key) {
		return nil, errors.New("LINDEX command on non-list item")
	}

	if _, err = server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	list, ok := server.GetValue(ctx, key).([]interface{})
	server.KeyRUnlock(ctx, key)

	if !ok {
		return nil, errors.New("LINDEX command on non-list item")
	}

	if !(index >= 0 && index < len(list)) {
		return nil, errors.New("index must be within list range")
	}

	return []byte(fmt.Sprintf("+%s\r\n", list[index])), nil
}

func handleLRange(ctx context.Context, cmd []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	keys, err := lrangeKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]
	start, startOk := utils.AdaptType(cmd[2]).(int)
	end, endOk := utils.AdaptType(cmd[3]).(int)

	if !startOk || !endOk {
		return nil, errors.New("start and end indices must be integers")
	}

	if !server.KeyExists(ctx, key) {
		return nil, errors.New("LRANGE command on non-list item")
	}

	if _, err = server.KeyRLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(ctx, key)

	list, ok := server.GetValue(ctx, key).([]interface{})
	if !ok {
		return nil, errors.New("LRANGE command on non-list item")
	}

	// Make sure start is within range
	if !(start >= 0 && start < len(list)) {
		return nil, errors.New("start index must be within list boundary")
	}

	// Make sure end is within range, or is -1 otherwise
	if !((end >= 0 && end < len(list)) || end == -1) {
		return nil, errors.New("end index must be within list range or -1")
	}

	var bytes []byte

	// If end is -1, read list from start to the end of the list
	if end == -1 {
		bytes = []byte("*" + fmt.Sprint(len(list)-int(start)) + "\r\n")
		for i := int(start); i < len(list); i++ {
			str := fmt.Sprintf("%v", list[i])
			bytes = append(bytes, []byte("$"+fmt.Sprint(len(str))+"\r\n"+str+"\r\n")...)
		}
		return bytes, nil
	}

	// Make sure start and end are not equal to each other
	if start == end {
		return nil, errors.New("start and end indices cannot be equal")
	}

	// If end is not -1:
	//	1) If end is larger than start, return slice from start -> end
	//	2) If end is smaller than start, return slice from end -> start
	bytes = []byte("*" + fmt.Sprint(int(math.Abs(float64(start-end)))+1) + "\r\n")

	i := start
	j := end + 1
	if start > end {
		j = end - 1
	}

	for i != j {
		str := fmt.Sprintf("%v", list[i])
		bytes = append(bytes, []byte("$"+fmt.Sprint(len(str))+"\r\n"+str+"\r\n")...)
		if start < end {
			i++
		} else {
			i--
		}
	}

	return bytes, nil
}

func handleLSet(ctx context.Context, cmd []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	keys, err := lsetKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	index, ok := utils.AdaptType(cmd[2]).(int)
	if !ok {
		return nil, errors.New("index must be an integer")
	}

	if !server.KeyExists(ctx, key) {
		return nil, errors.New("LSET command on non-list item")
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, key)

	list, ok := server.GetValue(ctx, key).([]interface{})
	if !ok {
		return nil, errors.New("LSET command on non-list item")
	}

	if !(index >= 0 && index < len(list)) {
		return nil, errors.New("index must be within list range")
	}

	list[index] = utils.AdaptType(cmd[3])
	if err = server.SetValue(ctx, key, list); err != nil {
		return nil, err
	}

	return []byte(utils.OkResponse), nil
}

func handleLTrim(ctx context.Context, cmd []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	keys, err := ltrimKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]
	start, startOk := utils.AdaptType(cmd[2]).(int)
	end, endOk := utils.AdaptType(cmd[3]).(int)

	if !startOk || !endOk {
		return nil, errors.New("start and end indices must be integers")
	}

	if end < start && end != -1 {
		return nil, errors.New("end index must be greater than start index or -1")
	}

	if !server.KeyExists(ctx, key) {
		return nil, errors.New("LTRIM command on non-list item")
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, key)

	list, ok := server.GetValue(ctx, key).([]interface{})
	if !ok {
		return nil, errors.New("LTRIM command on non-list item")
	}

	if !(start >= 0 && start < len(list)) {
		return nil, errors.New("start index must be within list boundary")
	}

	if end == -1 || end > len(list) {
		if err = server.SetValue(ctx, key, list[start:]); err != nil {
			return nil, err
		}
		return []byte(utils.OkResponse), nil
	}

	if err = server.SetValue(ctx, key, list[start:end]); err != nil {
		return nil, err
	}
	return []byte(utils.OkResponse), nil
}

func handleLRem(ctx context.Context, cmd []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	keys, err := lremKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]
	value := cmd[3]

	count, ok := utils.AdaptType(cmd[2]).(int)
	if !ok {
		return nil, errors.New("count must be an integer")
	}

	absoluteCount := utils.AbsInt(count)

	if !server.KeyExists(ctx, key) {
		return nil, errors.New("LREM command on non-list item")
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, key)

	list, ok := server.GetValue(ctx, key).([]interface{})
	if !ok {
		return nil, errors.New("LREM command on non-list item")
	}

	switch {
	default:
		// Count is zero, keep list the same
	case count > 0:
		// Start from the head
		for i := 0; i < len(list); i++ {
			if absoluteCount == 0 {
				break
			}
			if fmt.Sprintf("%v", list[i]) == value {
				list[i] = nil
				absoluteCount -= 1
			}
		}
	case count < 0:
		// Start from the tail
		for i := len(list) - 1; i >= 0; i-- {
			if absoluteCount == 0 {
				break
			}
			if fmt.Sprintf("%v", list[i]) == value {
				list[i] = nil
				absoluteCount -= 1
			}
		}
	}

	list = slices.DeleteFunc(list, func(elem interface{}) bool {
		return elem == nil
	})

	if err = server.SetValue(ctx, key, list); err != nil {
		return nil, err
	}

	return []byte(utils.OkResponse), nil
}

func handleLMove(ctx context.Context, cmd []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	keys, err := lmoveKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	source := keys[0]
	destination := keys[1]
	whereFrom := strings.ToLower(cmd[3])
	whereTo := strings.ToLower(cmd[4])

	if !slices.Contains([]string{"left", "right"}, whereFrom) || !slices.Contains([]string{"left", "right"}, whereTo) {
		return nil, errors.New("wherefrom and whereto arguments must be either LEFT or RIGHT")
	}

	if !server.KeyExists(ctx, source) || !server.KeyExists(ctx, destination) {
		return nil, errors.New("both source and destination must be lists")
	}

	if _, err = server.KeyLock(ctx, source); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, source)

	_, err = server.KeyLock(ctx, destination)
	if err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, destination)

	sourceList, sourceOk := server.GetValue(ctx, source).([]interface{})
	destinationList, destinationOk := server.GetValue(ctx, destination).([]interface{})

	if !sourceOk || !destinationOk {
		return nil, errors.New("both source and destination must be lists")
	}

	switch whereFrom {
	case "left":
		err = server.SetValue(ctx, source, append([]interface{}{}, sourceList[1:]...))
		if whereTo == "left" {
			err = server.SetValue(ctx, destination, append(sourceList[0:1], destinationList...))
		} else if whereTo == "right" {
			err = server.SetValue(ctx, destination, append(destinationList, sourceList[0]))
		}
	case "right":
		err = server.SetValue(ctx, source, append([]interface{}{}, sourceList[:len(sourceList)-1]...))
		if whereTo == "left" {
			err = server.SetValue(ctx, destination, append(sourceList[len(sourceList)-1:], destinationList...))
		} else if whereTo == "right" {
			err = server.SetValue(ctx, destination, append(destinationList, sourceList[len(sourceList)-1]))
		}
	}

	if err != nil {
		return nil, err
	}

	return []byte(utils.OkResponse), nil
}

func handleLPush(ctx context.Context, cmd []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	keys, err := lpushKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	var newElems []interface{}

	for _, elem := range cmd[2:] {
		newElems = append(newElems, utils.AdaptType(elem))
	}

	key := keys[0]

	if !server.KeyExists(ctx, key) {
		switch strings.ToLower(cmd[0]) {
		case "lpushx":
			return nil, errors.New("LPUSHX command on non-list item")
		default:
			if _, err = server.CreateKeyAndLock(ctx, key); err != nil {
				return nil, err
			}
			if err = server.SetValue(ctx, key, []interface{}{}); err != nil {
				return nil, err
			}
		}
	} else {
		if _, err = server.KeyLock(ctx, key); err != nil {
			return nil, err
		}
	}
	defer server.KeyUnlock(ctx, key)

	currentList := server.GetValue(ctx, key)

	l, ok := currentList.([]interface{})
	if !ok {
		return nil, errors.New("LPUSH command on non-list item")
	}

	if err = server.SetValue(ctx, key, append(newElems, l...)); err != nil {
		return nil, err
	}
	return []byte(utils.OkResponse), nil
}

func handleRPush(ctx context.Context, cmd []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	keys, err := rpushKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	var newElems []interface{}

	for _, elem := range cmd[2:] {
		newElems = append(newElems, utils.AdaptType(elem))
	}

	if !server.KeyExists(ctx, key) {
		switch strings.ToLower(cmd[0]) {
		case "rpushx":
			return nil, errors.New("RPUSHX command on non-list item")
		default:
			if _, err = server.CreateKeyAndLock(ctx, key); err != nil {
				return nil, err
			}
			defer server.KeyUnlock(ctx, key)
			if err = server.SetValue(ctx, key, []interface{}{}); err != nil {
				return nil, err
			}
		}
	} else {
		if _, err = server.KeyLock(ctx, key); err != nil {
			return nil, err
		}
		defer server.KeyUnlock(ctx, key)
	}

	currentList := server.GetValue(ctx, key)

	l, ok := currentList.([]interface{})

	if !ok {
		return nil, errors.New("RPUSH command on non-list item")
	}

	if err = server.SetValue(ctx, key, append(l, newElems...)); err != nil {
		return nil, err
	}
	return []byte(utils.OkResponse), nil
}

func handlePop(ctx context.Context, cmd []string, server utils.EchoVault, conn *net.Conn) ([]byte, error) {
	keys, err := popKeyFunc(cmd)
	if err != nil {
		return nil, err
	}

	key := keys[0]

	if !server.KeyExists(ctx, key) {
		return nil, fmt.Errorf("%s command on non-list item", strings.ToUpper(cmd[0]))
	}

	if _, err = server.KeyLock(ctx, key); err != nil {
		return nil, err
	}
	defer server.KeyUnlock(ctx, key)

	list, ok := server.GetValue(ctx, key).([]interface{})
	if !ok {
		return nil, fmt.Errorf("%s command on non-list item", strings.ToUpper(cmd[0]))
	}

	switch strings.ToLower(cmd[0]) {
	default:
		if err = server.SetValue(ctx, key, list[1:]); err != nil {
			return nil, err
		}
		return []byte(fmt.Sprintf("+%v\r\n", list[0])), nil
	case "rpop":
		if err = server.SetValue(ctx, key, list[:len(list)-1]); err != nil {
			return nil, err
		}
		return []byte(fmt.Sprintf("+%v\r\n", list[len(list)-1])), nil
	}
}

func Commands() []utils.Command {
	return []utils.Command{
		{
			Command:           "lpush",
			Categories:        []string{utils.ListCategory, utils.WriteCategory, utils.FastCategory},
			Description:       "(LPUSH key value1 [value2]) Prepends one or more values to the beginning of a list, creates the list if it does not exist.",
			Sync:              true,
			KeyExtractionFunc: lpushKeyFunc,
			HandlerFunc:       handleLPush,
		},
		{
			Command:           "lpushx",
			Categories:        []string{utils.ListCategory, utils.WriteCategory, utils.FastCategory},
			Description:       "(LPUSHX key value) Prepends a value to the beginning of a list only if the list exists.",
			Sync:              true,
			KeyExtractionFunc: lpushKeyFunc,
			HandlerFunc:       handleLPush,
		},
		{
			Command:           "lpop",
			Categories:        []string{utils.ListCategory, utils.WriteCategory, utils.FastCategory},
			Description:       "(LPOP key) Removes and returns the first element of a list.",
			Sync:              true,
			KeyExtractionFunc: popKeyFunc,
			HandlerFunc:       handlePop,
		},
		{
			Command:           "llen",
			Categories:        []string{utils.ListCategory, utils.ReadCategory, utils.FastCategory},
			Description:       "(LLEN key) Return the length of a list.",
			Sync:              false,
			KeyExtractionFunc: llenKeyFunc,
			HandlerFunc:       handleLLen,
		},
		{
			Command:           "lrange",
			Categories:        []string{utils.ListCategory, utils.ReadCategory, utils.SlowCategory},
			Description:       "(LRANGE key start end) Return a range of elements between the given indices.",
			Sync:              false,
			KeyExtractionFunc: lrangeKeyFunc,
			HandlerFunc:       handleLRange,
		},
		{
			Command:           "lindex",
			Categories:        []string{utils.ListCategory, utils.ReadCategory, utils.SlowCategory},
			Description:       "(LINDEX key index) Gets list element by index.",
			Sync:              false,
			KeyExtractionFunc: lindexKeyFunc,
			HandlerFunc:       handleLIndex,
		},
		{
			Command:           "lset",
			Categories:        []string{utils.ListCategory, utils.WriteCategory, utils.SlowCategory},
			Description:       "(LSET key index value) Sets the value of an element in a list by its index.",
			Sync:              true,
			KeyExtractionFunc: lsetKeyFunc,
			HandlerFunc:       handleLSet,
		},
		{
			Command:           "ltrim",
			Categories:        []string{utils.ListCategory, utils.WriteCategory, utils.SlowCategory},
			Description:       "(LTRIM key start end) Trims a list to the specified range.",
			Sync:              true,
			KeyExtractionFunc: ltrimKeyFunc,
			HandlerFunc:       handleLTrim,
		},
		{
			Command:           "lrem",
			Categories:        []string{utils.ListCategory, utils.WriteCategory, utils.SlowCategory},
			Description:       "(LREM key count value) Remove elements from list.",
			Sync:              true,
			KeyExtractionFunc: lremKeyFunc,
			HandlerFunc:       handleLRem,
		},
		{
			Command:           "lmove",
			Categories:        []string{utils.ListCategory, utils.WriteCategory, utils.SlowCategory},
			Description:       "(LMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT>) Move element from one list to the other specifying left/right for both lists.",
			Sync:              true,
			KeyExtractionFunc: lmoveKeyFunc,
			HandlerFunc:       handleLMove,
		},
		{
			Command:           "rpop",
			Categories:        []string{utils.ListCategory, utils.WriteCategory, utils.FastCategory},
			Description:       "(RPOP key) Removes and gets the last element in a list.",
			Sync:              true,
			KeyExtractionFunc: popKeyFunc,
			HandlerFunc:       handlePop,
		},
		{
			Command:           "rpush",
			Categories:        []string{utils.ListCategory, utils.WriteCategory, utils.FastCategory},
			Description:       "(RPUSH key value [value2]) Appends one or multiple elements to the end of a list.",
			Sync:              true,
			KeyExtractionFunc: rpushKeyFunc,
			HandlerFunc:       handleRPush,
		},
		{
			Command:           "rpushx",
			Categories:        []string{utils.ListCategory, utils.WriteCategory, utils.FastCategory},
			Description:       "(RPUSHX key value) Appends an element to the end of a list, only if the list exists.",
			Sync:              true,
			KeyExtractionFunc: rpushKeyFunc,
			HandlerFunc:       handleRPush,
		},
	}
}
