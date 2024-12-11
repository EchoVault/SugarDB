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
	"errors"
	"fmt"
	"github.com/echovault/sugardb/internal"
	"github.com/echovault/sugardb/internal/constants"
	"slices"
	"strconv"
	"strings"
)

func handleLLen(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := llenKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]

	if !keyExists {
		// If key does not exist, return 0
		return []byte(":0\r\n"), nil
	}

	if list, ok := params.GetValues(params.Context, []string{key})[key].([]string); ok {
		return []byte(fmt.Sprintf(":%d\r\n", len(list))), nil
	}

	return nil, errors.New("LLEN command on non-list item")
}

func handleLIndex(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := lindexKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]
	if !keyExists {
		return []byte(fmt.Sprintf("$-1\r\n")), nil
	}

	list, ok := params.GetValues(params.Context, []string{key})[key].([]string)
	if !ok {
		return nil, errors.New("LINDEX command on non-list item")
	}

	index, err := strconv.Atoi(params.Command[2])
	if err != nil {
		return nil, errors.New("index must be an integer")
	}
	// If index is less than 0, calculate index from the end of the list
	if index < 0 {
		index = len(list) + index
	}

	if index >= len(list) || index < 0 {
		return []byte(fmt.Sprintf("$-1\r\n")), nil
	}

	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(list[index]), list[index])), nil
}

func handleLRange(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := lrangeKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]
	if !keyExists {
		return []byte("*0\r\n"), nil
	}

	list, ok := params.GetValues(params.Context, []string{key})[key].([]string)
	if !ok {
		return nil, errors.New("LRANGE command on non-list item")
	}

	start, err := strconv.Atoi(params.Command[2])
	if err != nil {
		return nil, fmt.Errorf("start index must be an integer")
	}
	// If start is < 0, calculate it from the end of the list
	if start < 0 {
		start = len(list) + start
	}

	end, err := strconv.Atoi(params.Command[3])
	if err != nil {
		return nil, fmt.Errorf("end index must be an integer")
	}
	// If end is < 0, calculate it from the end of the list
	if end < 0 {
		end = len(list) - end
	}
	// If end is greater than list length, set it to the last element of the list
	if end > len(list) {
		end = len(list) - 1
	}

	if start > end || start > len(list) {
		return []byte("*0\r\n"), nil
	}

	res := fmt.Sprintf("*%d\r\n", end-start+1)
	for i := start; i <= end; i++ {
		res += fmt.Sprintf("$%d\r\n%s\r\n", len(list[i]), list[i])
	}

	return []byte(res), nil
}

func handleLSet(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := lsetKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]
	keyExists := params.KeysExist(params.Context, keys.WriteKeys)[key]
	if !keyExists {
		return nil, errors.New("LSET command on non-list item")
	}

	index, err := strconv.Atoi(params.Command[2])
	if err != nil {
		return nil, errors.New("index must be an integer")
	}

	list, ok := params.GetValues(params.Context, []string{key})[key].([]string)
	if !ok {
		return nil, errors.New("LSET command on non-list item")
	}

	// If index is negative set index to length - index
	if index < 0 {
		index = len(list) + index
	}

	if !(index >= 0 && index < len(list)) {
		return nil, errors.New("index must be within list range")
	}

	list[index] = params.Command[3]
	if err = params.SetValues(params.Context, map[string]interface{}{key: list}); err != nil {
		return nil, err
	}

	return []byte(constants.OkResponse), nil
}

func handleLTrim(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := ltrimKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]
	keyExists := params.KeysExist(params.Context, keys.WriteKeys)[key]
	if !keyExists {
		return []byte(constants.OkResponse), nil
	}

	start, err := strconv.Atoi(params.Command[2])
	if err != nil {
		return nil, fmt.Errorf("start index must be an integer")
	}
	end, err := strconv.Atoi(params.Command[3])
	if err != nil {
		return nil, fmt.Errorf("end index must be an integer")
	}

	list, ok := params.GetValues(params.Context, []string{key})[key].([]string)
	if !ok {
		return nil, errors.New("LTRIM command on non-list item")
	}

	// If start and end indices are negative, calculate them from the end of the list
	if start < 0 {
		start = len(list) + start
	}
	if end < 0 {
		end = len(list) + end
	}

	// If start index is greater than end index or greater than the index of the last element, delete the key.
	if start > end || start > len(list)-1 {
		if err = params.DeleteKey(params.Context, key); err != nil {
			return nil, err
		}
		return []byte(constants.OkResponse), nil
	}

	// If end is greater than the length of the list, set it to the length of the list
	if end > len(list) {
		end = len(list)
	}
	// In order to include end element, if the end index is within range, add 1
	if end <= len(list)-1 {
		end += 1
	}

	if err = params.SetValues(params.Context, map[string]interface{}{key: list[start:end]}); err != nil {
		return nil, err
	}

	return []byte(constants.OkResponse), nil
}

func handleLRem(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := lremKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]
	keyExists := params.KeysExist(params.Context, keys.WriteKeys)[key]

	value := params.Command[3]
	count, err := strconv.Atoi(params.Command[2])
	if err != nil {
		return nil, errors.New("count must be an integer")
	}
	absoluteCount := internal.AbsInt(count)

	if !keyExists {
		return []byte(":0\r\n"), nil
	}

	list, ok := params.GetValues(params.Context, []string{key})[key].([]string)
	if !ok {
		return nil, errors.New("LREM command on non-list item")
	}

	removedCount := len(list)

	switch {
	default:
		// Count is zero, remove all instances of the element from the list.
		for i := 0; i < len(list); i++ {
			if list[i] == value {
				list = append(list[:i], list[i+1:]...)
				absoluteCount += 1
			}
		}
	case count > 0:
		// Start from the head
		for i := 0; i < len(list); i++ {
			if absoluteCount == 0 {
				break
			}
			if list[i] == value {
				list = append(list[:i], list[i+1:]...)
				absoluteCount -= 1
			}
		}
	case count < 0:
		// Start from the tail
		for i := len(list) - 1; i >= 0; i-- {
			if absoluteCount == 0 {
				break
			}
			if list[i] == value {
				list = append(list[:i], list[i+1:]...)
				absoluteCount -= 1
				removedCount += 0
			}
		}
	}

	if err = params.SetValues(params.Context, map[string]interface{}{key: list}); err != nil {
		return nil, err
	}

	removedCount = removedCount - len(list)
	return []byte(fmt.Sprintf(":%d\r\n", removedCount)), nil
}

func handleLMove(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := lmoveKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	keysExist := params.KeysExist(params.Context, keys.WriteKeys)
	source, destination := keys.WriteKeys[0], keys.WriteKeys[1]
	whereFrom := strings.ToLower(params.Command[3])
	whereTo := strings.ToLower(params.Command[4])

	if !slices.Contains([]string{"left", "right"}, whereFrom) || !slices.Contains([]string{"left", "right"}, whereTo) {
		return nil, errors.New("wherefrom and whereto arguments must be either LEFT or RIGHT")
	}

	if !keysExist[source] || !keysExist[destination] {
		return nil, errors.New("both source and destination must be lists")
	}

	lists := params.GetValues(params.Context, keys.WriteKeys)
	sourceList, sourceOk := lists[source].([]string)
	destinationList, destinationOk := lists[destination].([]string)

	if !sourceOk || !destinationOk {
		return nil, errors.New("both source and destination must be lists")
	}

	switch whereFrom {
	case "left":
		err = params.SetValues(params.Context, map[string]interface{}{
			source: append([]string{}, sourceList[1:]...),
			destination: func() []string {
				if whereTo == "left" {
					return append(sourceList[0:1], destinationList...)
				}
				// whereTo == "right"
				return append(destinationList, sourceList[0])
			}(),
		})
	case "right":
		err = params.SetValues(params.Context, map[string]interface{}{
			source: append([]string{}, sourceList[:len(sourceList)-1]...),
			destination: func() []string {
				if whereTo == "left" {
					return append(sourceList[len(sourceList)-1:], destinationList...)
				}
				// whereTo == "right"
				return append(destinationList, sourceList[len(sourceList)-1])
			}(),
		})
	}

	if err != nil {
		return nil, err
	}

	return []byte(constants.OkResponse), nil
}

func handleLPush(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := lpushKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	var newElems []string

	for _, elem := range params.Command[2:] {
		newElems = append(newElems, elem)
	}

	key := keys.WriteKeys[0]
	keyExists := params.KeysExist(params.Context, keys.WriteKeys)[key]

	if !keyExists {
		switch strings.ToLower(params.Command[0]) {
		case "lpushx":
			return nil, errors.New("LPUSHX command on non-existent key")
		default:
			if err = params.SetValues(params.Context, map[string]interface{}{key: []string{}}); err != nil {
				return nil, err
			}
		}
	}

	currentList := params.GetValues(params.Context, []string{key})[key]
	l, ok := currentList.([]string)
	if !ok {
		return nil, errors.New("LPUSH command on non-list item")
	}

	if err = params.SetValues(params.Context, map[string]interface{}{key: append(newElems, l...)}); err != nil {
		return nil, err
	}

	return []byte(fmt.Sprintf(":%d\r\n", len(l)+len(newElems))), nil
}

func handleRPush(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := rpushKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]
	keyExists := params.KeysExist(params.Context, keys.WriteKeys)[key]

	var newElems []string

	for _, elem := range params.Command[2:] {
		newElems = append(newElems, elem)
	}

	if !keyExists {
		switch strings.ToLower(params.Command[0]) {
		case "rpushx":
			return nil, errors.New("RPUSHX command on non-existent key")
		default:
			if err = params.SetValues(params.Context, map[string]interface{}{key: []string{}}); err != nil {
				return nil, err
			}
		}
	}

	currentList := params.GetValues(params.Context, []string{key})[key]
	l, ok := currentList.([]string)
	if !ok {
		return nil, errors.New("RPUSH command on non-list item")
	}

	if err = params.SetValues(params.Context, map[string]interface{}{key: append(l, newElems...)}); err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf(":%d\r\n", len(l)+len(newElems))), nil
}

func handlePop(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := popKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]
	keyExists := params.KeysExist(params.Context, keys.WriteKeys)[key]
	if !keyExists {
		return []byte("$-1\r\n"), nil
	}

	list, ok := params.GetValues(params.Context, []string{key})[key].([]string)
	if !ok {
		return nil, fmt.Errorf("%s command on non-list item", strings.ToUpper(params.Command[0]))
	}

	withCount := false
	count := 1
	// Parse count
	if len(params.Command) == 3 {
		withCount = true
		count, err = strconv.Atoi(params.Command[2])
		if err != nil {
			return nil, fmt.Errorf("count must be an integer")
		}
		// Set absolute value for count
		count = internal.AbsInt(count)
		// If count is greater than the length of the list, set count to the length of the list.
		if count > len(list) {
			count = len(list)
		}
	}

	// Return nil if list is empty
	if len(list) == 0 {
		return []byte("$-1\r\n"), nil
	}

	var popped []string
	for i := 0; i < count; i++ {
		if strings.EqualFold(params.Command[0], "lpop") {
			// Pop from the left
			popped = append(popped, list[0])
			list = list[1:]
		} else {
			// Pop from the right
			popped = append(popped, list[len(list)-1])
			list = list[:len(list)-1]
		}
	}
	if err = params.SetValues(params.Context, map[string]interface{}{key: list}); err != nil {
		return nil, err
	}

	// If withCount is false, return a bulk string of the popped element.
	if !withCount {
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(popped[0]), popped[0])), nil
	}
	// Return an array of the popped elements.
	res := fmt.Sprintf("*%d\r\n", len(popped))
	for i := 0; i < len(popped); i++ {
		res += fmt.Sprintf("$%d\r\n%s\r\n", len(popped[i]), popped[i])
	}
	return []byte(res), nil
}

func Commands() []internal.Command {
	return []internal.Command{
		{
			Command:    "lpush",
			Module:     constants.ListModule,
			Categories: []string{constants.ListCategory, constants.WriteCategory, constants.FastCategory},
			Description: `(LPUSH key element [element ...]) 
Prepends one or more values to the beginning of a list, creates the list if it does not exist.`,
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: lpushKeyFunc,
			HandlerFunc:       handleLPush,
		},
		{
			Command:    "lpushx",
			Module:     constants.ListModule,
			Categories: []string{constants.ListCategory, constants.WriteCategory, constants.FastCategory},
			Description: `(LPUSHX key element [element ...]) 
Prepends a value to the beginning of a list only if the list exists.`,
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: lpushKeyFunc,
			HandlerFunc:       handleLPush,
		},
		{
			Command:    "lpop",
			Module:     constants.ListModule,
			Categories: []string{constants.ListCategory, constants.WriteCategory, constants.FastCategory},
			Description: `(LPOP key [count]) 
Removes count elements from the beginning of the list and returns an array of the elements removed.
Returns a bulk string of the first element when called without count.
Returns an array of n elements from the beginning of the list when called with a count when n=count. `,
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: popKeyFunc,
			HandlerFunc:       handlePop,
		},
		{
			Command:           "llen",
			Module:            constants.ListModule,
			Categories:        []string{constants.ListCategory, constants.ReadCategory, constants.FastCategory},
			Description:       "(LLEN key) Return the length of a list.",
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: llenKeyFunc,
			HandlerFunc:       handleLLen,
		},
		{
			Command:           "lrange",
			Module:            constants.ListModule,
			Categories:        []string{constants.ListCategory, constants.ReadCategory, constants.SlowCategory},
			Description:       "(LRANGE key start end) Return a range of elements between the given indices.",
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: lrangeKeyFunc,
			HandlerFunc:       handleLRange,
		},
		{
			Command:           "lindex",
			Module:            constants.ListModule,
			Categories:        []string{constants.ListCategory, constants.ReadCategory, constants.FastCategory},
			Description:       "(LINDEX key index) Gets list element by index.",
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: lindexKeyFunc,
			HandlerFunc:       handleLIndex,
		},
		{
			Command:           "lset",
			Module:            constants.ListModule,
			Categories:        []string{constants.ListCategory, constants.WriteCategory, constants.FastCategory},
			Description:       "(LSET key index element) Sets the value of an element in a list by its index.",
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: lsetKeyFunc,
			HandlerFunc:       handleLSet,
		},
		{
			Command:           "ltrim",
			Module:            constants.ListModule,
			Categories:        []string{constants.ListCategory, constants.WriteCategory, constants.SlowCategory},
			Description:       "(LTRIM key start end) Trims a list using the specified range.",
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: ltrimKeyFunc,
			HandlerFunc:       handleLTrim,
		},
		{
			Command:           "lrem",
			Module:            constants.ListModule,
			Categories:        []string{constants.ListCategory, constants.WriteCategory, constants.SlowCategory},
			Description:       "(LREM key count element) Remove <count> elements from list.",
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: lremKeyFunc,
			HandlerFunc:       handleLRem,
		},
		{
			Command:    "lmove",
			Module:     constants.ListModule,
			Categories: []string{constants.ListCategory, constants.WriteCategory, constants.SlowCategory},
			Description: `(LMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT>) 
Move element from one list to the other specifying left/right for both lists.`,
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: lmoveKeyFunc,
			HandlerFunc:       handleLMove,
		},
		{
			Command:    "rpop",
			Module:     constants.ListModule,
			Categories: []string{constants.ListCategory, constants.WriteCategory, constants.FastCategory},
			Description: `(RPOP key [count]) 
Removes count elements from the end of the list and returns an array of the elements removed.
Returns a bulk string of the last element when called without count.
Returns an array of n elements from the end of the list when called with a count when n=count.`,
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: popKeyFunc,
			HandlerFunc:       handlePop,
		},
		{
			Command:           "rpush",
			Module:            constants.ListModule,
			Categories:        []string{constants.ListCategory, constants.WriteCategory, constants.FastCategory},
			Description:       "(RPUSH key element [element ...]) Appends one or multiple elements to the end of a list.",
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: rpushKeyFunc,
			HandlerFunc:       handleRPush,
		},
		{
			Command:           "rpushx",
			Module:            constants.ListModule,
			Categories:        []string{constants.ListCategory, constants.WriteCategory, constants.FastCategory},
			Description:       "(RPUSHX key element [element ...]) Appends an element to the end of a list, only if the list exists.",
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: rpushKeyFunc,
			HandlerFunc:       handleRPush,
		},
	}
}
