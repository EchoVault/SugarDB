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

package echovault

import (
	"github.com/echovault/echovault/internal"
	"strconv"
)

// ZAddOptions allows you to modify the effects of the ZAdd command.
//
// "NX" only adds the member if it currently does not exist in the sorted set. This flag is mutually exclusive with the
// "GT" and "LT" flags. The "NX" flag takes higher priority than the "XX" flag.
//
// "XX" only updates the scores of members that exist in the sorted set.
//
// "GT"" only updates the score if the new score is greater than the current score. The "GT" flat is higher priority
// than the "LT" flag.
//
// "LT" only updates the score if the new score is less than the current score.
//
// "CH" modifies the result to return total number of members changed + added, instead of only new members added. When
// this flag is set to true, only the number of members that have been updated will be returned.
//
// "INCR" modifies the command to act like ZIncrBy, only one score/member pair can be specified in this mode. When this flag
// is provided, only one member/score pair is allowed.
type ZAddOptions struct {
	NX   bool
	XX   bool
	GT   bool
	LT   bool
	CH   bool
	INCR bool
}

// ZInterOptions allows you to modify the result of the ZInter* and ZUnion* family of commands
//
// Weights is a slice of float64 that determines the weights of each sorted set in the aggregation command.
// each weight will be each weight will be applied to the sorted set at the corresponding index.
// The weight value is multiplied by each member of corresponding sorted set.
//
// Aggregate determines how the scores are combined AFTER the weights are applied. There are 3 possible vales,
// "MIN" will select the minimum score element to place in the resulting sorted set.
// "MAX" will select the maximum score element to place in the resulting sorted set.
// "SUM" will add all the scores to place in the resulting sorted set.
//
// WithScores determines whether to return the scores of the resulting set.
type ZInterOptions struct {
	Weights    []float64
	Aggregate  string
	WithScores bool
}
type ZInterStoreOptions ZInterOptions
type ZUnionOptions ZInterOptions
type ZUnionStoreOptions ZInterOptions

// ZMPopOptions allows you to modify the result of the ZMPop command.
//
// Min instructs EchoVault to pop the minimum score elements. Min is higher priority than Max.
//
// Max instructs EchoVault to pop the maximum score elements.
//
// Count specifies the number of elements to pop.
type ZMPopOptions struct {
	Min   bool
	Max   bool
	Count uint
}

// ZRangeOptions allows you to modify the effects of the ZRange* family of commands.
//
// WithScores specifies whether to return the associated scores.
//
// ByScore compares the elements by score within the numerical ranges specified. ByScore is higher priority than ByLex.
//
// ByLex returns the elements within the lexicographical ranges specified.
//
// Rev reverses the result from the previous filters.
//
// Offset specifies the offset to from which to start the ZRange process.
//
// Count specifies the number of elements to return.
type ZRangeOptions struct {
	WithScores bool
	ByScore    bool
	ByLex      bool
	Rev        bool
	Offset     uint
	Count      uint
}
type ZRangeStoreOptions ZRangeOptions

func buildMemberScoreMap(arr [][]string, withscores bool) (map[string]float64, error) {
	result := make(map[string]float64, len(arr))
	for _, entry := range arr {
		if withscores {
			score, err := strconv.ParseFloat(entry[1], 64)
			if err != nil {
				return nil, err
			}
			result[entry[0]] = score
			continue
		}
		result[entry[0]] = 0
	}
	return result, nil
}

func buildIntegerScoreMap(arr [][]string, withscores bool) (map[int]float64, error) {
	result := make(map[int]float64, len(arr))
	for _, entry := range arr {
		rank, err := strconv.Atoi(entry[0])
		if err != nil {
			return nil, err
		}
		result[rank] = 0
		if withscores {
			score, err := strconv.ParseFloat(entry[1], 64)
			if err != nil {
				return nil, err
			}
			result[rank] = score
		}
	}
	return result, nil
}

// ZAdd adds member(s) to a sorted set. If the sorted set does not exist, a new sorted set is created with the
// member(s).
//
// Parameters:
//
// `key` - string - the key to update.
//
// `members` - map[string]float64 - a map of the members to add. The key is the string and the value is a float64 score.
//
// `options` - ZAddOptions
//
// Returns: The number of members added, or the number of members updated in the "CH" flag is true.
//
// Errors:
//
// "GT/LT flags not allowed if NX flag is provided" - when GT/LT flags are provided alongside NX flag.
//
// "cannot pass more than one score/member pair when INCR flag is provided" - when INCR flag is provided with more than
// one member-score pair.
//
// "value at <key> is not a sorted set" - when the provided key exists but is not a sorted set
func (server *EchoVault) ZAdd(key string, members map[string]float64, options ZAddOptions) (int, error) {
	cmd := []string{"ZADD", key}

	switch {
	case options.NX:
		cmd = append(cmd, "NX")
	case options.XX:
		cmd = append(cmd, "XX")
	}

	switch {
	case options.GT:
		cmd = append(cmd, "GT")
	case options.LT:
		cmd = append(cmd, "LT")
	}

	if options.CH {
		cmd = append(cmd, "CH")
	}

	if options.INCR {
		cmd = append(cmd, "INCR")
	}

	for member, score := range members {
		cmd = append(cmd, []string{strconv.FormatFloat(score, 'f', -1, 64), member}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

// ZCard returns the cardinality of the sorted set.
//
// Parameters:
//
// `key` - string - the key of the sorted set.
//
// Returns: The cardinality of the sorted set. Returns 0 if the keys does not exist.
//
// Errors:
//
// "value at <key> is not a sorted set" - when the provided key exists but is not a sorted set
func (server *EchoVault) ZCard(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ZCARD", key}), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// ZCount returns the number of elements in the sorted set key with scores in the range of min and max.
//
// Parameters:
//
// `key` - string - the key of the sorted set.
//
// `min` - float64 - the minimum score boundary.
//
// `max` - float64 - the maximum score boundary.
//
// Returns: The number of members with scores in the given range. Returns 0 if the keys does not exist.
//
// Errors:
//
// "value at <key> is not a sorted set" - when the provided key exists but is not a sorted set
func (server *EchoVault) ZCount(key string, min, max float64) (int, error) {
	cmd := []string{
		"ZCOUNT",
		key,
		strconv.FormatFloat(min, 'f', -1, 64),
		strconv.FormatFloat(max, 'f', -1, 64),
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// ZDiff Calculates the difference between the sorted sets and returns the resulting sorted set.
// All keys that are non-existed are skipped.
//
// Parameters:
//
// `withscores` - bool - whether to return the results with scores or not. If false, all the returned scores
// will be 0.
//
// `keys` - []string - the keys to the sorted sets to be used in calculating the difference.
//
// Returns: A map representing the resulting sorted set where the key is the member and the value is a float64 score.
//
// Errors:
//
// "value at <key> is not a sorted set" - when the provided key exists but is not a sorted set.
func (server *EchoVault) ZDiff(withscores bool, keys ...string) (map[string]float64, error) {
	cmd := append([]string{"ZDIFF"}, keys...)
	if withscores {
		cmd = append(cmd, "WITHSCORES")
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)
	if err != nil {
		return nil, err
	}

	return buildMemberScoreMap(arr, withscores)
}

// ZDiffStore Calculates the difference between the sorted sets and stores the resulting sorted set at 'destination'.
// Non-existent keys will be skipped.
//
// Parameters:
//
// `destination` - string - the destination key at which to store the resulting sorted set.
//
// `keys` - []string - the keys to the sorted sets to be used in calculating the difference.
//
// Returns: The cardinality of the new sorted set.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZDiffStore(destination string, keys ...string) (int, error) {
	cmd := append([]string{"ZDIFFSTORE", destination}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// ZInter Calculates the intersection between the sorted sets and returns the resulting sorted set.
// if any of the keys provided are non-existent, an empty map is returned.
//
// Parameters:
//
// `keys` - []string - the keys to the sorted sets to be used in calculating the intersection.
//
// `options` - ZInterOptions
//
// Returns: A map representing the resulting sorted set where the key is the member and the value is a float64 score.
//
// Errors:
//
// "value at <key> is not a sorted set" - when the provided key exists but is not a sorted set.
func (server *EchoVault) ZInter(keys []string, options ZInterOptions) (map[string]float64, error) {
	cmd := append([]string{"ZINTER"}, keys...)

	if len(options.Weights) > 0 {
		cmd = append(cmd, "WEIGHTS")
		for i := 0; i < len(options.Weights); i++ {
			cmd = append(cmd, strconv.FormatFloat(options.Weights[i], 'f', -1, 64))
		}
	}

	if options.Aggregate != "" {
		cmd = append(cmd, []string{"AGGREGATE", options.Aggregate}...)
	}

	if options.WithScores {
		cmd = append(cmd, "WITHSCORES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)
	if err != nil {
		return nil, err
	}

	return buildMemberScoreMap(arr, options.WithScores)
}

// ZInterStore Calculates the intersection between the sorted sets and stores the resulting sorted set at 'destination'.
// If any of the keys does not exist, the operation is abandoned.
//
// Parameters:
//
// `destination` - string - the destination key at which to store the resulting sorted set.
//
// `keys` - []string - the keys to the sorted sets to be used in calculating the intersection.
//
// `options` - ZInterStoreOptions
//
// Returns: The cardinality of the new sorted set.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZInterStore(destination string, keys []string, options ZInterStoreOptions) (int, error) {
	cmd := append([]string{"ZINTERSTORE", destination}, keys...)

	if len(options.Weights) > 0 {
		cmd = append(cmd, "WEIGHTS")
		for _, weight := range options.Weights {
			cmd = append(cmd, strconv.FormatFloat(weight, 'f', -1, 64))
		}
	}

	if options.Aggregate != "" {
		cmd = append(cmd, []string{"AGGREGATE", options.Aggregate}...)
	}

	if options.WithScores {
		cmd = append(cmd, "WITHSCORES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

// ZUnion Calculates the union between the sorted sets and returns the resulting sorted set.
// if any of the keys provided are non-existent, an error is returned.
//
// Parameters:
//
// `keys` - []string - the keys to the sorted sets to be used in calculating the union.
//
// `options` - ZUnionOptions
//
// Returns: A map representing the resulting sorted set where the key is the member and the value is a float64 score.
//
// Errors:
//
// "value at <key> is not a sorted set" - when the provided key exists but is not a sorted set.
func (server *EchoVault) ZUnion(keys []string, options ZUnionOptions) (map[string]float64, error) {
	cmd := append([]string{"ZUNION"}, keys...)

	if len(options.Weights) > 0 {
		cmd = append(cmd, "WEIGHTS")
		for _, weight := range options.Weights {
			cmd = append(cmd, strconv.FormatFloat(weight, 'f', -1, 64))
		}
	}

	if options.Aggregate != "" {
		cmd = append(cmd, []string{"AGGREGATE", options.Aggregate}...)
	}

	if options.WithScores {
		cmd = append(cmd, "WITHSCORES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)
	if err != nil {
		return nil, err
	}

	return buildMemberScoreMap(arr, options.WithScores)
}

// ZUnionStore Calculates the union between the sorted sets and stores the resulting sorted set at 'destination'.
// Non-existent keys will be skipped.
//
// Parameters:
//
// `destination` - string - the destination key at which to store the resulting sorted set.
//
// `keys` - []string - the keys to the sorted sets to be used in calculating the union.
//
// `options` - ZUnionStoreOptions
//
// Returns: The cardinality of the new sorted set.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZUnionStore(destination string, keys []string, options ZUnionStoreOptions) (int, error) {
	cmd := append([]string{"ZUNIONSTORE", destination}, keys...)

	if len(options.Weights) > 0 {
		cmd = append(cmd, "WEIGHTS")
		for _, weight := range options.Weights {
			cmd = append(cmd, strconv.FormatFloat(float64(weight), 'f', -1, 64))
		}
	}

	if options.Aggregate != "" {
		cmd = append(cmd, []string{"AGGREGATE", options.Aggregate}...)
	}

	if options.WithScores {
		cmd = append(cmd, "WITHSCORES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

// ZIncrBy Increments the score of the specified sorted set's member by the increment. If the member does not exist, it is created.
// If the key does not exist, it is created with new sorted set and the member added with the increment as its score.
//
// Parameters:
//
// `key` - string - the keys to the sorted set.
//
// `increment` - float64 - the increment to apply to the member's score.
//
// `member` - string - the member to increment.
//
// Returns: The cardinality of the new sorted set.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZIncrBy(key string, increment float64, member string) (float64, error) {
	cmd := []string{"ZINCRBY", key, strconv.FormatFloat(increment, 'f', -1, 64), member}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	f, err := internal.ParseFloatResponse(b)
	if err != nil {
		return 0, err
	}
	return f, nil
}

// ZMPop Pop a 'count' elements from multiple sorted sets. MIN or MAX determines whether to pop elements with the lowest
// or highest scores respectively.
//
// Parameters:
//
// `keys` - []string - the keys to the sorted sets to pop from.
//
// `options` - ZMPopOptions
//
// Returns: A 2-dimensional slice where each slice contains the member and score at the 0 and 1 indices respectively.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZMPop(keys []string, options ZMPopOptions) ([][]string, error) {
	cmd := append([]string{"ZMPOP"}, keys...)

	switch {
	case options.Min:
		cmd = append(cmd, "MIN")
	case options.Max:
		cmd = append(cmd, "MAX")
	default:
		cmd = append(cmd, "MIN")
	}

	switch {
	case options.Count != 0:
		cmd = append(cmd, []string{"COUNT", strconv.Itoa(int(options.Count))}...)
	default:
		cmd = append(cmd, []string{"COUNT", strconv.Itoa(1)}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	return internal.ParseNestedStringArrayResponse(b)
}

// ZMScore Returns the associated scores of the specified member in the sorted set.
//
// Parameters:
//
// `key` - string - The keys to the sorted set.
//
// `members` - ...string - Them members whose scores will be returned.
//
// Returns: A slice of interface{} with the result scores. For existing members, the entry will be represented by a string.
// For non-existent members, the score will be nil. You will have to format the string score into a float64 if you
// would like to use it as a float64.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZMScore(key string, members ...string) ([]interface{}, error) {
	cmd := []string{"ZMSCORE", key}
	for _, member := range members {
		cmd = append(cmd, member)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseStringArrayResponse(b)
	if err != nil {
		return nil, err
	}

	scores := make([]interface{}, len(arr))
	for i, e := range arr {
		if e == "" {
			scores[i] = nil
			continue
		}
		score, err := strconv.ParseFloat(e, 64)
		if err != nil {
			return nil, err
		}
		scores[i] = score
	}

	return scores, nil
}

// ZLexCount returns the number of elements in the sorted set within the lexicographical range between min and max.
// This function only returns a non-zero value if all the members have the same score.
//
// Parameters:
//
// `key` - string - the key of the sorted set.
//
// `min` - string - the minimum lex boundary.
//
// `max` - string - the maximum lex boundary.
//
// Returns: The number of members within the given lexicographical range.
// Returns 0 if the keys does not exist or all the members don't have the same score.
//
// Errors:
//
// "value at <key> is not a sorted set" - when the provided key exists but is not a sorted set
func (server *EchoVault) ZLexCount(key, min, max string) (int, error) {
	cmd := []string{"ZLEXCOUNT", key, min, max}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// ZPopMax Removes and returns 'count' number of members in the sorted set with the highest scores. Default count is 1.
//
// Parameters:
//
// `key` - string - The keys to the sorted set.
//
// `count` - uint - The number of max elements to pop. If a count of 0 is provided, it will be ignored
// and 1 element will be popped instead.
//
// Returns: A 2-dimensional slice where each slice contains a member and its score at the 0 and 1 indices respectively.
// The returned scores are strings. If you'd like to use them as float64 or another numeric type, you will have to
// format them.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZPopMax(key string, count uint) ([][]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ZPOPMAX", key, strconv.Itoa(int(count))}), nil, false, true)
	if err != nil {
		return nil, err
	}
	return internal.ParseNestedStringArrayResponse(b)
}

// ZPopMin Removes and returns 'count' number of members in the sorted set with the lowest scores. Default count is 1.
//
// Parameters:
//
// `key` - string - The keys to the sorted set.
//
// `count` - uint - The number of min elements to pop. If a count of 0 is provided, it will be ignored
// and 1 element will be popped instead.
//
// Returns: A 2-dimensional slice where each slice contains a member and its score at the 0 and 1 indices respectively.
// The returned scores are strings. If you'd like to use them as float64 or another numeric type, you will have to
// format them.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZPopMin(key string, count uint) ([][]string, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ZPOPMIN", key, strconv.Itoa(int(count))}), nil, false, true)
	if err != nil {
		return nil, err
	}
	return internal.ParseNestedStringArrayResponse(b)
}

// ZRandMember Returns a list of length equivalent to 'count' containing random members of the sorted set.
// If count is negative, repeated elements are allowed. If count is positive, the returned elements will be distinct.
// The default count is 1. If a count of 0 is passed, it will be ignored.
//
// Parameters:
//
// `key` - string - The keys to the sorted set.
//
// `count` - int - The number of random members to return. If the absolute value of count is greater than the
// sorted set's cardinality, the whole sorted set will be returned.
//
// `withscores` - bool - Whether to return the members' associated scores. If this is false, the returned scores will
// be 0.
//
// Returns: A 2-dimensional slice where each slice contains a member and its score at the 0 and 1 indices respectively.
// The returned scores are strings. If you'd like to use them as float64 or another numeric type, you will have to
// format them.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZRandMember(key string, count int, withscores bool) ([][]string, error) {
	cmd := []string{"ZRANDMEMBER", key}
	if count != 0 {
		cmd = append(cmd, strconv.Itoa(count))
	}
	if withscores {
		cmd = append(cmd, "WITHSCORES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	return internal.ParseNestedStringArrayResponse(b)
}

// ZRank Returns the rank of the specified member in the sorted set. The rank is derived from organising the members
// in descending order of score.
//
// Parameters:
//
// `key` - string - The keys to the sorted set.
//
// `member` - string - The member whose rank will be returned.
//
// `withscores` - bool - Whether to return the member associated scores. If this is false, the returned score will
// be 0.
//
// Returns: A map of map[string]float64 where the key is the member and the value is the score.
// If the member does not exist in the sorted set, an empty map is returned.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZRank(key string, member string, withscores bool) (map[int]float64, error) {
	cmd := []string{"ZRANK", key, member}
	if withscores {
		cmd = append(cmd, "WITHSCORES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseStringArrayResponse(b)

	if len(arr) == 0 {
		return nil, nil
	}

	s, err := strconv.Atoi(arr[0])
	if err != nil {
		return nil, err
	}

	res := map[int]float64{s: 0}

	if withscores {
		f, err := strconv.ParseFloat(arr[1], 64)
		if err != nil {
			return nil, err
		}
		res[s] = f
	}

	return res, nil
}

// ZRevRank works the same as ZRank but derives the member's rank based on ascending order of
// the members' scores.
func (server *EchoVault) ZRevRank(key string, member string, withscores bool) (map[int]float64, error) {
	cmd := []string{"ZREVRANK", key, member}
	if withscores {
		cmd = append(cmd, "WITHSCORES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)

	return buildIntegerScoreMap(arr, withscores)
}

// ZScore Returns the score of the member in the sorted set.
//
// Parameters:
//
// `key` - string - The keys to the sorted set.
//
// `member` - string - The member whose rank will be returned.
//
// Returns: An interface representing the score of the member. If the member does not exist in the sorted set, nil is
// returned. Otherwise, a float64 is returned.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZScore(key string, member string) (interface{}, error) {
	cmd := []string{"ZSCORE", key, member}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	isNil, err := internal.ParseNilResponse(b)
	if err != nil {
		return nil, err
	}

	if isNil {
		return nil, nil
	}

	score, err := internal.ParseFloatResponse(b)
	if err != nil {
		return 0, err
	}

	return score, nil
}

// ZRem Removes the listed members from the sorted set.
//
// Parameters:
//
// `key` - string - The keys to the sorted set.
//
// `members` - ...string - The members to remove.
//
// Returns: The number of elements that were successfully removed.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZRem(key string, members ...string) (int, error) {
	cmd := []string{"ZREM", key}
	for _, member := range members {
		cmd = append(cmd, member)
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// ZRemRangeByScore Removes the elements whose scores are in the range between min and max.
//
// Parameters:
//
// `key` - string - The keys to the sorted set.
//
// `min` - float64 - The minimum score boundary.
//
// `max` - float64 - The maximum score boundary.
//
// Returns: The number of elements that were successfully removed.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZRemRangeByScore(key string, min float64, max float64) (int, error) {
	cmd := []string{
		"ZREMRANGEBYSCORE",
		key,
		strconv.FormatFloat(min, 'f', -1, 64),
		strconv.FormatFloat(max, 'f', -1, 64),
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

// ZRemRangeByLex Removes the elements that are lexicographically between min and max.
//
// Parameters:
//
// `key` - string - The keys to the sorted set.
//
// `min` - string - The minimum lexicographic boundary.
//
// `max` - string - The maximum lexicographic boundary.
//
// Returns: The number of elements that were successfully removed.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZRemRangeByLex(key, min, max string) (int, error) {
	b, err := server.handleCommand(
		server.context, internal.EncodeCommand([]string{"ZREMRANGEBYLEX", key, min, max}),
		nil,
		false,
		true,
	)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// ZRemRangeByRank Removes the elements that are ranked between min and max.
//
// Parameters:
//
// `key` - string - The keys to the sorted set.
//
// `min` - int - The minimum rank boundary.
//
// `max` - int - The maximum rank boundary.
//
// Returns: The number of elements that were successfully removed.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZRemRangeByRank(key string, min, max int) (int, error) {
	b, err := server.handleCommand(
		server.context, internal.EncodeCommand([]string{"ZREMRANGEBYRANK", key, strconv.Itoa(min), strconv.Itoa(max)}),
		nil,
		false,
		true,
	)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// ZRange Returns the range of elements in the sorted set.
//
// Parameters:
//
// `key` - string - The keys to the sorted set.
//
// `start` - string - The minimum boundary.
//
// `stop` - string - The maximum boundary.
//
// `options` - ZRangeOptions
//
// Returns: A map of map[string]float64 where the key is the member and the value is its score.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZRange(key, start, stop string, options ZRangeOptions) (map[string]float64, error) {
	cmd := []string{"ZRANGE", key, start, stop}

	switch {
	case options.ByScore:
		cmd = append(cmd, "BYSCORE")
	case options.ByLex:
		cmd = append(cmd, "BYLEX")
	case options.Rev:
		cmd = append(cmd, "REV")
	default:
		cmd = append(cmd, "BYSCORE")
	}

	if options.WithScores {
		cmd = append(cmd, "WITHSCORES")
	}

	if options.Offset != 0 && options.Count != 0 {
		cmd = append(cmd, []string{"LIMIT", strconv.Itoa(int(options.Offset)), strconv.Itoa(int(options.Count))}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)
	if err != nil {
		return nil, err
	}

	return buildMemberScoreMap(arr, options.WithScores)
}

// ZRangeStore Works like ZRange but stores the result in at the 'destination' key.
//
// Parameters:
//
// `destination` - string - The key at which to store the new sorted set
//
// `key` - string - The keys to the sorted set.
//
// `start` - string - The minimum boundary.
//
// `stop` - string - The maximum boundary.
//
// `options` - ZRangeStoreOptions
//
// Returns: The cardinality of the new sorted set.
//
// Errors:
//
// "value at <key> is not a sorted set" - when a key exists but is not a sorted set.
func (server *EchoVault) ZRangeStore(destination, source, start, stop string, options ZRangeStoreOptions) (int, error) {
	cmd := []string{"ZRANGESTORE", destination, source, start, stop}

	switch {
	case options.ByScore:
		cmd = append(cmd, "BYSCORE")
	case options.ByLex:
		cmd = append(cmd, "BYLEX")
	case options.Rev:
		cmd = append(cmd, "REV")
	default:
		cmd = append(cmd, "BYSCORE")
	}

	if options.Offset != 0 && options.Count != 0 {
		cmd = append(cmd, []string{"LIMIT", strconv.Itoa(int(options.Offset)), strconv.Itoa(int(options.Count))}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false, true)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}
