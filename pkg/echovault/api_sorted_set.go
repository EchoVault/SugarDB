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

type Member string
type Score float64

type ZADDOptions struct {
	NX   bool
	XX   bool
	GT   bool
	LT   bool
	CH   bool
	INCR bool
}

type ZINTEROptions struct {
	Weights    []Score
	Aggregate  string
	WithScores bool
}
type ZINTERSTOREOptions ZINTEROptions
type ZUNIONOptions ZINTEROptions
type ZUNIONSTOREOptions ZINTEROptions

type ZMPOPOptions struct {
	Min   bool
	Max   bool
	Count int
}

type ZRANGEOptions struct {
	ByScore bool
	ByLex   bool
	Rev     bool
	Offset  int
	Count   int
}
type ZRANGESTOREOptions ZRANGEOptions

func buildMemberScoreMap(arr [][]string, withscores bool) (map[Member]Score, error) {
	result := make(map[Member]Score, len(arr))
	for _, entry := range arr {
		if withscores {
			score, err := strconv.ParseFloat(entry[1], 64)
			if err != nil {
				return nil, err
			}
			result[Member(entry[0])] = Score(score)
			continue
		}
		result[Member(entry[0])] = Score(0)
	}
	return result, nil
}

func buildIntegerScoreMap(arr [][]string, withscores bool) (map[int]Score, error) {
	result := make(map[int]Score, len(arr))
	for _, entry := range arr {
		rank, err := strconv.Atoi(entry[0])
		if err != nil {
			return nil, err
		}
		result[rank] = Score(0)
		if withscores {
			score, err := strconv.ParseFloat(entry[1], 64)
			if err != nil {
				return nil, err
			}
			result[rank] = Score(score)
		}
	}
	return result, nil
}

func (server *EchoVault) ZADD(entries map[Member]Score, options ZADDOptions) (int, error) {
	cmd := []string{"ZADD"}

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

	for member, score := range entries {
		cmd = append(cmd, []string{string(member), strconv.FormatFloat(float64(score), 'f', -1, 64)}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) ZCARD(key string) (int, error) {
	b, err := server.handleCommand(server.context, internal.EncodeCommand([]string{"ZCARD", key}), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) ZCOUNT(key string, min, max Score) (int, error) {
	cmd := []string{
		"ZCOUNT",
		key,
		strconv.FormatFloat(float64(min), 'f', -1, 64),
		strconv.FormatFloat(float64(max), 'f', -1, 64),
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) ZDIFF(withscores bool, keys ...string) (map[Member]Score, error) {
	cmd := append([]string{"ZDIFF"}, keys...)
	if withscores {
		cmd = append(cmd, "WITHSCORES")
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)
	if err != nil {
		return nil, err
	}

	return buildMemberScoreMap(arr, withscores)
}

func (server *EchoVault) ZDIFFSTORE(destination string, keys ...string) (int, error) {
	cmd := append([]string{"ZDIFFSTORE", destination}, keys...)
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) ZINCRBY(key string, increment Score, member Member) (Score, error) {
	cmd := []string{"ZINCRBY", key, strconv.FormatFloat(float64(increment), 'f', -1, 64), string(member)}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	f, err := internal.ParseFloatResponse(b)
	if err != nil {
		return 0, err
	}
	return Score(f), nil
}

func (server *EchoVault) ZINTER(keys []string, options ZINTEROptions) (map[Member]Score, error) {
	cmd := append([]string{"ZINTER"}, keys...)

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

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)
	if err != nil {
		return nil, err
	}

	return buildMemberScoreMap(arr, options.WithScores)
}

func (server *EchoVault) ZINTERSTORE(destination string, keys []string, options ZINTERSTOREOptions) (int, error) {
	cmd := append([]string{"ZINTERSTORE", destination}, keys...)

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

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) ZUNION(keys []string, options ZUNIONOptions) (map[Member]Score, error) {
	cmd := append([]string{"ZUNION"}, keys...)

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

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)
	if err != nil {
		return nil, err
	}

	return buildMemberScoreMap(arr, options.WithScores)
}

func (server *EchoVault) ZUNIONSTORE(destination string, keys []string, options ZUNIONSTOREOptions) (int, error) {
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

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) ZMPOP(keys []string, options ZMPOPOptions) ([][]string, error) {
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
		cmd = append(cmd, []string{"COUNT", strconv.Itoa(options.Count)}...)
	default:
		cmd = append(cmd, []string{"COUNT", strconv.Itoa(1)}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	return internal.ParseNestedStringArrayResponse(b)
}

func (server *EchoVault) ZMSCORE(key string, members ...Member) ([]Score, error) {
	cmd := []string{"ZMSCORE", key}
	for _, member := range members {
		cmd = append(cmd, string(member))
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseStringArrayResponse(b)
	if err != nil {
		return nil, err
	}

	scores := make([]Score, len(arr))
	for i, e := range arr {
		score, err := strconv.ParseFloat(e, 64)
		if err != nil {
			return nil, err
		}
		scores[i] = Score(score)
	}

	return scores, nil
}

func (server *EchoVault) ZPOPMAX(key string, count int) ([][]string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"ZPOPMAX", key, strconv.Itoa(count)}),
		nil,
		false,
	)
	if err != nil {
		return nil, err
	}
	return internal.ParseNestedStringArrayResponse(b)
}

func (server *EchoVault) ZPOPMIN(key string, count int) ([][]string, error) {
	b, err := server.handleCommand(
		server.context,
		internal.EncodeCommand([]string{"ZPOPMIN", key, strconv.Itoa(count)}),
		nil,
		false,
	)
	if err != nil {
		return nil, err
	}
	return internal.ParseNestedStringArrayResponse(b)
}

func (server *EchoVault) ZRANDMEMBER(key string, count int, withscores bool) (map[Member]Score, error) {
	cmd := []string{"ZRANDMEMBER", key}
	if count != 0 {
		cmd = append(cmd, strconv.Itoa(count))
	}
	if withscores {
		cmd = append(cmd, "WITHSCORES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)
	if err != nil {
		return nil, err
	}

	return buildMemberScoreMap(arr, withscores)
}

func (server *EchoVault) ZRANK(key string, member Member, withscores bool) (map[int]Score, error) {
	cmd := []string{"ZRANK", key, string(member)}
	if withscores {
		cmd = append(cmd, "WITHSCORES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)

	return buildIntegerScoreMap(arr, withscores)
}

func (server *EchoVault) ZREVRANK(key string, member Member, withscores bool) (map[int]Score, error) {
	cmd := []string{"ZREVRANK", key, string(member)}
	if withscores {
		cmd = append(cmd, "WITHSCORES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)

	return buildIntegerScoreMap(arr, withscores)
}

func (server *EchoVault) ZREM(key string, members ...Member) (int, error) {
	cmd := []string{"ZREM", key}
	for _, member := range members {
		cmd = append(cmd, string(member))
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

// TODO: Look into returning nil here when member does not exist in the sorted set
func (server *EchoVault) ZSCORE(key string, member Member) (Score, error) {
	cmd := []string{"ZSCORE", key, string(member)}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	score, err := internal.ParseFloatResponse(b)
	if err != nil {
		return 0, err
	}

	return Score(score), nil
}

func (server *EchoVault) ZREMRANGEBYSCORE(key string, min Score, max Score) (int, error) {
	cmd := []string{
		"ZREMRANGEBYSCORE",
		key,
		strconv.FormatFloat(float64(min), 'f', -1, 64),
		strconv.FormatFloat(float64(max), 'f', -1, 64),
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) ZLEXCOUNT(key, min, max string) (int, error) {
	cmd := []string{"ZLEXCOUNT", key, min, max}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) ZRANGE(key, start, stop string, options ZRANGEOptions) (map[Member]Score, error) {
	cmd := []string{"ZRANGE", key, start, stop}

	switch {
	case options.ByScore:
		cmd = append(cmd, "BYSCORE")
	case options.ByLex:
		cmd = append(cmd, "BYLEX")
	default:
		cmd = append(cmd, "BYSCORE")
	}

	if options.Rev {
		cmd = append(cmd, "REV")
	}

	cmd = append(cmd, []string{"LIMIT", strconv.Itoa(options.Offset), strconv.Itoa(options.Count)}...)

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	arr, err := internal.ParseNestedStringArrayResponse(b)
	if err != nil {
		return nil, err
	}

	return buildMemberScoreMap(arr, true)
}

func (server *EchoVault) ZRANGESTORE(destination, source, start, stop string, options ZRANGESTOREOptions) (int, error) {
	cmd := []string{"ZRANGESTORE", destination, source, start, stop}

	switch {
	case options.ByScore:
		cmd = append(cmd, "BYSCORE")
	case options.ByLex:
		cmd = append(cmd, "BYLEX")
	default:
		cmd = append(cmd, "BYSCORE")
	}

	if options.Rev {
		cmd = append(cmd, "REV")
	}

	cmd = append(cmd, []string{"LIMIT", strconv.Itoa(options.Offset), strconv.Itoa(options.Count)}...)

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}
