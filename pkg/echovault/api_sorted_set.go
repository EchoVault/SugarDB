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

type ZADDOptions struct {
	NX   bool
	XX   bool
	GT   bool
	LT   bool
	CH   bool
	INCR bool
}

type ZINTEROptions struct {
	Weights    []float64
	Aggregate  string
	WithScores bool
}
type ZINTERSTOREOptions ZINTEROptions
type ZUNIONOptions ZINTEROptions
type ZUNIONSTOREOptions ZINTEROptions

type ZMPOPOptions struct {
	Min   bool
	Max   bool
	Count uint
}

type ZRANGEOptions struct {
	WithScores bool
	ByScore    bool
	ByLex      bool
	Offset     uint
	Count      uint
}
type ZRANGESTOREOptions ZRANGEOptions

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

func (server *EchoVault) ZADD(key string, entries map[string]float64, options ZADDOptions) (int, error) {
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

	for member, score := range entries {
		cmd = append(cmd, []string{strconv.FormatFloat(score, 'f', -1, 64), member}...)
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

func (server *EchoVault) ZCOUNT(key string, min, max float64) (int, error) {
	cmd := []string{
		"ZCOUNT",
		key,
		strconv.FormatFloat(min, 'f', -1, 64),
		strconv.FormatFloat(max, 'f', -1, 64),
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) ZDIFF(withscores bool, keys ...string) (map[string]float64, error) {
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

func (server *EchoVault) ZINCRBY(key string, increment float64, member string) (float64, error) {
	cmd := []string{"ZINCRBY", key, strconv.FormatFloat(increment, 'f', -1, 64), member}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	f, err := internal.ParseFloatResponse(b)
	if err != nil {
		return 0, err
	}
	return f, nil
}

func (server *EchoVault) ZINTER(keys []string, options ZINTEROptions) (map[string]float64, error) {
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

func (server *EchoVault) ZUNION(keys []string, options ZUNIONOptions) (map[string]float64, error) {
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
		cmd = append(cmd, []string{"COUNT", strconv.Itoa(int(options.Count))}...)
	default:
		cmd = append(cmd, []string{"COUNT", strconv.Itoa(1)}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return nil, err
	}

	return internal.ParseNestedStringArrayResponse(b)
}

func (server *EchoVault) ZMSCORE(key string, members ...string) ([]interface{}, error) {
	cmd := []string{"ZMSCORE", key}
	for _, member := range members {
		cmd = append(cmd, member)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
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

func (server *EchoVault) ZRANDMEMBER(key string, count int, withscores bool) ([][]string, error) {
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

	return internal.ParseNestedStringArrayResponse(b)
}

func (server *EchoVault) ZRANK(key string, member string, withscores bool) (map[int]float64, error) {
	cmd := []string{"ZRANK", key, member}
	if withscores {
		cmd = append(cmd, "WITHSCORES")
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
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

func (server *EchoVault) ZREVRANK(key string, member string, withscores bool) (map[int]float64, error) {
	cmd := []string{"ZREVRANK", key, member}
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

func (server *EchoVault) ZREM(key string, members ...string) (int, error) {
	cmd := []string{"ZREM", key}
	for _, member := range members {
		cmd = append(cmd, member)
	}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}
	return internal.ParseIntegerResponse(b)
}

func (server *EchoVault) ZSCORE(key string, member string) (interface{}, error) {
	cmd := []string{"ZSCORE", key, member}
	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
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

func (server *EchoVault) ZREMRANGEBYSCORE(key string, min float64, max float64) (int, error) {
	cmd := []string{
		"ZREMRANGEBYSCORE",
		key,
		strconv.FormatFloat(min, 'f', -1, 64),
		strconv.FormatFloat(max, 'f', -1, 64),
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

func (server *EchoVault) ZRANGE(key, start, stop string, options ZRANGEOptions) (map[string]float64, error) {
	cmd := []string{"ZRANGE", key, start, stop}

	switch {
	case options.ByScore:
		cmd = append(cmd, "BYSCORE")
	case options.ByLex:
		cmd = append(cmd, "BYLEX")
	default:
		cmd = append(cmd, "BYSCORE")
	}

	if options.WithScores {
		cmd = append(cmd, "WITHSCORES")
	}

	if options.Offset != 0 && options.Count != 0 {
		cmd = append(cmd, []string{"LIMIT", strconv.Itoa(int(options.Offset)), strconv.Itoa(int(options.Count))}...)
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

	if options.Offset != 0 && options.Count != 0 {
		cmd = append(cmd, []string{"LIMIT", strconv.Itoa(int(options.Offset)), strconv.Itoa(int(options.Count))}...)
	}

	b, err := server.handleCommand(server.context, internal.EncodeCommand(cmd), nil, false)
	if err != nil {
		return 0, err
	}

	return internal.ParseIntegerResponse(b)
}
