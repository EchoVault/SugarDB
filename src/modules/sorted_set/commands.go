package sorted_set

import (
	"context"
	"errors"
	"fmt"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"math"
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

func (p Plugin) HandleCommand(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
	switch strings.ToLower(cmd[0]) {
	default:
		return nil, errors.New("command unknown")
	case "zadd":
		return handleZADD(ctx, cmd, server)
	case "zcard":
		return handleZCARD(ctx, cmd, server)
	case "zcount":
		return handleZCOUNT(ctx, cmd, server)
	case "zdiff":
		return handleZDIFF(ctx, cmd, server)
	case "zdiffstore":
		return handleZDIFF(ctx, cmd, server)
	case "zincrby":
		return handleZINCRBY(ctx, cmd, server)
	case "zinter":
		return handleZINTER(ctx, cmd, server)
	case "zinterstore":
		return handleZINTER(ctx, cmd, server)
	case "zmpop":
		return handleZMPOP(ctx, cmd, server)
	case "zmpopmax":
		return handleZMPOPMAX(ctx, cmd, server)
	case "zmpopmin":
		return handleZMPOPMIN(ctx, cmd, server)
	case "zmscore":
		return handleZMSCORE(ctx, cmd, server)
	case "zscore":
		return handleZSCORE(ctx, cmd, server)
	case "zrank":
		return handleZRANK(ctx, cmd, server)
	case "zrevrank":
		return handleZREVRANK(ctx, cmd, server)
	case "zrem":
		return handleZREM(ctx, cmd, server)
	case "zrandmember":
		return handleZRANDMEMBER(ctx, cmd, server)
	case "zremrangebylex":
		return handleZREMRANGEBYLEX(ctx, cmd, server)
	case "zremrangebyscore":
		return handleZREMRANGEBYSCORE(ctx, cmd, server)
	case "zremrangebyrank":
		return handleZREMRANGEBYRANK(ctx, cmd, server)
	case "zrange":
		return handleZRANGE(ctx, cmd, server)
	case "zrangestore":
		return handleZRANGESTORE(ctx, cmd, server)
	case "zunion":
		return handleZUNION(ctx, cmd, server)
	case "zunionstore":
		return handleZUNION(ctx, cmd, server)
	}
}

func handleZADD(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if len(cmd) < 4 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]

	var updatePolicy interface{} = nil
	var comparison interface{} = nil
	var changed interface{} = nil
	var incr interface{} = nil

	// Find the first valid score and this will be the start of the score/member pairs
	var membersStartIndex int
	for i := 0; i < len(cmd); i++ {
		if membersStartIndex != 0 {
			break
		}
		switch utils.AdaptType(cmd[i]).(type) {
		case string:
			if utils.Contains([]string{"-inf", "+inf"}, strings.ToLower(cmd[i])) {
				membersStartIndex = i
			}
		case float64:
			membersStartIndex = i
		case int:
			membersStartIndex = i
		}
	}

	if membersStartIndex < 2 || len(cmd[membersStartIndex:])%2 != 0 {
		return nil, errors.New("score/member pairs must be float/string")
	}

	var members []MemberParam

	for i := 0; i < len(cmd[membersStartIndex:]); i++ {
		if i%2 != 0 {
			continue
		}
		score := utils.AdaptType(cmd[membersStartIndex:][i])
		switch score.(type) {
		default:
			return nil, errors.New("invalid score in score/member list")
		case string:
			var s float64
			if strings.ToLower(score.(string)) == "-inf" {
				s = math.Inf(-1)
				members = append(members, MemberParam{
					value: Value(cmd[membersStartIndex:][i+1]),
					score: Score(s),
				})
			}
			if strings.ToLower(score.(string)) == "+inf" {
				s = math.Inf(1)
				members = append(members, MemberParam{
					value: Value(cmd[membersStartIndex:][i+1]),
					score: Score(s),
				})
			}
		case float64:
			s, _ := score.(float64)
			members = append(members, MemberParam{
				value: Value(cmd[membersStartIndex:][i+1]),
				score: Score(s),
			})
		case int:
			s, _ := score.(int)
			members = append(members, MemberParam{
				value: Value(cmd[membersStartIndex:][i+1]),
				score: Score(s),
			})
		}
	}

	// Parse options using membersStartIndex as the upper limit
	if membersStartIndex > 2 {
		options := cmd[2:membersStartIndex]
		for _, option := range options {
			if utils.Contains([]string{"xx", "nx"}, strings.ToLower(option)) {
				updatePolicy = option
				continue
			}
			if utils.Contains([]string{"gt", "lt"}, strings.ToLower(option)) {
				comparison = option
				continue
			}
			if strings.EqualFold(option, "ch") {
				changed = option
				continue
			}
			if strings.EqualFold(option, "incr") {
				incr = option
				continue
			}
			return nil, fmt.Errorf("invalid option %s", option)
		}
	}

	if server.KeyExists(key) {
		// Key exists
		_, err := server.KeyLock(ctx, key)
		if err != nil {
			return nil, err
		}
		defer server.KeyUnlock(key)
		set, ok := server.GetValue(key).(*SortedSet)
		if !ok {
			return nil, fmt.Errorf("value at %s is not a sorted set")
		}
		count, err := set.AddOrUpdate(members, updatePolicy, comparison, changed, incr)
		if err != nil {
			return nil, err
		}
		// If INCR option is provided, return the new score value
		if incr != nil {
			m := set.Get(members[0].value)
			return []byte(fmt.Sprintf("+%f\r\n\r\n", m.score)), nil
		}

		return []byte(fmt.Sprintf(":%d\r\n\r\n", count)), nil
	}

	// Key does not exist
	_, err := server.CreateKeyAndLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	set := NewSortedSet(members)
	server.SetValue(ctx, key, set)

	return []byte(fmt.Sprintf(":%d\r\n\r\n", set.Cardinality())), nil
}

func handleZCARD(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
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

	set, ok := server.GetValue(key).(*SortedSet)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a sorted set", key)
	}

	return []byte(fmt.Sprintf(":%d\r\n\r\n", set.Cardinality())), nil
}

func handleZCOUNT(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]

	minimum := Score(math.Inf(-1))
	switch utils.AdaptType(cmd[2]).(type) {
	default:
		return nil, errors.New("min constraint must be a double")
	case string:
		if strings.ToLower(cmd[2]) == "+inf" {
			minimum = Score(math.Inf(1))
		} else {
			return nil, errors.New("min constraint must be a double")
		}
	case float64:
		s, _ := utils.AdaptType(cmd[2]).(float64)
		minimum = Score(s)
	case int:
		s, _ := utils.AdaptType(cmd[2]).(int)
		minimum = Score(s)
	}

	maximum := Score(math.Inf(1))
	switch utils.AdaptType(cmd[3]).(type) {
	default:
		return nil, errors.New("max constraint must be a double")
	case string:
		if strings.ToLower(cmd[3]) == "-inf" {
			maximum = Score(math.Inf(-1))
		} else {
			return nil, errors.New("max constraint must be a double")
		}
	case float64:
		s, _ := utils.AdaptType(cmd[3]).(float64)
		maximum = Score(s)
	case int:
		s, _ := utils.AdaptType(cmd[3]).(int)
		maximum = Score(s)
	}

	if !server.KeyExists(key) {
		return []byte("*0\r\n\r\n"), nil
	}

	_, err := server.KeyRLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyRUnlock(key)

	set, ok := server.GetValue(key).(*SortedSet)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a sorted set", key)
	}

	var members []MemberParam
	for _, m := range set.GetAll() {
		if m.score >= minimum && m.score <= maximum {
			members = append(members, m)
		}
	}

	return []byte(fmt.Sprintf(":%d\r\n\r\n", len(members))), nil
}

func handleZDIFF(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZDIFF not implemented")
}

func handleZINCRBY(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if len(cmd) != 4 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]
	member := Value(cmd[3])
	var increment Score

	switch utils.AdaptType(cmd[2]).(type) {
	default:
		return nil, errors.New("increment must be a double")
	case string:
		if strings.EqualFold("-inf", strings.ToLower(cmd[2])) {
			increment = Score(math.Inf(-1))
		} else if strings.EqualFold("+inf", strings.ToLower(cmd[2])) {
			increment = Score(math.Inf(1))
		} else {
			return nil, errors.New("increment must be a double")
		}
	case float64:
		s, _ := utils.AdaptType(cmd[2]).(float64)
		increment = Score(s)
	case int:
		s, _ := utils.AdaptType(cmd[2]).(int)
		increment = Score(s)
	}

	if server.KeyExists(key) {
		_, err := server.KeyLock(ctx, key)
		if err != nil {
			return nil, err
		}
		defer server.KeyUnlock(key)
		set, ok := server.GetValue(key).(*SortedSet)
		if !ok {
			return nil, fmt.Errorf("value at %s is not a sorted set", key)
		}
		_, err = set.AddOrUpdate(
			[]MemberParam{{value: member, score: increment}}, "xx", nil, nil, "incr")
		if err != nil {
			return nil, err
		}
		return []byte(fmt.Sprintf("+%f\r\n\r\n", set.Get(member).score)), nil
	}

	_, err := server.CreateKeyAndLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	set := NewSortedSet([]MemberParam{
		{
			value: member,
			score: increment,
		},
	})
	server.SetValue(ctx, key, set)

	return []byte(fmt.Sprintf("+%f\r\n\r\n", set.Get(member).score)), nil
}

func handleZINTER(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZINTER not implemented")
}

func handleZMPOP(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZMPOP not implemented")
}

func handleZMPOPMAX(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZMPOPMAX not implemented")
}

func handleZMPOPMIN(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZMPOPMIN not implemented")
}

func handleZMSCORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if len(cmd) < 3 {
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

	set, ok := server.GetValue(key).(*SortedSet)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a sorted set", key)
	}

	res := fmt.Sprintf("*%d", len(cmd[2:]))
	var member MemberObject
	for i, m := range cmd[2:] {
		member = set.Get(Value(m))
		if !member.exists {
			res = fmt.Sprintf("%s\r\n+(nil)", res)
		} else {
			res = fmt.Sprintf("%s\r\n+%f", res, member.score)
		}
		if i == len(cmd[2:])-1 {
			res += "\r\n\r\n"
		}
	}

	return []byte(res), nil
}

func handleZRANDMEMBER(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZRANDMEMBER not implemented")
}

func handleZRANK(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZRANK not implemented")
}

func handleZREM(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	if len(cmd) < 3 {
		return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
	}

	key := cmd[1]

	if !server.KeyExists(key) {
		return []byte(":0\r\n\r\n"), nil
	}

	_, err := server.KeyLock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer server.KeyUnlock(key)

	set, ok := server.GetValue(key).(*SortedSet)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a sorted set", key)
	}

	deletedCount := 0
	for _, m := range cmd[2:] {
		if set.Remove(Value(m)) {
			deletedCount += 1
		}
	}

	return []byte(fmt.Sprintf(":%d\r\n\r\n", deletedCount)), nil
}

func handleZREVRANK(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZREVRANK not implemented")
}

func handleZSCORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZSCORE not implemented")
}

func handleZREMRANGEBYLEX(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZREMRANGEBYLEX not implemented")
}

func handleZREMRANGEBYSCORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZREMRANGEBYSCORE not implemented")
}

func handleZREMRANGEBYRANK(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZREMRANGEBYRANK not implemented")
}

func handleZRANGE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZRANGE not implemented")
}

func handleZRANGESTORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZRANGESTORE not implemented")
}

func handleZUNION(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZUNION not implemented")
}

func NewModule() Plugin {
	return Plugin{
		name: "SortedSetCommand",
		commands: []utils.Command{
			{
				Command:    "zadd",
				Categories: []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: `(ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member...])
Adds all the specified members with the specified scores to the sorted set at the key`,
				Sync: true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
			},
			{
				Command:     "zcard",
				Categories:  []string{utils.SortedSetCategory, utils.ReadCategory},
				Description: `(ZCARD key) Returns the set cardinality of the sorted set at key.`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
			},
			{
				Command:    "zcount",
				Categories: []string{utils.SortedSetCategory, utils.ReadCategory},
				Description: `(ZCOUNT key min max) 
Returns the number of elements in the sorted set key with scores in the range of min and max.`,
				Sync: false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
			},
			{
				Command:    "zdiff",
				Categories: []string{utils.SortedSetCategory, utils.ReadCategory},
				Description: `(ZDIFF key [key...] [WITHSCORES]) 
Computes the difference between all the sorted sets specifies in the list of keys and returns the result.`,
				Sync: false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					keys := utils.Filter(cmd[1:], func(elem string) bool {
						return !strings.EqualFold(elem, "WITHSCORES")
					})
					return keys, nil
				},
			},
			{
				Command:    "zdiffstore",
				Categories: []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: `(ZDIFFSTORE destination key [key...]). 
Computes the difference between all the sorted sets specifies in the list of keys. Stores the result in destination.`,
				Sync: true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[2:], nil
				},
			},
			{
				Command:    "zincrby",
				Categories: []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: `(ZINCRBY key increment member). 
Increments the score of the specified sorted set's member by the increment. If the member does not exist, it is created.
If the key does not exist, it is created with new sorted set and the member added with the increment as its score.`,
				Sync: true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
			},
			{
				Command:    "zinter",
				Categories: []string{utils.SortedSetCategory, utils.ReadCategory},
				Description: `(ZINTER key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]).
Computes the intersection of the sets in the keys, with weights, aggregate and scores`,
				Sync: false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					endIdx := slices.IndexFunc(cmd[1:], func(s string) bool {
						if strings.EqualFold(s, "WEIGHTS") ||
							strings.EqualFold(s, "AGGREGATE") ||
							strings.EqualFold(s, "WITHSCORES") {
							return true
						}
						return false
					})
					if endIdx == -1 {
						return cmd[1:], nil
					}
					if endIdx >= 2 {
						return cmd[1:endIdx], nil
					}
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
				},
			},
			{
				Command:    "zinterstore",
				Categories: []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: `
(ZINTERSTORE destination key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]).
Computes the intersection of the sets in the keys, with weights, aggregate and scores. The result is stored in destination.`,
				Sync: true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					endIdx := slices.IndexFunc(cmd[1:], func(s string) bool {
						if strings.EqualFold(s, "WEIGHTS") ||
							strings.EqualFold(s, "AGGREGATE") ||
							strings.EqualFold(s, "WITHSCORES") {
							return true
						}
						return false
					})
					if endIdx == -1 {
						return cmd[1:], nil
					}
					if endIdx >= 2 {
						return cmd[1:endIdx], nil
					}
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
				},
			},
			{
				Command:    "zmpop",
				Categories: []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: `(ZMPOP key [key ...] <MIN | MAX> [COUNT count])
Pop a 'count' elements from sorted set. MIN or MAX determines whether to pop elements with the lowest or highest scores
respectively.`,
				Sync: true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					endIdx := slices.IndexFunc(cmd, func(s string) bool {
						return utils.Contains([]string{"MIN", "MAX", "COUNT"}, strings.ToUpper(s))
					})
					if endIdx == -1 {
						return cmd[1:], nil
					}
					if endIdx >= 2 {
						return cmd[1:endIdx], nil
					}
					return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
				},
			},
			{
				Command:    "zmscore",
				Categories: []string{utils.SortedSetCategory, utils.ReadCategory},
				Description: `(ZMSCORE key member [member ...])
Returns the associated scores of the specified member in the sorted set. 
Returns nil for members that do not exist in the set`,
				Sync: false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
			},
			{
				Command:    "zpopmax",
				Categories: []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: `(ZPOPMAX key [count])
Removes and returns 'count' number of members in the sorted set with the highest scores. Default count is 1.`,
				Sync: true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
			},
			{
				Command:    "zpopmin",
				Categories: []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: `(ZPOPMIN key [count])
Removes and returns 'count' number of members in the sorted set with the lowest scores. Default count is 1.`,
				Sync: true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
			},
			{
				Command:    "zrandmember",
				Categories: []string{utils.SortedSetCategory, utils.ReadCategory},
				Description: `(ZRANDMEMBER key [count [WITHSCORES]])
Return a list of length equivalent to count containing random members of the sorted set.
If count is negative, repeated elements are allowed. If count is positive, the returned elements will be distinct.
WITHSCORES modifies the result to include scores in the result.`,
				Sync: false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
			},
			{
				Command:    "zrank",
				Categories: []string{utils.SortedSetCategory, utils.ReadCategory},
				Description: `(ZRANK key member [WITHSCORE])
Returns the rank of the specified member in the sorted set. WITHSCORE modifies the result to also return the score.`,
				Sync: false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
			},
			{
				Command:     "zrem",
				Categories:  []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: `(ZREM key member [member ...]) Removes the listed members from the sorted set.`,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
			},
			{
				Command:    "zrevrank",
				Categories: []string{utils.SortedSetCategory, utils.ReadCategory},
				Description: `(ZREVRANK key member [WITHSCORE])
Returns the rank of the member in the sorted set. WITHSCORE modifies the result to include the score.`,
				Sync: false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
			},
			{
				Command:     "zscore",
				Categories:  []string{utils.SortedSetCategory, utils.ReadCategory, utils.FastCategory},
				Description: `(ZSCORE key member) Returns the score of the member in the sorted set.`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
			},
			{
				Command:     "zremrangebylex",
				Categories:  []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: ``,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
			{
				Command:     "zremrangebyrank",
				Categories:  []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: ``,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
			{
				Command:     "zremrangebyscore",
				Categories:  []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: ``,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
			{
				Command:     "zlexcount",
				Categories:  []string{},
				Description: ``,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
			{
				Command:     "zrange",
				Categories:  []string{},
				Description: ``,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
			{
				Command:     "zrangebylex",
				Categories:  []string{},
				Description: ``,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
			{
				Command:     "zrangebyscore",
				Categories:  []string{},
				Description: ``,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
			{
				Command:     "zrangestore",
				Categories:  []string{},
				Description: ``,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
			{
				Command:     "zunion",
				Categories:  []string{utils.SortedSetCategory, utils.ReadCategory},
				Description: ``,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
			{
				Command:     "zunionstore",
				Categories:  []string{utils.SortedSetCategory, utils.WriteCategory},
				Description: ``,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					return []string{}, nil
				},
			},
		},
		description: "Handle commands on sorted set data type",
	}
}
