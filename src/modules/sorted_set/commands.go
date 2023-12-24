package sorted_set

import (
	"context"
	"errors"
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
		return handleZDIFFSTORE(ctx, cmd, server)
	case "zincrby":
		return handleZINCRBY(ctx, cmd, server)
	case "zinter":
		return handleZINTER(ctx, cmd, server)
	case "zinterstore":
		return handleZINTERSTORE(ctx, cmd, server)
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
	case "zrangebylex":
		return handleZRANGEBYLEX(ctx, cmd, server)
	case "zrangebyscore":
		return handleZRANGEBYSCORE(ctx, cmd, server)
	case "zrangestore":
		return handleZRANGESTORE(ctx, cmd, server)
	case "zunion":
		return handleZUNION(ctx, cmd, server)
	case "zunionstore":
		return handleZUNIONSCORE(ctx, cmd, server)
	}
}

func handleZADD(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZADD not implemented")
}

func handleZCARD(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZCARD not implemented")
}

func handleZCOUNT(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZCOUNT not implemented")
}

func handleZDIFF(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZDIFF not implemented")
}

func handleZDIFFSTORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZDIFFSTORE not implemented")
}

func handleZINCRBY(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZINCRBY not implemented")
}

func handleZINTER(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZINTER not implemented")
}

func handleZINTERSTORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZINTERSTORE not implemented")
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
	return nil, errors.New("ZMSCORE not implemented")
}

func handleZRANDMEMBER(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZRANDMEMBER not implemented")
}

func handleZRANK(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZRANK not implemented")
}

func handleZREM(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZREM not implemented")
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

func handleZRANGEBYLEX(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZRANGEBYLEX not implemented")
}

func handleZRANGEBYSCORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZRANGEBYSCORE not implemented")
}

func handleZRANGESTORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZRANGESTORE not implemented")
}

func handleZUNION(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZUNION not implemented")
}

func handleZUNIONSCORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("ZUNIONSTORE not implemented")
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
