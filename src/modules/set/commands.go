package set

import (
	"context"
	"errors"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
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
	case "sadd":
		return handleSADD(ctx, cmd, server)
	case "scard":
		return handleSCARD(ctx, cmd, server)
	case "sdiff":
		return handleSDIFF(ctx, cmd, server)
	case "sdiffstore":
		return handleSDIFFSTORE(ctx, cmd, server)
	case "sinter":
		return handleSINTERSTORE(ctx, cmd, server)
	case "sintercard":
		return handleSINTERCARD(ctx, cmd, server)
	case "sinterstore":
		return handleSINTERSTORE(ctx, cmd, server)
	case "sismember":
		return handleSISMEMBER(ctx, cmd, server)
	case "smembers":
		return handleSMEMBERS(ctx, cmd, server)
	case "smismember":
		return handleSMISMEMBER(ctx, cmd, server)
	case "smove":
		return handleSMOVE(ctx, cmd, server)
	case "spop":
		return handleSPOP(ctx, cmd, server)
	case "srandmember":
		return handleSRANDMEMBER(ctx, cmd, server)
	case "srem":
		return handleSREM(ctx, cmd, server)
	case "sunion":
		return handleSUNION(ctx, cmd, server)
	case "sunionstore":
		return handleSUNIONSTORE(ctx, cmd, server)
	}
}

func handleSADD(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SADD not implemented")
}

func handleSCARD(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SCARD not implemented")
}

func handleSDIFF(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SDIFF not implemented")
}

func handleSDIFFSTORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SDIFFSTORE not implemented")
}

func handleSINTER(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SINTER not implemented")
}

func handleSINTERCARD(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SINTERCARD not implemented")
}

func handleSINTERSTORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SINTERSTORE not implemented")
}

func handleSISMEMBER(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SISMEMBER not implemented")
}

func handleSMEMBERS(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SMEMBERS not implemented")
}

func handleSMISMEMBER(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SMISMEMBER not implemented")
}

func handleSMOVE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SMOVE not implemented")
}

func handleSPOP(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SPOP not implemented")
}

func handleSRANDMEMBER(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SRANDMEMBER not implemented")
}

func handleSREM(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SREM not implemented")
}

func handleSUNION(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SUNION not implemented")
}

func handleSUNIONSTORE(ctx context.Context, cmd []string, server utils.Server) ([]byte, error) {
	return nil, errors.New("SUNIONSTORE not implemented")
}

func NewModule() Plugin {
	return Plugin{
		name: "SetCommands",
		commands: []utils.Command{
			{
				Command:     "sadd",
				Categories:  []string{},
				Description: "(SADD key member [member...]) Add one or more members to the set.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
			},
			{
				Command:     "scard",
				Categories:  []string{},
				Description: "(SCARD key) Returns the cardinality of the set.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
			},
			{
				Command:     "sdiff",
				Categories:  []string{},
				Description: "(SDIFF key [key...]) Returns the difference between all the sets in the given keys.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
			},
			{
				Command:     "sdiffstore",
				Categories:  []string{},
				Description: "(SDIFFSTORE destination key [key...]) Stores the difference between all the sets at the destination key.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
			},
			{
				Command:     "sinter",
				Categories:  []string{},
				Description: "(SINTER key [key...]) Returns the intersection of multiple sets.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
			},
			{
				Command:     "sintercard",
				Categories:  []string{},
				Description: "(SINTERCARD key [key...]) Returns the cardinality of the intersection between multiple sets.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
			},
			{
				Command:     "sinterstore",
				Categories:  []string{},
				Description: "(SINTERSTORE destination key [key...]) Stores the intersection of multiple sets at the destination key.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
			},
			{
				Command:     "sismember",
				Categories:  []string{},
				Description: "(SISMEMBER key member) Returns if member is contained in the set.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
			},
			{
				Command:     "smembers",
				Categories:  []string{},
				Description: "(SMEMBERS key) Returns all members of a set.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
			},
			{
				Command:     "smismember",
				Categories:  []string{},
				Description: "(SMISMEMBER key member [member...]) Returns if multiple members are in the set.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
			},

			{
				Command:     "smove",
				Categories:  []string{},
				Description: "(SMOVE source destination member) Moves a member from source set to destination set.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:3], nil
				},
			},
			{
				Command:     "spop",
				Categories:  []string{},
				Description: "(SPOP key [count]) Returns and removes one or more random members from the set.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
			},
			{
				Command:     "srandmember",
				Categories:  []string{},
				Description: "(SRANDMEMBER key [count]) Returns one or more random members from the set without removing them.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
			},
			{
				Command:     "srem",
				Categories:  []string{},
				Description: "(SREM key member [member...]) Remove one or more members from a set.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return []string{cmd[1]}, nil
				},
			},
			{
				Command:     "sunion",
				Categories:  []string{},
				Description: "(SUNION key [key...]) Returns the members of the set resulting from the union of the provided sets.",
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
			},
			{
				Command:     "sunionstore",
				Categories:  []string{},
				Description: "(SUNIONSTORE destination key [key...]) Stores the union of the given sets into destination.",
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
			},
		},
		description: "Handle commands for sets",
	}
}
