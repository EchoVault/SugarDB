package hash

import (
	"context"
	"errors"
	"github.com/kelvinmwinuka/memstore/src/utils"
	"net"
)

type Plugin struct {
	name        string
	commands    []utils.Command
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

func NewModule() Plugin {
	SetModule := Plugin{
		name: "HashCommands",
		commands: []utils.Command{
			{
				Command:     "hset",
				Categories:  []string{utils.HashCategory, utils.WriteCategory, utils.FastCategory},
				Description: `(HSET key field value [field value ...]) Set update each field of the hash with the corresponding value`,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
					return nil, errors.New("hset command not implemented")
				},
			},
			{
				Command:     "hsetnx",
				Categories:  []string{utils.HashCategory, utils.WriteCategory, utils.FastCategory},
				Description: `(HSETNX key field value) Set hash field value only if the field does not exist`,
				Sync:        true,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
					return nil, errors.New("hsetnx command not implemented")
				},
			},
			{
				Command:     "hget",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.FastCategory},
				Description: `(HGET key field [field ...]) Retrieve the of each of the listed fields from the hash`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
					return nil, errors.New("hget command not implemented")
				},
			},
			{
				Command:    "hstrlen",
				Categories: []string{utils.HashCategory, utils.ReadCategory, utils.FastCategory},
				Description: `(HSTRLEN key field) Return the string length of the value stored at field.
			0 if the value does not exist`,
				Sync: false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 3 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:2], nil
				},
				HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
					return nil, errors.New("hstrlen command not implemented")
				},
			},
			{
				Command:     "hvals",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.SlowCategory},
				Description: `(HVALS key) Returns all the values of the hash at key.`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
					return nil, errors.New("hvals command not implemented")
				},
			},
			{
				Command:     "hrandfield",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.SlowCategory},
				Description: `(HRANDFIELD key [count] [WITHVALUES]) Returns one or more random fields from the hash`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) < 2 || len(cmd) > 4 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
					return nil, errors.New("hrandfield command not implemented")
				},
			},
			{
				Command:     "hlen",
				Categories:  []string{utils.HashCategory, utils.ReadCategory, utils.FastCategory},
				Description: `(HLEN key) Returns the number of fields in the hash`,
				Sync:        false,
				KeyExtractionFunc: func(cmd []string) ([]string, error) {
					if len(cmd) != 2 {
						return nil, errors.New(utils.WRONG_ARGS_RESPONSE)
					}
					return cmd[1:], nil
				},
				HandlerFunc: func(ctx context.Context, cmd []string, server utils.Server, conn *net.Conn) ([]byte, error) {
					return nil, errors.New("command not implemented")
				},
			},
		},
		description: "Handle HASH commands",
	}

	return SetModule
}
