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

package admin

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/echovault/pkg/constants"
	"github.com/echovault/echovault/pkg/types"
	"github.com/gobwas/glob"
	"net"
	"slices"
	"strings"
)

func handleGetAllCommands(ctx context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	commands := server.GetAllCommands()

	res := ""
	commandCount := 0

	for _, c := range commands {
		if c.SubCommands == nil || len(c.SubCommands) <= 0 {
			res += "*6\r\n"
			// Command name
			res += fmt.Sprintf("+command\r\n*1\r\n$%d\r\n%s\r\n", len(c.Command), c.Command)
			// Command categories
			res += fmt.Sprintf("+categories\r\n*%d\r\n", len(c.Categories))
			for _, category := range c.Categories {
				res += fmt.Sprintf("$%d\r\n%s\r\n", len(category), category)
			}
			// Description
			res += fmt.Sprintf("+description\r\n*1\r\n$%d\r\n%s\r\n", len(c.Description), c.Description)

			commandCount += 1
			continue
		}
		// There are sub-commands
		for _, sc := range c.SubCommands {
			res += "*6\r\n"
			// Command name
			command := fmt.Sprintf("%s %s", c.Command, sc.Command)
			res += fmt.Sprintf("+command\r\n*1\r\n$%d\r\n%s\r\n", len(command), command)
			// Command categories
			res += fmt.Sprintf("+categories\r\n*%d\r\n", len(sc.Categories))
			for _, category := range sc.Categories {
				res += fmt.Sprintf("$%d\r\n%s\r\n", len(category), category)
			}
			// Description
			res += fmt.Sprintf("+description\r\n*1\r\n$%d\r\n%s\r\n", len(sc.Description), sc.Description)

			commandCount += 1
		}
	}

	res = fmt.Sprintf("*%d\r\n%s", commandCount, res)

	return []byte(res), nil
}

func handleCommandCount(_ context.Context, _ []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	var count int

	commands := server.GetAllCommands()
	for _, command := range commands {
		if command.SubCommands != nil && len(command.SubCommands) > 0 {
			for _, _ = range command.SubCommands {
				count += 1
			}
			continue
		}
		count += 1
	}

	return []byte(fmt.Sprintf(":%d\r\n", count)), nil
}

func handleCommandList(_ context.Context, cmd []string, server types.EchoVault, _ *net.Conn) ([]byte, error) {
	switch len(cmd) {
	case 2:
		// Command is COMMAND LIST
		var count int
		var res string
		commands := server.GetAllCommands()
		for _, command := range commands {
			if command.SubCommands != nil && len(command.SubCommands) > 0 {
				for _, subcommand := range command.SubCommands {
					comm := fmt.Sprintf("%s %s", command.Command, subcommand.Command)
					res += fmt.Sprintf("$%d\r\n%s\r\n", len(comm), comm)
					count += 1
				}
				continue
			}
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(command.Command), command.Command)
			count += 1
		}
		res = fmt.Sprintf("*%d\r\n%s", count, res)
		return []byte(res), nil

	case 5:
		var count int
		var res string
		// Command has filter
		if !strings.EqualFold("FILTERBY", cmd[2]) {
			return nil, fmt.Errorf("expected FILTERBY, got %s", strings.ToUpper(cmd[2]))
		}
		if strings.EqualFold("ACLCAT", cmd[3]) {
			// ACL Category filter
			commands := server.GetAllCommands()
			category := strings.ToLower(cmd[4])
			for _, command := range commands {
				if command.SubCommands != nil && len(command.SubCommands) > 0 {
					for _, subcommand := range command.SubCommands {
						if slices.Contains(subcommand.Categories, category) {
							comm := fmt.Sprintf("%s %s", command.Command, subcommand.Command)
							res += fmt.Sprintf("$%d\r\n%s\r\n", len(comm), comm)
							count += 1
						}
					}
					continue
				}
				if slices.Contains(command.Categories, category) {
					res += fmt.Sprintf("$%d\r\n%s\r\n", len(command.Command), command.Command)
					count += 1
				}
			}
		} else if strings.EqualFold("PATTERN", cmd[3]) {
			// Pattern filter
			commands := server.GetAllCommands()
			g := glob.MustCompile(cmd[4])
			for _, command := range commands {
				if command.SubCommands != nil && len(command.SubCommands) > 0 {
					for _, subcommand := range command.SubCommands {
						comm := fmt.Sprintf("%s %s", command.Command, subcommand.Command)
						if g.Match(comm) {
							res += fmt.Sprintf("$%d\r\n%s\r\n", len(comm), comm)
							count += 1
						}
					}
					continue
				}
				if g.Match(command.Command) {
					res += fmt.Sprintf("$%d\r\n%s\r\n", len(command.Command), command.Command)
					count += 1
				}
			}
		} else if strings.EqualFold("MODULE", cmd[3]) {
			// Module filter
			commands := server.GetAllCommands()
			module := strings.ToLower(cmd[4])
			for _, command := range commands {
				if command.SubCommands != nil && len(command.SubCommands) > 0 {
					for _, subcommand := range command.SubCommands {
						if strings.EqualFold(subcommand.Module, module) {
							comm := fmt.Sprintf("%s %s", command.Command, subcommand.Command)
							res += fmt.Sprintf("$%d\r\n%s\r\n", len(comm), comm)
							count += 1
						}
					}
					continue
				}
				if strings.EqualFold(command.Module, module) {
					res += fmt.Sprintf("$%d\r\n%s\r\n", len(command.Command), command.Command)
					count += 1
				}
			}
		} else {
			return nil, fmt.Errorf("expected filter to be ACLCAT or PATTERN, got %s", strings.ToUpper(cmd[3]))
		}
		res = fmt.Sprintf("*%d\r\n%s", count, res)
		return []byte(res), nil
	default:
		return nil, errors.New(constants.WrongArgsResponse)
	}
}

func handleCommandDocs(_ context.Context, _ []string, _ types.EchoVault, _ *net.Conn) ([]byte, error) {
	return []byte("*0\r\n"), nil
}

func Commands() []types.Command {
	return []types.Command{
		{
			Command:           "commands",
			Module:            constants.AdminModule,
			Categories:        []string{constants.AdminCategory, constants.SlowCategory},
			Description:       "Get a list of all the commands in available on the echovault with categories and descriptions",
			Sync:              false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) { return []string{}, nil },
			HandlerFunc:       handleGetAllCommands,
		},
		{
			Command:     "command",
			Module:      constants.AdminModule,
			Categories:  []string{},
			Description: "Commands pertaining to echovault commands",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				return []string{}, nil
			},
			SubCommands: []types.SubCommand{
				{
					Command:           "docs",
					Module:            constants.AdminModule,
					Categories:        []string{constants.SlowCategory, constants.ConnectionCategory},
					Description:       "Get command documentation",
					Sync:              false,
					KeyExtractionFunc: func(cmd []string) ([]string, error) { return []string{}, nil },
					HandlerFunc:       handleCommandDocs,
				},
				{
					Command:           "count",
					Module:            constants.AdminModule,
					Categories:        []string{constants.SlowCategory},
					Description:       "Get the dumber of commands in the echovault",
					Sync:              false,
					KeyExtractionFunc: func(cmd []string) ([]string, error) { return []string{}, nil },
					HandlerFunc:       handleCommandCount,
				},
				{
					Command:    "list",
					Module:     constants.AdminModule,
					Categories: []string{constants.SlowCategory},
					Description: `(COMMAND LIST [FILTERBY <ACLCAT category | PATTERN pattern | MODULE module>]) Get the list of command names.
Allows for filtering by ACL category or glob pattern.`,
					Sync:              false,
					KeyExtractionFunc: func(cmd []string) ([]string, error) { return []string{}, nil },
					HandlerFunc:       handleCommandList,
				},
			},
		},
		{
			Command:     "save",
			Module:      constants.AdminModule,
			Categories:  []string{constants.AdminCategory, constants.SlowCategory, constants.DangerousCategory},
			Description: "(SAVE) Trigger a snapshot save",
			Sync:        true,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				return []string{}, nil
			},
			HandlerFunc: func(ctx context.Context, cmd []string, server types.EchoVault, conn *net.Conn) ([]byte, error) {
				if err := server.TakeSnapshot(); err != nil {
					return nil, err
				}
				return []byte(constants.OkResponse), nil
			},
		},
		{
			Command:     "lastsave",
			Module:      constants.AdminModule,
			Categories:  []string{constants.AdminCategory, constants.FastCategory, constants.DangerousCategory},
			Description: "(LASTSAVE) Get unix timestamp for the latest snapshot in milliseconds.",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				return []string{}, nil
			},
			HandlerFunc: func(ctx context.Context, cmd []string, server types.EchoVault, conn *net.Conn) ([]byte, error) {
				msec := server.GetLatestSnapshot()
				if msec == 0 {
					return nil, errors.New("no snapshot")
				}
				return []byte(fmt.Sprintf(":%d\r\n", msec)), nil
			},
		},
		{
			Command:     "rewriteaof",
			Module:      constants.AdminModule,
			Categories:  []string{constants.AdminCategory, constants.SlowCategory, constants.DangerousCategory},
			Description: "(REWRITEAOF) Trigger re-writing of append process",
			Sync:        false,
			KeyExtractionFunc: func(cmd []string) ([]string, error) {
				return []string{}, nil
			},
			HandlerFunc: func(ctx context.Context, cmd []string, server types.EchoVault, conn *net.Conn) ([]byte, error) {
				if err := server.RewriteAOF(); err != nil {
					return nil, err
				}
				return []byte(constants.OkResponse), nil
			},
		},
	}
}
