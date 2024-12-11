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
	"errors"
	"fmt"
	"github.com/echovault/sugardb/internal"
	"github.com/echovault/sugardb/internal/constants"
	"github.com/gobwas/glob"
	"slices"
	"strings"
)

func handleGetAllCommands(params internal.HandlerFuncParams) ([]byte, error) {
	commands := params.GetAllCommands()

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

func handleCommandCount(params internal.HandlerFuncParams) ([]byte, error) {
	var count int

	commands := params.GetAllCommands()
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

func handleCommandList(params internal.HandlerFuncParams) ([]byte, error) {
	switch len(params.Command) {
	case 2:
		// Command is COMMAND LIST
		var count int
		var res string
		commands := params.GetAllCommands()
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
		if !strings.EqualFold("FILTERBY", params.Command[2]) {
			return nil, fmt.Errorf("expected FILTERBY, got %s", strings.ToUpper(params.Command[2]))
		}
		if strings.EqualFold("ACLCAT", params.Command[3]) {
			// ACL Category filter
			commands := params.GetAllCommands()
			category := strings.ToLower(params.Command[4])
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
		} else if strings.EqualFold("PATTERN", params.Command[3]) {
			// Pattern filter
			commands := params.GetAllCommands()
			g := glob.MustCompile(params.Command[4])
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
		} else if strings.EqualFold("MODULE", params.Command[3]) {
			// Module filter
			commands := params.GetAllCommands()
			module := strings.ToLower(params.Command[4])
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
			return nil, fmt.Errorf("expected filter to be ACLCAT or PATTERN, got %s", strings.ToUpper(params.Command[3]))
		}
		res = fmt.Sprintf("*%d\r\n%s", count, res)
		return []byte(res), nil
	default:
		return nil, errors.New(constants.WrongArgsResponse)
	}
}

func handleCommandDocs(params internal.HandlerFuncParams) ([]byte, error) {
	return []byte("*0\r\n"), nil
}

func Commands() []internal.Command {
	return []internal.Command{
		{
			Command:     "commands",
			Module:      constants.AdminModule,
			Categories:  []string{constants.AdminCategory, constants.SlowCategory},
			Description: "Get a list of all the commands in available on the echovault with categories and descriptions.",
			Sync:        false,
			Type:        "BUILT_IN",
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: handleGetAllCommands,
		},
		{
			Command:     "command",
			Module:      constants.AdminModule,
			Categories:  []string{},
			Description: "Commands pertaining to echovault commands",
			Sync:        false,
			Type:        "BUILT_IN",
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
				}, nil
			},
			SubCommands: []internal.SubCommand{
				{
					Command:     "docs",
					Module:      constants.AdminModule,
					Categories:  []string{constants.SlowCategory, constants.ConnectionCategory},
					Description: "Get command documentation",
					Sync:        false,
					KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
						return internal.KeyExtractionFuncResult{
							Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
						}, nil
					},
					HandlerFunc: handleCommandDocs,
				},
				{
					Command:     "count",
					Module:      constants.AdminModule,
					Categories:  []string{constants.AdminCategory, constants.SlowCategory},
					Description: "Get the dumber of commands in the echovault instance.",
					Sync:        false,
					KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
						return internal.KeyExtractionFuncResult{
							Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
						}, nil
					},
					HandlerFunc: handleCommandCount,
				},
				{
					Command:    "list",
					Module:     constants.AdminModule,
					Categories: []string{constants.AdminCategory, constants.SlowCategory},
					Description: `(COMMAND LIST [FILTERBY <ACLCAT category | PATTERN pattern | MODULE module>]) 
Get the list of command names. Allows for filtering by ACL category or glob pattern.`,
					Sync: false,
					KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
						return internal.KeyExtractionFuncResult{
							Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
						}, nil
					},
					HandlerFunc: handleCommandList,
				},
			},
		},
		{
			Command:     "save",
			Module:      constants.AdminModule,
			Categories:  []string{constants.AdminCategory, constants.SlowCategory, constants.DangerousCategory},
			Description: "(SAVE) Trigger a snapshot save.",
			Sync:        true,
			Type:        "BUILT_IN",
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: func(params internal.HandlerFuncParams) ([]byte, error) {
				if err := params.TakeSnapshot(); err != nil {
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
			Type:        "BUILT_IN",
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: func(params internal.HandlerFuncParams) ([]byte, error) {
				msec := params.GetLatestSnapshotTime()
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
			Description: "(REWRITEAOF) Trigger re-writing of append process.",
			Sync:        false,
			Type:        "BUILT_IN",
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
				}, nil
			},
			HandlerFunc: func(params internal.HandlerFuncParams) ([]byte, error) {
				if err := params.RewriteAOF(); err != nil {
					return nil, err
				}
				return []byte(constants.OkResponse), nil
			},
		},
		{
			Command:     "module",
			Module:      constants.AdminModule,
			Categories:  []string{},
			Description: "Module commands",
			Type:        "BUILT_IN",
			KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				return internal.KeyExtractionFuncResult{
					Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
				}, nil
			},
			SubCommands: []internal.SubCommand{
				{
					Command:    "load",
					Module:     constants.AdminModule,
					Categories: []string{constants.AdminCategory, constants.SlowCategory, constants.DangerousCategory},
					Description: `(MODULE LOAD path [arg [arg ...]]) Load a module from a dynamic library at runtime. 
The path should be the full path to the module, including the .so filename. Any args will be be passed unmodified to the
module's key extraction and handler functions.`,
					Sync: true,
					KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
						return internal.KeyExtractionFuncResult{
							Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
						}, nil
					},
					HandlerFunc: func(params internal.HandlerFuncParams) ([]byte, error) {
						if len(params.Command) < 3 {
							return nil, errors.New(constants.WrongArgsResponse)
						}
						var args []string
						if len(params.Command) > 3 {
							args = params.Command[3:]
						}
						if err := params.LoadModule(params.Command[2], args...); err != nil {
							return nil, err
						}
						return []byte(constants.OkResponse), nil
					},
				},
				{
					Command:    "unload",
					Module:     constants.AdminModule,
					Categories: []string{constants.AdminCategory, constants.SlowCategory, constants.DangerousCategory},
					Description: `(MODULE UNLOAD name) 
Unloads a module based on the its name as displayed by the MODULE LIST command.`,
					Sync: true,
					KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
						return internal.KeyExtractionFuncResult{
							Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
						}, nil
					},
					HandlerFunc: func(params internal.HandlerFuncParams) ([]byte, error) {
						if len(params.Command) != 3 {
							return nil, errors.New(constants.WrongArgsResponse)
						}
						params.UnloadModule(params.Command[2])
						return []byte(constants.OkResponse), nil
					},
				},
				{
					Command:     "list",
					Module:      constants.AdminModule,
					Categories:  []string{constants.AdminModule, constants.SlowCategory, constants.DangerousCategory},
					Description: `(MODULE LIST) List all the modules that are currently loaded in the server.`,
					Sync:        false,
					KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
						return internal.KeyExtractionFuncResult{
							Channels: make([]string, 0), ReadKeys: make([]string, 0), WriteKeys: make([]string, 0),
						}, nil
					},
					HandlerFunc: func(params internal.HandlerFuncParams) ([]byte, error) {
						modules := params.ListModules()
						res := fmt.Sprintf("*%d\r\n", len(modules))
						for _, module := range modules {
							res += fmt.Sprintf("$%d\r\n%s\r\n", len(module), module)
						}
						return []byte(res), nil
					},
				},
			},
		},
	}
}
