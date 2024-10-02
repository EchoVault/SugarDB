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

package sugardb

import (
	"context"
	"errors"
	"fmt"
	"github.com/echovault/sugardb/internal"
	lua "github.com/yuin/gopher-lua"
	"io/fs"
	"os"
	"plugin"
	"slices"
	"strings"
	"sync"
)

func (server *SugarDB) AddScript(engine string, scriptType string, content string) error {
	return nil
}

func (server *SugarDB) AddScriptCommand(scriptCommand string, engine string, scriptType string, content string) error {
	// Check if the engine is supported
	supportedEngines := []string{"lua"}
	if !slices.Contains(supportedEngines, strings.ToLower(engine)) {
		return fmt.Errorf("engine %s not supported, only %v engines are supported", engine, supportedEngines)
	}

	// Check if the script type is supported
	if !slices.Contains([]string{"file", "raw"}, strings.ToLower(scriptType)) {
		return errors.New("script type must be wither file or raw")
	}

	// Check if the command already exists.
	server.commandsRWMut.RLock()
	for _, command := range server.commands {
		if strings.EqualFold(command.Command, scriptCommand) {
			return fmt.Errorf("command %s already exists", scriptCommand)
		}
	}
	server.commandsRWMut.RUnlock()

	// Initialise VM and lock for the commands depending on the engine.
	var vm any
	var module string
	var categories []string
	var description string
	var synchronize bool
	var commandType string

	switch strings.ToLower(engine) {
	case "lua":
		L := lua.NewState()
		if strings.EqualFold(scriptType, "file") {
			// Load lua file
			if err := L.DoFile(content); err != nil {
				return fmt.Errorf("could not load lua script file %s: %v", content, err)
			}
		} else {
			// Load raw lua script
			if err := L.DoString(content); err != nil {
				return fmt.Errorf("could not load lua script string: %v", err)
			}
		}
		// Get the module
		m := L.GetGlobal("module")
		if _, ok := m.(lua.LString); !ok {
			return errors.New("module does not exist in script or is not string")
		}
		module = m.String()
		// Get the categories
		c := L.GetGlobal("categories")
		if _, ok := c.(*lua.LTable); !ok {
			return errors.New("categories does not exist or is not an array")
		}
		for i := 0; i < c.(*lua.LTable).Len(); i++ {
			categories = append(categories, c.(*lua.LTable).RawGetInt(i+1).String())
		}
		// Get the description
		d := L.GetGlobal("description")
		if _, ok := m.(lua.LString); !ok {
			return errors.New("description does not exist or is not a string")
		}
		description = d.String()
		// Get the sync
		s := L.GetGlobal("sync")
		synchronize = s == lua.LTrue
		// Set command type
		commandType = "LUA_SCRIPT"
		vm = L
	}

	// Save the script's VM to the server's list of VMs.
	server.scriptVMs.Store(scriptCommand, struct {
		vm   any
		lock sync.Mutex
	}{
		vm: vm,
		// lock is the script mutex for the commands.
		// This mutex will be locked everytime the command is executed because
		// the script's VM is not thread safe.
		lock: sync.Mutex{},
	})

	// Build the command:
	command := internal.Command{
		Command:     strings.ToLower(scriptCommand),
		Module:      module,
		Categories:  categories,
		Description: description,
		Sync:        synchronize,
		Type:        commandType,
		KeyExtractionFunc: func(engine string, vm any) internal.KeyExtractionFunc {
			// Wrapper for the key function
			return func(cmd []string) (internal.KeyExtractionFuncResult, error) {
				switch strings.ToLower(engine) {
				case "lua":
					L := vm.(*lua.LState)
					// Create command table to pass to the Lua function
					command := L.NewTable()
					for i, s := range cmd {
						command.RawSetInt(i+1, lua.LString(s))
					}
					// Call the Lua key extraction function
					if err := L.CallByParam(lua.P{
						Fn:      L.GetGlobal("keyExtractionFunc"),
						NRet:    1,
						Protect: true,
					}, command); err != nil {
						return internal.KeyExtractionFuncResult{}, err
					}
					// Get the returned value
					ret := L.Get(-1)
					if keys, ok := ret.(*lua.LTable); ok {
						// If the returned value is a table, get the keys from the table
						return internal.KeyExtractionFuncResult{
							Channels: make([]string, 0),
							ReadKeys: func() []string {
								table := keys.RawGetString("readKeys").(*lua.LTable)
								var k []string
								for i := 1; i <= table.Len(); i++ {
									k = append(k, table.RawGetInt(i).String())
								}
								return k
							}(),
							WriteKeys: func() []string {
								table := keys.RawGetString("writeKeys").(*lua.LTable)
								var k []string
								for i := 1; i <= table.Len(); i++ {
									k = append(k, table.RawGetInt(i).String())
								}
								return k
							}(),
						}, nil
					} else {
						// If the returned value is a string, return the string error
						return internal.KeyExtractionFuncResult{}, errors.New(ret.(lua.LString).String())
					}
				}
				return internal.KeyExtractionFuncResult{
					Channels:  make([]string, 0),
					ReadKeys:  make([]string, 0),
					WriteKeys: make([]string, 0),
				}, nil
			}
		}(engine, vm),
		HandlerFunc: func(scriptCommand string, engine string, vm any) internal.HandlerFunc {
			// Wrapper for the handler function
			return func(params internal.HandlerFuncParams) ([]byte, error) {
				switch strings.ToLower(engine) {
				case "lua":
				}
				return nil, fmt.Errorf("command %s handler not implemented", scriptCommand)
			}
		}(scriptCommand, engine, vm),
	}

	// Add the commands to the list of commands.
	server.commandsRWMut.Lock()
	defer server.commandsRWMut.Unlock()
	server.commands = append(server.commands, command)

	return nil
}

// LoadModule loads an external module into SugarDB ar runtime.
//
// Parameters:
//
// `path` - string - The full path to the .so plugin to be loaded.
//
// `args` - ...string - A list of args that will be passed unmodified to the plugins command's
// KeyExtractionFunc and HandlerFunc
func (server *SugarDB) LoadModule(path string, args ...string) error {
	server.commandsRWMut.Lock()
	defer server.commandsRWMut.Unlock()

	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("load module: module %s not found", path)
		}
		return fmt.Errorf("load module: %v", err)
	}

	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("plugin open: %v", err)
	}

	commandSymbol, err := p.Lookup("Command")
	if err != nil {
		return err
	}
	command, ok := commandSymbol.(*string)
	if !ok {
		return errors.New("command symbol is not a string")
	}

	categoriesSymbol, err := p.Lookup("Categories")
	if err != nil {
		return err
	}
	categories, ok := categoriesSymbol.(*[]string)
	if !ok {
		return errors.New("categories symbol not a string slice")
	}

	descriptionSymbol, err := p.Lookup("Description")
	if err != nil {
		return err
	}
	description, ok := descriptionSymbol.(*string)
	if !ok {
		return errors.New("description symbol is no a string")
	}

	syncSymbol, err := p.Lookup("Sync")
	if err != nil {
		return err
	}
	sync, ok := syncSymbol.(*bool)
	if !ok {
		return errors.New("sync symbol is not a bool")
	}

	keyExtractionFuncSymbol, err := p.Lookup("KeyExtractionFunc")
	if err != nil {
		return fmt.Errorf("key extraction func symbol: %v", err)
	}
	keyExtractionFunc, ok := keyExtractionFuncSymbol.(func(cmd []string, args ...string) ([]string, []string, error))
	if !ok {
		return errors.New("key extraction function has unexpected signature")
	}

	handlerFuncSymbol, err := p.Lookup("HandlerFunc")
	if err != nil {
		return fmt.Errorf("handler func symbol: %v", err)
	}
	handlerFunc, ok := handlerFuncSymbol.(func(
		ctx context.Context,
		command []string,
		keysExist func(ctx context.Context, key []string) map[string]bool,
		getValues func(ctx context.Context, key []string) map[string]interface{},
		setValues func(ctx context.Context, entries map[string]interface{}) error,
		args ...string,
	) ([]byte, error))
	if !ok {
		return errors.New("handler function has unexpected signature")
	}

	// Remove the currently loaded version of this module and replace it with the new one
	server.commands = slices.DeleteFunc(server.commands, func(command internal.Command) bool {
		return strings.EqualFold(command.Module, path)
	})

	// Add the new command
	server.commands = append(server.commands, internal.Command{
		Command: *command,
		Module:  path,
		Categories: func() []string {
			// Convert all the categories to lower case for uniformity
			cats := make([]string, len(*categories))
			for i, cat := range *categories {
				cats[i] = strings.ToLower(cat)
			}
			return cats
		}(),
		Description: *description,
		Sync:        *sync,
		SubCommands: make([]internal.SubCommand, 0),
		KeyExtractionFunc: func(cmd []string) (internal.KeyExtractionFuncResult, error) {
			readKeys, writeKeys, err := keyExtractionFunc(cmd, args...)
			if err != nil {
				return internal.KeyExtractionFuncResult{}, err
			}
			return internal.KeyExtractionFuncResult{
				Channels:  make([]string, 0),
				ReadKeys:  readKeys,
				WriteKeys: writeKeys,
			}, nil
		},
		HandlerFunc: func(params internal.HandlerFuncParams) ([]byte, error) {
			return handlerFunc(
				params.Context,
				params.Command,
				params.KeysExist,
				params.GetValues,
				params.SetValues,
				args...,
			)
		},
	})

	return nil
}

// UnloadModule unloads the provided module
//
// Parameters:
//
// `module` - string - module name as displayed by the ListModules method.
func (server *SugarDB) UnloadModule(module string) {
	server.commandsRWMut.Lock()
	defer server.commandsRWMut.Unlock()
	server.commands = slices.DeleteFunc(server.commands, func(command internal.Command) bool {
		return strings.EqualFold(command.Module, module)
	})
}

// ListModules lists the currently loaded modules
//
// Returns: a string slice representing all the currently loaded modules.
func (server *SugarDB) ListModules() []string {
	server.commandsRWMut.RLock()
	defer server.commandsRWMut.RUnlock()
	var modules []string
	for _, command := range server.commands {
		if !slices.ContainsFunc(modules, func(module string) bool {
			return strings.EqualFold(module, command.Module)
		}) {
			modules = append(modules, strings.ToLower(command.Module))
		}
	}
	return modules
}
