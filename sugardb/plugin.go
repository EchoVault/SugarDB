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

func (server *SugarDB) AddScript(engine string, scriptType string, content string, args []string) error {
	return nil
}

func (server *SugarDB) AddScriptCommand(
	path string,
	args []string,
) error {
	// Extract the engine from the script file extension
	var engine string
	if strings.HasSuffix(path, ".lua") {
		engine = "lua"
	}

	// Check if the engine is supported
	supportedEngines := []string{"lua"}
	if !slices.Contains(supportedEngines, strings.ToLower(engine)) {
		return fmt.Errorf("engine %s not supported, only %v engines are supported", engine, supportedEngines)
	}

	// Initialise VM for the command depending on the engine.
	var vm any
	var commandName string
	var module string
	var categories []string
	var description string
	var synchronize bool
	var commandType string
	var err error

	switch strings.ToLower(engine) {
	case "lua":
		vm, commandName, module, categories, description, synchronize, commandType, err = generateLuaCommandInfo(path)
	}

	if err != nil {
		return err
	}

	// Save the script's VM to the server's list of VMs.
	server.scriptVMs.Store(commandName, struct {
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
		Command:     commandName,
		Module:      module,
		Categories:  categories,
		Description: description,
		Sync:        synchronize,
		Type:        commandType,
		KeyExtractionFunc: func(engine string, vm any, args []string) internal.KeyExtractionFunc {
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
					// Create args table to pass to the Lua function
					funcArgs := L.NewTable()
					for i, s := range args {
						funcArgs.RawSetInt(i+1, lua.LString(s))
					}
					// Call the Lua key extraction function
					if err := L.CallByParam(lua.P{
						Fn:      L.GetGlobal("keyExtractionFunc"),
						NRet:    2,
						Protect: true,
					}, command, funcArgs); err != nil {
						return internal.KeyExtractionFuncResult{}, err
					}
					// Check if error is returned
					if err, ok := L.Get(-1).(lua.LString); ok {
						return internal.KeyExtractionFuncResult{}, errors.New(err.String())
					}
					// Get the returned value
					ret := L.Get(-2)
					L.Pop(2)
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
		}(engine, vm, args),
		HandlerFunc: func(engine string, vm any, args []string) internal.HandlerFunc {
			// Wrapper for the handler function
			return func(params internal.HandlerFuncParams) ([]byte, error) {
				switch strings.ToLower(engine) {
				case "lua":
					L := vm.(*lua.LState)
					// Lua table context
					ctx := L.NewTable()
					ctx.RawSetString("protocol", lua.LNumber(params.Context.Value("Protocol").(int)))
					// Command that triggered the handler (Array)
					cmd := L.NewTable()
					for i, s := range params.Command {
						cmd.RawSetInt(i+1, lua.LString(s))
					}
					// Function that checks if keys exist
					keysExist := L.NewFunction(func(state *lua.LState) int {
						// Get the keys array and pop it from the stack.
						v := state.Get(-1).(*lua.LTable)
						state.Pop(1)
						// Extract the keys from the keys array passed from the lua script.
						var keys []string
						for i := 1; i <= v.Len(); i++ {
							keys = append(keys, v.RawGetInt(i).String())
						}
						// Call the keysExist method to check if the key exists in the store.
						exist := server.keysExist(params.Context, keys)
						// Build the response table that specifies if each key exists.
						res := state.NewTable()
						for key, exists := range exist {
							res.RawSetString(key, lua.LBool(exists))
						}
						// Push the response to the stack.
						state.Push(res)
						return 1
					})
					// Function that gets values from keys
					getValues := L.NewFunction(func(state *lua.LState) int {
						// Get the keys array and pop it from the stack.
						v := state.Get(-1).(*lua.LTable)
						state.Pop(1)
						// Extract the keys from the keys array passed from the lua script.
						var keys []string
						for i := 1; i <= v.Len(); i++ {
							keys = append(keys, v.RawGetInt(i).String())
						}
						// Call the getValues method to get the values for each of the keys.
						values := server.getValues(params.Context, keys)
						// Build the response table that contains each key/value pair.
						res := state.NewTable()
						for key, value := range values {
							// TODO: Actually parse the value and set it in the response as the appropriate LValue.
							res.RawSetString(key, lua.LString(value.(string)))
						}
						// Push the value to the stack
						state.Push(res)
						return 1
					})
					// Function that sets values on keys
					setValues := L.NewFunction(func(state *lua.LState) int {
						// Get the keys array and pop it from the stack.
						v := state.Get(-1).(*lua.LTable)
						state.Pop(1)
						// Get values passed from the Lua script and add.
						values := make(map[string]interface{})
						v.ForEach(func(key lua.LValue, value lua.LValue) {
							// TODO: Actually parse the value and set it in the response as the appropriate LValue.
							values[key.String()] = value
						})
						if err := server.setValues(params.Context, values); err != nil {
							state.Push(lua.LString(err.Error()))
							return 1
						}
						state.Push(nil)
						return 1
					})
					// Args (Array)
					funcArgs := L.NewTable()
					for i, s := range args {
						funcArgs.RawSetInt(i+1, lua.LString(s))
					}
					// Call the lua handler function
					if err := L.CallByParam(lua.P{
						Fn:      L.GetGlobal("handler"),
						NRet:    2,
						Protect: true,
					}, ctx, cmd, keysExist, getValues, setValues, funcArgs); err != nil {
						return nil, err
					}
					// Get and pop the 2 values at the top of the stack, checking whether an error is returned.
					defer L.Pop(2)
					if err, ok := L.Get(-1).(lua.LString); ok {
						return nil, errors.New(err.String())
					}
					return []byte(L.Get(-2).String()), nil
				}
				return nil, fmt.Errorf("unkown return value for command %s", commandName)
			}
		}(engine, vm, args),
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
