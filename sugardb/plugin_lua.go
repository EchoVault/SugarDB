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
	"errors"
	"fmt"
	lua "github.com/yuin/gopher-lua"
)

func generateLuaCommandInfo(path string) (*lua.LState, string, string, []string, string, bool, string, error) {
	L := lua.NewState()

	// Load lua file
	if err := L.DoFile(path); err != nil {
		return nil, "", "", nil, "", false, "", fmt.Errorf("could not load lua script file %s: %v", path, err)
	}

	// Get the command name
	cn := L.GetGlobal("command")
	if _, ok := cn.(lua.LString); !ok {
		return nil, "", "", nil, "", false, "", errors.New("command name does not exist or is not a string")
	}

	// Get the module
	m := L.GetGlobal("module")
	if _, ok := m.(lua.LString); !ok {
		return nil, "", "", nil, "", false, "", errors.New("module does not exist in script or is not string")
	}

	// Get the categories
	c := L.GetGlobal("categories")
	var categories []string
	if _, ok := c.(*lua.LTable); !ok {
		return nil, "", "", nil, "", false, "", errors.New("categories does not exist or is not an array")
	}
	for i := 0; i < c.(*lua.LTable).Len(); i++ {
		categories = append(categories, c.(*lua.LTable).RawGetInt(i+1).String())
	}

	// Get the description
	d := L.GetGlobal("description")
	if _, ok := m.(lua.LString); !ok {
		return nil, "", "", nil, "", false, "", errors.New("description does not exist or is not a string")
	}

	// Get the sync
	synchronize := L.GetGlobal("sync") == lua.LTrue

	// Set command type
	commandType := "LUA_SCRIPT"

	return L, cn.String(), m.String(), categories, d.String(), synchronize, commandType, nil
}
