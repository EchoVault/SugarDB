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
	"github.com/echovault/sugardb/internal"
	"github.com/echovault/sugardb/internal/modules/set"
	"github.com/echovault/sugardb/internal/modules/sorted_set"
	lua "github.com/yuin/gopher-lua"
	"strings"
)

func generateLuaCommandInfo(path string) (*lua.LState, string, string, []string, string, bool, string, error) {
	L := lua.NewState()

	// Load lua file
	if err := L.DoFile(path); err != nil {
		return nil, "", "", nil, "", false, "", fmt.Errorf("could not load lua script file %s: %v", path, err)
	}

	// Register set data type
	setMetaTable := L.NewTypeMetatable("set")
	L.SetGlobal("set", setMetaTable)
	// Static fields
	L.SetField(setMetaTable, "new", L.NewFunction(func(state *lua.LState) int {
		// Create set
		s := set.NewSet([]string{})
		// If the default values are passed, add them to the set.
		if state.GetTop() == 1 {
			elems := state.CheckTable(1)
			elems.ForEach(func(key lua.LValue, value lua.LValue) {
				s.Add([]string{value.String()})
			})
			state.Pop(1)
		}
		// Push the set to the stack
		ud := state.NewUserData()
		ud.Value = s
		state.SetMetatable(ud, state.GetTypeMetatable("set"))
		state.Push(ud)
		return 1
	}))
	// Set methods
	L.SetField(setMetaTable, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"add": func(state *lua.LState) int {
			s := checkSet(state, 1)
			// Extract the elements from the args
			var elems []string
			tbl := state.CheckTable(2)
			tbl.ForEach(func(key lua.LValue, value lua.LValue) {
				elems = append(elems, value.String())
			})
			// Add the elements to the set
			state.Push(lua.LNumber(s.Add(elems)))
			return 1
		},
		"pop": func(state *lua.LState) int {
			s := checkSet(state, 1)
			count := state.CheckNumber(2)
			// Create the table of popped elements
			popped := state.NewTable()
			for i, elem := range s.Pop(int(count)) {
				popped.RawSetInt(i+1, lua.LString(elem))
			}
			// Return popped elements
			state.Push(popped)
			return 1
		},
		"contains": func(state *lua.LState) int {
			s := checkSet(state, 1)
			state.Push(lua.LBool(s.Contains(state.CheckString(2))))
			return 1
		},
		"cardinality": func(state *lua.LState) int {
			s := checkSet(state, 1)
			state.Push(lua.LNumber(s.Cardinality()))
			return 1
		},
		"remove": func(state *lua.LState) int {
			s := checkSet(state, 1)
			// Extract elements to be removed
			var elems []string
			tbl := state.CheckTable(2)
			tbl.ForEach(func(key lua.LValue, value lua.LValue) {
				elems = append(elems, value.String())
			})
			// Remove the elements and return the removed count
			state.Push(lua.LNumber(s.Remove(elems)))
			return 1
		},
		"move": func(state *lua.LState) int {
			s1 := checkSet(state, 1)
			s2 := checkSet(state, 2)
			elem := state.CheckString(3)
			moved := s1.Move(s2, elem)
			state.Push(lua.LBool(moved == 1))
			return 1
		},
		"subtract": func(state *lua.LState) int {
			s1 := checkSet(state, 1)
			var sets []*set.Set
			// Extract sets to subtract
			tbl := state.CheckTable(2)
			tbl.ForEach(func(key lua.LValue, value lua.LValue) {
				ud, ok := value.(*lua.LUserData)
				if !ok {
					state.ArgError(2, "table must only contain sets")
					return
				}
				s, ok := ud.Value.(*set.Set)
				if !ok {
					state.ArgError(2, "table must only contain sets")
					return
				}
				sets = append(sets, s)
			})
			// Return the resulting set
			ud := state.NewUserData()
			ud.Value = s1.Subtract(sets)
			state.SetMetatable(ud, state.GetTypeMetatable("set"))
			state.Push(ud)
			return 1
		},
		"all": func(state *lua.LState) int {
			s := checkSet(state, 1)
			// Build table of all the elements in the set
			elems := state.NewTable()
			for i, e := range s.GetAll() {
				elems.RawSetInt(i+1, lua.LString(e))
			}
			// Return all the set's elements
			state.Push(elems)
			return 1
		},
		"random": func(state *lua.LState) int {
			s := checkSet(state, 1)
			count := state.CheckNumber(2)
			// Build table of random elements
			elems := state.NewTable()
			for i, e := range s.GetRandom(int(count)) {
				elems.RawSetInt(i+1, lua.LString(e))
			}
			// Return random elements
			state.Push(elems)
			return 1
		},
	}))

	// Register sorted set member data type
	sortedSetMemberMetaTable := L.NewTypeMetatable("zmember")
	L.SetGlobal("zmember", sortedSetMemberMetaTable)
	// Static fields
	L.SetField(sortedSetMemberMetaTable, "new", L.NewFunction(func(state *lua.LState) int {
		// Create sorted set member param
		param := &sorted_set.MemberParam{}
		// Make sure a value table is passed
		if state.GetTop() != 1 {
			state.ArgError(1, "expected table containing value and score to be passed")
		}
		// Set the passed values in params
		arg := state.CheckTable(1)
		arg.ForEach(func(key lua.LValue, value lua.LValue) {
			switch strings.ToLower(key.String()) {
			case "score":
				if score, ok := value.(lua.LNumber); ok {
					param.Score = sorted_set.Score(score)
					return
				}
				state.ArgError(1, "score is not a number")
			case "value":
				param.Value = sorted_set.Value(value.String())
			default:
				state.ArgError(1, fmt.Sprintf("unexpected key '%s' in zmember table", key.String()))
			}
		})
		// Check if value is not empty
		if param.Value == "" {
			state.ArgError(1, fmt.Sprintf("value is empty string"))
		}
		// Push the param to the stack and return
		ud := state.NewUserData()
		ud.Value = param
		state.SetMetatable(ud, state.GetTypeMetatable("zmember"))
		state.Push(ud)
		return 1
	}))
	// Sorted set member methods
	L.SetField(sortedSetMemberMetaTable, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"value": func(state *lua.LState) int {
			m := checkSortedSetMember(state, 1)
			if state.GetTop() == 2 {
				m.Value = sorted_set.Value(state.CheckString(2))
				return 0
			}
			L.Push(lua.LString(m.Value))
			return 1
		},
		"score": func(state *lua.LState) int {
			m := checkSortedSetMember(state, 1)
			if state.GetTop() == 2 {
				m.Score = sorted_set.Score(state.CheckNumber(2))
				return 0
			}
			L.Push(lua.LNumber(m.Score))
			return 1
		},
	}))

	// Register sorted set data type
	sortedSetMetaTable := L.NewTypeMetatable("zset")
	L.SetGlobal("zset", sortedSetMetaTable)
	// Static fields
	L.SetField(sortedSetMetaTable, "new", L.NewFunction(func(state *lua.LState) int {
		// If default values are passed, add them to the set
		var members []sorted_set.MemberParam
		if state.GetTop() == 1 {
			params := state.CheckTable(1)
			params.ForEach(func(key lua.LValue, value lua.LValue) {
				d, ok := value.(*lua.LUserData)
				if !ok {
					state.ArgError(1, "expected user data")
				}
				if m, ok := d.Value.(*sorted_set.MemberParam); ok {
					members = append(members, sorted_set.MemberParam{Value: m.Value, Score: m.Score})
					return
				}
				state.ArgError(1, fmt.Sprintf("expected member param, got %s", value.Type().String()))
			})
		}
		// Create the sorted set
		ss := sorted_set.NewSortedSet(members)
		ud := state.NewUserData()
		ud.Value = ss
		state.SetMetatable(ud, state.GetTypeMetatable("zset"))
		state.Push(ud)
		return 1
	}))
	// Sorted set methods
	L.SetField(sortedSetMetaTable, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"add": func(state *lua.LState) int {
			ss := checkSortedSet(state, 1)

			// Extract member params
			paramArgs := state.CheckTable(2)
			var params []sorted_set.MemberParam
			paramArgs.ForEach(func(key lua.LValue, value lua.LValue) {
				ud, ok := value.(*lua.LUserData)
				if !ok {
					state.ArgError(2, "expected zmember")
				}
				if m, ok := ud.Value.(*sorted_set.MemberParam); ok {
					params = append(params, sorted_set.MemberParam{Value: m.Value, Score: m.Score})
					return
				}
				state.ArgError(2, "expected zmember to be sorted set member param")
			})

			// Extract the update options
			var updatePolicy interface{} = nil
			var comparison interface{} = nil
			var changed interface{} = nil
			var incr interface{} = nil
			if state.GetTop() == 3 {
				optsArgs := state.CheckTable(3)
				optsArgs.ForEach(func(key lua.LValue, value lua.LValue) {
					switch key.String() {
					default:
						state.ArgError(3, fmt.Sprintf("unknown option '%s'", key.String()))
					case "exists":
						if value == lua.LTrue {
							updatePolicy = "xx"
						} else {
							updatePolicy = "nx"
						}
					case "comparison":
						comparison = value.String()
					case "changed":
						if value == lua.LTrue {
							changed = "ch"
						}
					case "incr":
						if value == lua.LTrue {
							incr = "incr"
						}
					}
				})
			}

			ch, err := ss.AddOrUpdate(params, updatePolicy, comparison, changed, incr)
			if err != nil {
				state.ArgError(3, err.Error())
			}
			L.Push(lua.LNumber(ch))
			return 1
		},
		"update": func(state *lua.LState) int {
			ss := checkSortedSet(state, 1)

			// Extract member params
			paramArgs := state.CheckTable(2)
			var params []sorted_set.MemberParam
			paramArgs.ForEach(func(key lua.LValue, value lua.LValue) {
				ud, ok := value.(*lua.LUserData)
				if !ok {
					state.ArgError(2, "expected zmember")
				}
				if m, ok := ud.Value.(*sorted_set.MemberParam); ok {
					params = append(params, sorted_set.MemberParam{Value: m.Value, Score: m.Score})
					return
				}
				state.ArgError(2, "expected zmember to be sorted set member param")
			})

			// Extract the update options
			var updatePolicy interface{} = nil
			var comparison interface{} = nil
			var changed interface{} = nil
			var incr interface{} = nil
			if state.GetTop() == 3 {
				optsArgs := state.CheckTable(3)
				optsArgs.ForEach(func(key lua.LValue, value lua.LValue) {
					switch key.String() {
					default:
						state.ArgError(3, fmt.Sprintf("unknown option '%s'", key.String()))
					case "exists":
						if value == lua.LTrue {
							updatePolicy = "xx"
						} else {
							updatePolicy = "nx"
						}
					case "comparison":
						comparison = value.String()
					case "changed":
						if value == lua.LTrue {
							changed = "ch"
						}
					case "incr":
						if value == lua.LTrue {
							incr = "incr"
						}
					}
				})
			}

			ch, err := ss.AddOrUpdate(params, updatePolicy, comparison, changed, incr)
			if err != nil {
				state.ArgError(3, err.Error())
			}
			L.Push(lua.LNumber(ch))
			return 1
		},
		"remove": func(state *lua.LState) int {
			ss := checkSortedSet(state, 1)
			L.Push(lua.LBool(ss.Remove(sorted_set.Value(state.CheckString(2)))))
			return 1
		},
		"cardinality": func(state *lua.LState) int {
			state.Push(lua.LNumber(checkSortedSet(state, 1).Cardinality()))
			return 1
		},
		"contains": func(state *lua.LState) int {
			ss := checkSortedSet(state, 1)
			L.Push(lua.LBool(ss.Contains(sorted_set.Value(state.Get(-2).String()))))
			return 1
		},
		"random": func(state *lua.LState) int {
			ss := checkSortedSet(state, 1)
			count := 1
			// If a count is passed, use that
			if state.GetTop() == 2 {
				count = state.CheckInt(2)
			}
			// Build members table
			random := state.NewTable()
			members := ss.GetRandom(count)
			for i, member := range members {
				ud := state.NewUserData()
				ud.Value = sorted_set.MemberParam{Value: member.Value, Score: member.Score}
				state.SetMetatable(ud, state.GetTypeMetatable("zmember"))
				random.RawSetInt(i+1, ud)
			}
			// Push the table to the stack
			state.Push(random)
			return 1
		},
		"all": func(state *lua.LState) int {
			ss := checkSortedSet(state, 1)
			// Build members table
			members := state.NewTable()
			for i, member := range ss.GetAll() {
				ud := state.NewUserData()
				ud.Value = &sorted_set.MemberParam{Value: member.Value, Score: member.Score}
				state.SetMetatable(ud, state.GetTypeMetatable("zmember"))
				members.RawSetInt(i+1, ud)
			}
			// Push members table to stack and return
			state.Push(members)
			return 1
		},
		"subtract": func(state *lua.LState) int {
			ss := checkSortedSet(state, 1)
			// Get the sorted sets from the args
			var others []*sorted_set.SortedSet
			arg := state.CheckTable(2)
			arg.ForEach(func(key lua.LValue, value lua.LValue) {
				ud, ok := value.(*lua.LUserData)
				if !ok {
					state.ArgError(2, "expected user data")
				}
				zset, ok := ud.Value.(*sorted_set.SortedSet)
				if !ok {
					state.ArgError(2, fmt.Sprintf("expected zset at key '%s'", key.String()))
				}
				others = append(others, zset)
			})
			// Calculate result
			result := ss.Subtract(others)
			// Push result to the stack and return
			ud := state.NewUserData()
			ud.Value = result
			state.SetMetatable(ud, state.GetTypeMetatable("zset"))
			L.Push(ud)
			return 1
		},
	}))

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

func buildLuaKeyExtractionFunc(vm any, cmd []string, args []string) (internal.KeyExtractionFuncResult, error) {
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

func (server *SugarDB) buildLuaHandlerFunc(vm any, args []string, params internal.HandlerFuncParams) ([]byte, error) {
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
			// Actually parse the value and set it in the response as the appropriate LValue.
			res.RawSetString(key, nativeTypeToLuaType(value))
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
			// Actually parse the value and set it in the response as the appropriate LValue.
			values[key.String()] = luaTypeToNativeType(L, value)
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
		Fn:      L.GetGlobal("handlerFunc"),
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

func checkSet(L *lua.LState, n int) *set.Set {
	ud := L.CheckUserData(n)
	if v, ok := ud.Value.(*set.Set); ok {
		return v
	}
	L.ArgError(n, "set expected")
	return nil
}

func checkSortedSetMember(L *lua.LState, n int) *sorted_set.MemberParam {
	ud := L.CheckUserData(n)
	if v, ok := ud.Value.(*sorted_set.MemberParam); ok {
		return v
	}
	L.ArgError(n, "zmember expected")
	return nil
}

func checkSortedSet(L *lua.LState, n int) *sorted_set.SortedSet {
	ud := L.CheckUserData(n)
	if v, ok := ud.Value.(*sorted_set.SortedSet); ok {
		return v
	}
	L.ArgError(n, "zset expected")
	return nil
}

func luaTypeToNativeType(L *lua.LState, value lua.LValue) interface{} {
	// TODO: Translate lua type to native type
	return nil
}

func nativeTypeToLuaType(value interface{}) lua.LValue {
	// TODO: Translate native type to lua type
	return nil
}
