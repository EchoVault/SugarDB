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
	"github.com/echovault/sugardb/internal/modules/hash"
	"github.com/echovault/sugardb/internal/modules/set"
	"github.com/echovault/sugardb/internal/modules/sorted_set"
	"github.com/robertkrimen/otto"
	"math"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	objectRegistry sync.Map
	idCounter      uint64
)

func registerObject(object interface{}) string {
	id := fmt.Sprintf("id-%d", atomic.AddUint64(&idCounter, 1))
	objectRegistry.Store(id, object)
	return id
}

func getObjectById(id string) (interface{}, bool) {
	return objectRegistry.Load(id)
}

func clearObjectRegistry() {
	atomic.StoreUint64(&idCounter, 0)
	objectRegistry.Clear()
}

func generateJSCommandInfo(path string) (*otto.Otto, string, []string, string, bool, string, error) {
	// Initialize the Otto vm
	vm := otto.New()

	// Load JS file
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, "", nil, "", false, "", fmt.Errorf("could not load javascript script file %s: %v", path, err)
	}
	if _, err = vm.Run(content); err != nil {
		return nil, "", nil, "", false, "", fmt.Errorf("could not run javascript script file %s: %v", path, err)
	}

	// Register hash data type
	_ = vm.Set("Hash", func(call otto.FunctionCall) otto.Value {
		// Initialize hash
		h := hash.Hash{}
		// If an object is passed then initialize the default values of the hash
		if len(call.ArgumentList) > 0 {
			args := call.Argument(0).Object()
			for _, key := range args.Keys() {
				value, _ := args.Get(key)
				v, _ := value.ToString()
				h[key] = hash.HashValue{Value: v}
			}
		}

		obj, _ := call.Otto.Object(`({})`)
		buildHashObject(obj, h)
		return obj.Value()
	})

	// Register set data type
	_ = vm.Set("Set", func(call otto.FunctionCall) otto.Value {
		// Initialize set
		s := set.NewSet([]string{})
		// If an array is passed add the values to the set
		if len(call.ArgumentList) > 0 {
			args := call.Argument(0).Object()
			var elems []string
			for _, key := range args.Keys() {
				value, _ := args.Get(key)
				v, _ := value.ToString()
				elems = append(elems, v)
			}
			s.Add(elems)
		}

		obj, _ := call.Otto.Object(`({})`)
		buildSetObject(obj, s)
		return obj.Value()
	})

	// Register sorted set member data type
	_ = vm.Set("ZMember", func(call otto.FunctionCall) otto.Value {
		obj, _ := call.Otto.Object(`({})`)

		m := &sorted_set.MemberParam{}
		if len(call.ArgumentList) != 1 {
			panicWithFunctionCall(call, "expected an object with score and value properties")
		}
		arg := call.Argument(0).Object()
		// Validate the object
		if err = validateMemberParamObject(arg); err != nil {
			panicWithFunctionCall(call, err.Error())
		}
		// Get the value
		value, _ := arg.Get("value")
		m.Value = sorted_set.Value(value.String())
		// Get the score
		s, _ := arg.Get("score")
		score, _ := s.ToFloat()
		m.Score = sorted_set.Score(score)
		// Build the Otto member param object
		buildMemberParamObject(obj, m)
		return obj.Value()
	})

	// Register sorted set data type
	_ = vm.Set("ZSet", func(call otto.FunctionCall) otto.Value {
		// If default args are passed when initializing sorted set, add them to the member params
		var params []sorted_set.MemberParam
		for _, arg := range call.ArgumentList {
			if !arg.IsObject() {
				panicWithFunctionCall(call, "zset constructor args must be sorted set members")
			}
			id, _ := arg.Object().Get("__id")
			o, exists := getObjectById(id.String())
			if !exists {
				panicWithFunctionCall(call, "unknown object passed to zset constructor")
			}
			p, ok := o.(*sorted_set.MemberParam)
			if !ok {
				panicWithFunctionCall(call, "unknown object passed to createZSet function")
			}
			params = append(params, *p)
		}
		ss := sorted_set.NewSortedSet(params)

		obj, _ := call.Otto.Object(`({})`)
		buildSortedSetObject(obj, ss)
		return obj.Value()
	})

	// Get the command name
	v, err := vm.Get("command")
	if err != nil {
		return nil, "", nil, "", false, "", fmt.Errorf("could not get javascript command %s: %v", path, err)
	}
	command, err := v.ToString()
	if err != nil || len(command) <= 0 {
		return nil, "", nil, "", false, "", fmt.Errorf("javascript command not found %s: %v", path, err)
	}

	// Get the categories
	v, err = vm.Get("categories")
	if err != nil {
		return nil, "", nil, "", false, "", fmt.Errorf("could not get javascript command categories %s: %v", path, err)
	}
	isArray, _ := vm.Run(`Array.isArray(categories)`)
	if ok, _ := isArray.ToBoolean(); !ok {
		return nil, "", nil, "", false, "", fmt.Errorf("javascript command categories is not an array %s: %v", path, err)
	}
	c, _ := v.Export()
	categories := c.([]string)

	// Get the description
	v, err = vm.Get("description")
	if err != nil {
		return nil, "", nil, "", false, "", fmt.Errorf("could not get javascript command description %s: %v", path, err)
	}
	description, err := v.ToString()
	if err != nil || len(description) <= 0 {
		return nil, "", nil, "", false, "", fmt.Errorf("javascript command description not found %s: %v", path, err)
	}

	// Get the sync policy
	v, err = vm.Get("sync")
	if err != nil {
		return nil, "", nil, "", false, "", fmt.Errorf("could not get javascript command sync policy %s: %v", path, err)
	}
	if !v.IsBoolean() {
		return nil, "", nil, "", false, "", fmt.Errorf("javascript command sync policy is not a boolean %s: %v", path, err)
	}
	synchronize, _ := v.ToBoolean()

	// Set command type
	commandType := "JS_SCRIPT"

	return vm, strings.ToLower(command), categories, description, synchronize, commandType, nil
}

// jsKeyExtractionFunc executes the extraction function defined in the script and returns the result or error.
func (server *SugarDB) jsKeyExtractionFunc(cmd []string, args []string) (internal.KeyExtractionFuncResult, error) {
	// Lock the script before executing the key extraction function.
	script, ok := server.scriptVMs.Load(strings.ToLower(cmd[0]))
	if !ok {
		return internal.KeyExtractionFuncResult{}, fmt.Errorf("no lock found for script command %s", cmd[0])
	}
	machine := script.(struct {
		vm   any
		lock *sync.Mutex
	})
	machine.lock.Lock()
	defer machine.lock.Unlock()

	vm := machine.vm.(*otto.Otto)

	f, _ := vm.Get("keyExtractionFunc")
	if !f.IsFunction() {
		return internal.KeyExtractionFuncResult{}, errors.New("keyExtractionFunc is not a function")
	}
	v, err := f.Call(f, cmd, args)
	if err != nil {
		return internal.KeyExtractionFuncResult{}, err
	}
	if !v.IsObject() {
		return internal.KeyExtractionFuncResult{}, errors.New("keyExtractionFunc return type is not an object")
	}
	data := v.Object()

	rk, _ := data.Get("readKeys")
	rkv, _ := rk.Export()
	readKeys, ok := rkv.([]string)
	if !ok {
		if _, ok = rkv.([]interface{}); !ok {
			return internal.KeyExtractionFuncResult{}, fmt.Errorf("readKeys for command %s is not an array", cmd[0])
		}
		readKeys = []string{}
	}

	wk, _ := data.Get("writeKeys")
	wkv, _ := wk.Export()
	writeKeys, ok := wkv.([]string)
	if !ok {
		if _, ok = wkv.([]interface{}); !ok {
			return internal.KeyExtractionFuncResult{}, fmt.Errorf("writeKeys for command %s is not an array", cmd[0])
		}
		writeKeys = []string{}
	}

	return internal.KeyExtractionFuncResult{
		Channels:  make([]string, 0),
		ReadKeys:  readKeys,
		WriteKeys: writeKeys,
	}, nil
}

// jsHandlerFunc executes the extraction function defined in the script nad returns the RESP response or error.
func (server *SugarDB) jsHandlerFunc(command string, args []string, params internal.HandlerFuncParams) ([]byte, error) {
	// Lock the script before executing the key extraction function.
	script, ok := server.scriptVMs.Load(strings.ToLower(command))
	if !ok {
		return nil, fmt.Errorf("no lock found for script command %s", command)
	}
	machine := script.(struct {
		vm   any
		lock *sync.Mutex
	})
	machine.lock.Lock()
	defer machine.lock.Unlock()

	vm := machine.vm.(*otto.Otto)

	f, _ := vm.Get("handlerFunc")
	if !f.IsFunction() {
		return nil, errors.New("handlerFunc is not a function")
	}
	v, err := f.Call(
		f,

		// Build context
		func() otto.Value {
			obj, _ := vm.Object(`({})`)
			_ = obj.Set("protocol", params.Context.Value("Protocol").(int))
			_ = obj.Set("database", params.Context.Value("Database").(int))
			return obj.Value()
		}(),

		// Command
		params.Command,

		// Build keysExist function
		func(keys []string) otto.Value {
			obj, _ := vm.Object(`({})`)
			exists := server.keysExist(params.Context, keys)
			for key, value := range exists {
				_ = obj.Set(key, value)
			}
			return obj.Value()
		},

		// Build getValues function
		func(keys []string) otto.Value {
			obj, _ := vm.Object(`({})`)
			values := server.getValues(params.Context, keys)
			for key, value := range values {
				switch value.(type) {
				default:
					_ = obj.Set(key, value)
				case nil:
					_ = obj.Set(key, otto.NullValue())
				case []string:
					l, _ := vm.Object(`([])`)
					for i, elem := range value.([]string) {
						_ = l.Set(fmt.Sprintf("%d", i), elem)
					}
					_ = obj.Set(key, l.Value())
				case hash.Hash:
					h, _ := vm.Object(`({})`)
					buildHashObject(h, value.(hash.Hash))
					_ = obj.Set(key, h.Value())
				case *set.Set:
					s, _ := vm.Object(`({})`)
					buildSetObject(s, value.(*set.Set))
					_ = obj.Set(key, s.Value())
				case *sorted_set.SortedSet:
					ss, _ := vm.Object(`({})`)
					buildSortedSetObject(ss, value.(*sorted_set.SortedSet))
					_ = obj.Set(key, ss.Value())
				}
			}
			return obj.Value()
		},

		// Build setValues function
		func(entries map[string]interface{}) {
			values := make(map[string]interface{})
			for key, entry := range entries {
				switch entry.(type) {
				default:
					panicInHandler(fmt.Sprintf("unknown type %s on key %s", reflect.TypeOf(entry).String(), key))
				case nil:
					values[key] = nil
				case string:
					values[key] = internal.AdaptType(entry.(string))
				case int64:
					values[key] = int(entry.(int64))
				case float64:
					values[key] = entry.(float64)
				case []string:
					values[key] = entry.([]string)
				case map[string]interface{}:
					value, ok := entry.(map[string]interface{})
					if !ok || value["__id"] == nil {
						panicInHandler(fmt.Sprintf("unknown object on key %s", key))
					}
					obj, exists := getObjectById(value["__id"].(string))
					if !exists {
						panicInHandler(
							fmt.Sprintf(
								"could not find object of id %s in the object registry on key %s",
								value["__id"].(string),
								key,
							),
						)
					}
					switch obj.(type) {
					default:
						panicInHandler(fmt.Sprintf("unknown type on key %s for command %s\n", key, command))
					case hash.Hash:
						values[key] = obj.(hash.Hash)
					case *set.Set:
						values[key] = obj.(*set.Set)
					case *sorted_set.SortedSet:
						values[key] = obj.(*sorted_set.SortedSet)
					}
				}
			}
			if err := server.setValues(params.Context, values); err != nil {
				panicInHandler(err.Error())
			}
		},

		// Args
		args,
	)
	if err != nil {
		return nil, err
	}
	res, err := v.ToString()

	clearObjectRegistry()

	return []byte(res), err
}

func buildHashObject(obj *otto.Object, h hash.Hash) {
	_ = obj.Set("__type", "hash")
	_ = obj.Set("__id", registerObject(h))
	_ = obj.Set("set", func(call otto.FunctionCall) otto.Value {
		args := call.Argument(0).Object()
		for _, key := range args.Keys() {
			value, _ := args.Get(key)
			v, _ := value.ToString()
			h[key] = hash.HashValue{Value: v}
		}
		// Return changed count using the set data type
		count, _ := otto.ToValue(set.NewSet(args.Keys()).Cardinality())
		return count
	})
	_ = obj.Set("setnx", func(call otto.FunctionCall) otto.Value {
		count := 0
		args := call.Argument(0).Object()
		for _, key := range args.Keys() {
			if _, exists := h[key]; exists {
				continue
			}
			count += 1
			value, _ := args.Get(key)
			v, _ := value.ToString()
			h[key] = hash.HashValue{Value: v}
		}
		c, _ := otto.ToValue(count)
		return c
	})
	_ = obj.Set("get", func(call otto.FunctionCall) otto.Value {
		result, _ := call.Otto.Object(`({})`)
		for _, arg := range call.ArgumentList {
			key, _ := arg.ToString()
			value, _ := otto.ToValue(h[key].Value)
			_ = result.Set(key, value)
		}
		return result.Value()
	})
	_ = obj.Set("len", func(call otto.FunctionCall) otto.Value {
		length, _ := otto.ToValue(len(h))
		return length
	})
	_ = obj.Set("all", func(call otto.FunctionCall) otto.Value {
		result, _ := call.Otto.Object(`({})`)
		for key, value := range h {
			v, _ := otto.ToValue(value.Value)
			_ = result.Set(key, v)
		}
		return result.Value()
	})
	_ = obj.Set("exists", func(call otto.FunctionCall) otto.Value {
		result, _ := call.Otto.Object(`({})`)
		for _, arg := range call.ArgumentList {
			key, _ := arg.ToString()
			_, ok := h[key]
			exists, _ := call.Otto.ToValue(ok)
			_ = result.Set(key, exists)
		}
		return result.Value()
	})
	_ = obj.Set("del", func(call otto.FunctionCall) otto.Value {
		count := 0
		for _, arg := range call.ArgumentList {
			key, _ := arg.ToString()
			if _, exists := h[key]; exists {
				count += 1
				delete(h, key)
			}
		}
		result, _ := otto.ToValue(count)
		return result
	})
}

func buildSetObject(obj *otto.Object, s *set.Set) {
	_ = obj.Set("__type", "set")
	_ = obj.Set("__id", registerObject(s))
	_ = obj.Set("add", func(call otto.FunctionCall) otto.Value {
		args := call.Argument(0).Object()
		if args == nil {
			panicWithFunctionCall(call, "set add method argument not an object")
		}
		var elems []string
		for _, key := range args.Keys() {
			value, _ := args.Get(key)
			v, _ := value.ToString()
			elems = append(elems, v)
		}
		count := s.Add(elems)
		result, _ := otto.ToValue(count)
		return result
	})
	_ = obj.Set("pop", func(call otto.FunctionCall) otto.Value {
		count, _ := call.Argument(0).ToInteger()
		popped := s.Pop(int(count))
		result, _ := call.Otto.Object(`([])`)
		_ = result.Set("length", len(popped))
		for i, p := range popped {
			_ = result.Set(fmt.Sprintf("%d", i), p)
		}
		return result.Value()
	})
	_ = obj.Set("contains", func(call otto.FunctionCall) otto.Value {
		value, _ := call.Argument(0).ToString()
		result, _ := otto.ToValue(s.Contains(value))
		return result
	})
	_ = obj.Set("cardinality", func(call otto.FunctionCall) otto.Value {
		result, _ := otto.ToValue(s.Cardinality())
		return result
	})
	_ = obj.Set("remove", func(call otto.FunctionCall) otto.Value {
		args := call.Argument(0).Object()
		if args == nil {
			panicWithFunctionCall(call, "set remove method argument not an object")
		}
		var elems []string
		for _, key := range args.Keys() {
			value, _ := args.Get(key)
			v, _ := value.ToString()
			elems = append(elems, v)
		}
		result, _ := otto.ToValue(s.Remove(elems))
		return result
	})
	_ = obj.Set("all", func(call otto.FunctionCall) otto.Value {
		all := s.GetAll()
		result, _ := call.Otto.Object(`([])`)
		_ = result.Set("length", len(all))
		for i, e := range all {
			_ = result.Set(fmt.Sprintf("%d", i), e)
		}
		return result.Value()
	})
	_ = obj.Set("random", func(call otto.FunctionCall) otto.Value {
		count, _ := call.Argument(0).ToInteger()
		random := s.GetRandom(int(count))
		result, _ := call.Otto.Object(`([])`)
		_ = result.Set("length", len(random))
		for i, r := range random {
			_ = result.Set(fmt.Sprintf("%d", i), r)
		}
		return result.Value()
	})
	_ = obj.Set("move", func(call otto.FunctionCall) otto.Value {
		arg := call.Argument(0).Object()
		elem := call.Argument(1).String()
		id, _ := arg.Get("__id")
		o, exists := getObjectById(id.String())
		if !exists {
			panicWithFunctionCall(call, "move target set does not exist")
		}
		switch o.(type) {
		default:
			panicWithFunctionCall(call, "move target is not a set")
		case *set.Set:
			moved := s.Move(o.(*set.Set), elem) == 1
			result, _ := otto.ToValue(moved)
			return result
		}
		return otto.NullValue()
	})
	_ = obj.Set("subtract", func(call otto.FunctionCall) otto.Value {
		extractSets := func(call otto.FunctionCall) ([]*set.Set, error) {
			var sets []*set.Set
			if len(call.ArgumentList) > 1 {
				return sets, fmt.Errorf("set subtract method expects 1 arg, got %d", len(call.ArgumentList))
			}
			arg1 := call.Argument(0).Object()
			if arg1.Class() != "Array" {
				return sets, errors.New("set subtract method expects the first argument to be an array")
			}
			for _, key := range arg1.Keys() {
				// Check if the array element is a valid MemberParam type.
				argMember, _ := arg1.Get(key)
				if !argMember.IsObject() {
					panicWithFunctionCall(call, "set subtract method first arg must be an array of valid sets")
				}
				// Get the member param from the object registry
				argMemberObj := argMember.Object()
				id, _ := argMemberObj.Get("__id")
				o, exists := getObjectById(id.String())
				if !exists {
					panicWithFunctionCall(call, "set subtract method first arg must be an array of valid sets")
				}
				m, ok := o.(*set.Set)
				if !ok {
					panicWithFunctionCall(call, "set subtract method first arg must be an array of valid sets")
				}
				sets = append(sets, m)
			}
			return sets, nil
		}
		sets, err := extractSets(call)
		if err != nil {
			panicWithFunctionCall(call, err.Error())
		}
		diff := s.Subtract(sets)
		result, _ := call.Otto.Object(`({})`)
		buildSetObject(result, diff)
		return result.Value()
	})
}

func buildMemberParamObject(obj *otto.Object, m *sorted_set.MemberParam) {
	_ = obj.Set("__type", "zmember")
	_ = obj.Set("__id", registerObject(m))
	_ = obj.Set("value", func(call otto.FunctionCall) otto.Value {
		switch len(call.ArgumentList) {
		case 0:
			// If no value is passed, then return the current value
			v, _ := otto.ToValue(m.Value)
			return v
		case 1:
			// If a value is passed, then set the value
			v := call.Argument(0).String()
			if len(v) <= 0 {
				panicWithFunctionCall(call, "zset member value must be a non-empty string")
			}
			m.Value = sorted_set.Value(v)
		default:
			panicWithFunctionCall(
				call,
				fmt.Sprintf(
					"expected either 0 or 1 args for value method of zmember, got %d",
					len(call.ArgumentList),
				),
			)
		}
		return otto.NullValue()
	})
	_ = obj.Set("score", func(call otto.FunctionCall) otto.Value {
		switch len(call.ArgumentList) {
		case 0:
			s, _ := otto.ToValue(m.Score)
			return s
		case 1:
			s, _ := call.Argument(0).ToFloat()
			if math.IsNaN(s) {
				panicWithFunctionCall(call, "zset member score must be a valid number")
			}
			m.Score = sorted_set.Score(s)
		default:
			panicWithFunctionCall(
				call,
				fmt.Sprintf(
					"expected either 0 or 1 args for score method of zmember, got %d",
					len(call.ArgumentList),
				),
			)
		}
		return otto.NullValue()
	})
}

func validateMemberParamObject(obj *otto.Object) error {
	value, _ := obj.Get("value")
	if slices.Contains([]otto.Value{otto.UndefinedValue(), otto.NullValue()}, value) ||
		len(value.String()) == 0 {
		return errors.New("zset member value must be a non-empty string")
	}
	s, _ := obj.Get("score")
	if slices.Contains([]otto.Value{otto.UndefinedValue(), otto.NullValue()}, s) {
		return errors.New("zset member must have a score")
	}
	score, _ := s.ToFloat()
	if math.IsNaN(score) {
		return errors.New("zset member score must be a valid number")
	}
	return nil
}

func buildSortedSetObject(obj *otto.Object, ss *sorted_set.SortedSet) {
	// Function to extract member param arguments for "add" and "update" methods.
	extractMembers := func(call otto.FunctionCall) ([]sorted_set.MemberParam, error) {
		var members []sorted_set.MemberParam
		if !call.Argument(0).IsObject() {
			return members, errors.New("zset add or update method expects the first argument to be an array")
		}
		arg1 := call.Argument(0).Object()
		if arg1.Class() != "Array" {
			return members, errors.New("zset add or update method expects the first argument to be an array")
		}
		for _, key := range arg1.Keys() {
			// Check if the array element is a valid MemberParam type.
			argMember, _ := arg1.Get(key)
			if !argMember.IsObject() {
				panicWithFunctionCall(call, "zset add or update method first arg must be an array of valid zmembers")
			}
			// Get the member param from the object registry
			argMemberObj := argMember.Object()
			id, _ := argMemberObj.Get("__id")
			o, exists := getObjectById(id.String())
			if !exists {
				panicWithFunctionCall(call, "zset add or update method first arg must be an array of valid zmembers")
			}
			m, ok := o.(*sorted_set.MemberParam)
			if !ok {
				panicWithFunctionCall(call, "zset add or update method first arg must be an array of valid zmembers")
			}
			members = append(members, *m)
		}
		return members, nil
	}

	// Function to build and verify the update policy for "add" and "update" methods
	type updateModifiers struct {
		updatePolicy interface{}
		comparison   interface{}
		changed      interface{}
		incr         interface{}
	}
	extractUpdateModifiers := func(call otto.FunctionCall) (updateModifiers, error) {
		modifiers := updateModifiers{updatePolicy: nil, comparison: nil, changed: nil, incr: nil}
		if len(call.ArgumentList) < 2 {
			return modifiers, nil
		}
		if !call.Argument(1).IsObject() {
			return modifiers, errors.New("zset add or update method second arg must be an object")
		}
		arg2 := call.Argument(1).Object()
		acceptedKeys := []string{"exists", "comparison", "changed", "incr"}
		for _, key := range arg2.Keys() {
			if !slices.Contains(acceptedKeys, key) {
				return modifiers, fmt.Errorf(
					"zset add or update method second arg unknown key '%s', expected %+v", key, acceptedKeys)
			}
			v, _ := arg2.Get(key)
			switch key {
			case "exists":
				if !v.IsBoolean() {
					return modifiers, errors.New("zset add or update method second arg 'exists' key should be a boolean")
				}
				exists, _ := v.ToBoolean()
				if exists {
					modifiers.updatePolicy = "xx"
				} else {
					modifiers.updatePolicy = "nx"
				}
			case "comparison":
				modifiers.comparison = v.String()
			case "changed":
				if !v.IsBoolean() {
					return modifiers, errors.New("zset add or update method second arg 'changed' key should be a boolean")
				}
				changed, _ := v.ToBoolean()
				modifiers.changed = changed
			case "incr":
				if !v.IsBoolean() {
					return modifiers, errors.New("zset add or update method second arg 'incr' key should be a boolean")
				}
				incr, _ := v.ToBoolean()
				modifiers.incr = incr
			}
		}
		return modifiers, nil
	}

	_ = obj.Set("__type", "zset")
	_ = obj.Set("__id", registerObject(ss))
	_ = obj.Set("add", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) < 1 || len(call.ArgumentList) > 2 {
			panicWithFunctionCall(call, fmt.Sprintf("zset add method expects 1 or 2 args, got %d", len(call.ArgumentList)))
		}
		// Extract the member params from the first arg
		members, err := extractMembers(call)
		if err != nil {
			panicWithFunctionCall(call, err.Error())
		}
		// Extract the modifiers in the second arg, if they are passed.
		modifiers, err := extractUpdateModifiers(call)
		if err != nil {
			panicWithFunctionCall(call, err.Error())
		}
		count, err := ss.AddOrUpdate(
			members,
			modifiers.updatePolicy,
			modifiers.comparison,
			modifiers.changed,
			modifiers.incr,
		)
		if err != nil {
			panicWithFunctionCall(call, err.Error())
		}
		v, _ := call.Otto.ToValue(count)
		return v
	})
	_ = obj.Set("update", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) < 1 || len(call.ArgumentList) > 2 {
			panicWithFunctionCall(call, fmt.Sprintf("zset update method expects 1 or 2 args, got %d", len(call.ArgumentList)))
		}
		// Extract the member params from the first arg
		members, err := extractMembers(call)
		if err != nil {
			panicWithFunctionCall(call, err.Error())
		}
		// Extract the modifiers in the second arg, if they are passed.
		modifiers, err := extractUpdateModifiers(call)
		if err != nil {
			panicWithFunctionCall(call, err.Error())
		}
		count, err := ss.AddOrUpdate(
			members,
			modifiers.updatePolicy,
			modifiers.comparison,
			modifiers.changed,
			modifiers.incr,
		)
		if err != nil {
			panicWithFunctionCall(call, err.Error())
		}
		v, _ := call.Otto.ToValue(count)
		return v
	})
	_ = obj.Set("remove", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) != 1 {
			panicWithFunctionCall(call, fmt.Sprintf("zset remove method expects 1 ard, got %d", len(call.ArgumentList)))
		}
		value := sorted_set.Value(call.Argument(0).String())
		v, _ := call.Otto.ToValue(ss.Remove(value))
		return v
	})
	_ = obj.Set("cardinality", func(call otto.FunctionCall) otto.Value {
		value, _ := otto.ToValue(ss.Cardinality())
		return value
	})
	_ = obj.Set("contains", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) != 1 {
			panicWithFunctionCall(call, fmt.Sprintf("zset contains method expects 1 arg, got %d", len(call.ArgumentList)))
		}
		v, _ := otto.ToValue(ss.Contains(sorted_set.Value(call.Argument(0).String())))
		return v
	})
	_ = obj.Set("random", func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) != 1 {
			panicWithFunctionCall(call, fmt.Sprintf("zset random method expects 1 arg, got %d", len(call.ArgumentList)))
		}
		count, _ := call.Argument(0).ToInteger()
		var paramValues []otto.Value
		for _, p := range ss.GetRandom(int(count)) {
			m, _ := call.Otto.Object(`({})`)
			buildMemberParamObject(m, &p)
			paramValues = append(paramValues, m.Value())
		}
		p, _ := call.Otto.ToValue(paramValues)
		return p
	})
	_ = obj.Set("all", func(call otto.FunctionCall) otto.Value {
		var paramValues []otto.Value
		for _, p := range ss.GetAll() {
			m, _ := call.Otto.Object(`({})`)
			buildMemberParamObject(m, &p)
			paramValues = append(paramValues, m.Value())
		}
		p, _ := call.Otto.ToValue(paramValues)
		return p
	})
	_ = obj.Set("subtract", func(call otto.FunctionCall) otto.Value {
		extractZSets := func(call otto.FunctionCall) ([]*sorted_set.SortedSet, error) {
			var zsets []*sorted_set.SortedSet
			if len(call.ArgumentList) > 1 {
				return zsets, fmt.Errorf("zset subtract method expects 1 arg, got %d", len(call.ArgumentList))
			}
			arg1 := call.Argument(0).Object()
			if arg1.Class() != "Array" {
				return zsets, errors.New("zset subtract method expects the first argument to be an array")
			}
			for _, key := range arg1.Keys() {
				// Check if the array element is a valid MemberParam type.
				argMember, _ := arg1.Get(key)
				if !argMember.IsObject() {
					panicWithFunctionCall(call, "zset subtract method first arg must be an array of valid zsets")
				}
				// Get the member param from the object registry
				argMemberObj := argMember.Object()
				id, _ := argMemberObj.Get("__id")
				o, exists := getObjectById(id.String())
				if !exists {
					panicWithFunctionCall(call, "zset subtract method first arg must be an array of valid zsets")
				}
				m, ok := o.(*sorted_set.SortedSet)
				if !ok {
					panicWithFunctionCall(call, "zset subtract method first arg must be an array of valid zsets")
				}
				zsets = append(zsets, m)
			}
			return zsets, nil
		}
		zsets, err := extractZSets(call)
		if err != nil {
			panicWithFunctionCall(call, err.Error())
		}
		diff := ss.Subtract(zsets)
		result, _ := call.Otto.Object(`({})`)
		buildSortedSetObject(result, diff)
		return result.Value()
	})
}

func panicWithFunctionCall(call otto.FunctionCall, message string) {
	err, _ := call.Otto.ToValue(message)
	panic(err)
}

func panicInHandler(message string) {
	value, _ := otto.ToValue(message)
	panic(value)
}
