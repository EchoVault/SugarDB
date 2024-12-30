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
	"log"
	"os"
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
	_ = vm.Set("createHash", func(call otto.FunctionCall) otto.Value {
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

		obj, _ := vm.Object(`({})`)
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
			result, _ := vm.Object(`({})`)
			for _, arg := range call.ArgumentList {
				key, _ := arg.ToString()
				value, _ := otto.ToValue(h[key].Value)
				_ = result.Set(key, value)
			}
			return result.Value()
		})
		_ = obj.Set("length", func(call otto.FunctionCall) otto.Value {
			length, _ := otto.ToValue(len(h))
			return length
		})
		_ = obj.Set("all", func(call otto.FunctionCall) otto.Value {
			result, _ := vm.Object(`({})`)
			for key, value := range h {
				v, _ := otto.ToValue(value.Value)
				_ = result.Set(key, v)
			}
			return result.Value()
		})
		_ = obj.Set("exists", func(call otto.FunctionCall) otto.Value {
			result, _ := vm.Object(`({})`)
			for _, arg := range call.ArgumentList {
				key, _ := arg.ToString()
				_, ok := h[key]
				exists, _ := vm.ToValue(ok)
				_ = result.Set(key, exists)
			}
			return result.Value()
		})
		_ = obj.Set("delete", func(call otto.FunctionCall) otto.Value {
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
		return obj.Value()
	})

	// Register set data type
	_ = vm.Set("createSet", func(call otto.FunctionCall) otto.Value {
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

		obj, _ := vm.Object(`({})`)
		_ = obj.Set("__type", "set")
		_ = obj.Set("__id", registerObject(s))
		_ = obj.Set("add", func(call otto.FunctionCall) otto.Value {
			args := call.Argument(0).Object()
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
			result, _ := vm.Object(`([])`)
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
			result, _ := vm.Object(`([])`)
			_ = result.Set("length", len(all))
			for i, e := range all {
				_ = result.Set(fmt.Sprintf("%d", i), e)
			}
			return result.Value()
		})
		_ = obj.Set("random", func(call otto.FunctionCall) otto.Value {
			count, _ := call.Argument(0).ToInteger()
			result, _ := otto.ToValue(s.GetRandom(int(count)))
			return result
		})
		_ = obj.Set("move", func(call otto.FunctionCall) otto.Value {
			arg := call.Argument(0).Object()
			elem := call.Argument(1).String()
			id, _ := arg.Get("__id")
			o, exists := getObjectById(id.String())
			if !exists {
				result, _ := otto.ToValue(false)
				return result
			}
			switch o.(type) {
			default:
				result, _ := otto.ToValue(false)
				return result
			case *set.Set:
				moved := s.Move(o.(*set.Set), elem) == 1
				result, _ := otto.ToValue(moved)
				return result
			}
		})
		// TODO: Implement set subtraction feature
		// _ = obj.Set("subtract", func(call otto.FunctionCall) otto.Value {})
		return obj.Value()
	})

	// TODO: Register sorted set member data type

	// TODO: Register sorted set data type

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
				// TODO: Add conditional statement for converting custom types to javascript types
				_ = obj.Set(key, value)
			}
			return obj.Value()
		},

		// Build setValues function
		func(entries map[string]interface{}) {
			values := make(map[string]interface{})
			for key, entry := range entries {
				switch entry.(type) {
				case string:
					values[key] = internal.AdaptType(entry.(string))
				case map[string]interface{}:
					value := entry.(map[string]interface{})
					obj, exists := getObjectById(value["__id"].(string))
					if !exists {
						continue
					}
					switch obj.(type) {
					default:
						log.Printf("unknown type on key %s for command %s\n", key, command)
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
				log.Printf("setValues error for command %s: %+v\n", command, err)
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
