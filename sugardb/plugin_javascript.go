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
	"github.com/robertkrimen/otto"
	"log"
	"os"
	"strings"
	"sync"
)

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

	// TODO: Register hash data type

	// TODO: Register set data type

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
			// TODO: Set custom type values in the store
			err := server.setValues(params.Context, entries)
			if err != nil {
				log.Printf("set value error for command %s: %+v", command, err)
			}
		},

		// Args
		args,
	)
	if err != nil {
		return nil, err
	}
	res, err := v.ToString()

	return []byte(res), err
}
