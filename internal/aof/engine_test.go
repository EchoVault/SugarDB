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

package aof_test

import (
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/aof"
	"github.com/echovault/echovault/internal/aof/log"
	"github.com/echovault/echovault/internal/aof/preamble"
	"github.com/echovault/echovault/internal/clock"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func marshalRespCommand(command []string) []byte {
	return []byte(fmt.Sprintf(
		"*%d\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s", len(command),
		len(command[0]), command[0],
		len(command[1]), command[1],
		len(command[2]), command[2],
	))
}

func Test_AOFEngine(t *testing.T) {
	strategy := "always"
	directory := "./testdata"

	var rewriteInProgress atomic.Bool
	startRewriteFunc := func() {
		if rewriteInProgress.Load() {
			t.Error("expected rewriteInProgress to be false, got true")
		}
		rewriteInProgress.Store(true)
	}
	finishRewriteFunc := func() {
		if !rewriteInProgress.Load() {
			t.Error("expected rewriteInProgress to be true, got false")
			rewriteInProgress.Store(false)
		}
	}

	state := map[string]internal.KeyData{
		"key1": {Value: "value1", ExpireAt: clock.NewClock().Now().Add(10 * time.Second)},
		"key2": {Value: "value2", ExpireAt: clock.NewClock().Now().Add(-10 * time.Second)}, // Should be excluded on restore
		"key3": {Value: "value3", ExpireAt: clock.NewClock().Now().Add(10 * time.Second)},
	}
	restoredState := map[string]internal.KeyData{}
	wantRestoredState := map[string]internal.KeyData{
		"key1":  {Value: "value1", ExpireAt: clock.NewClock().Now().Add(10 * time.Second)},
		"key3":  {Value: "value3", ExpireAt: clock.NewClock().Now().Add(10 * time.Second)},
		"key4":  {Value: "value4", ExpireAt: time.Time{}},
		"key5":  {Value: "value5", ExpireAt: time.Time{}},
		"key6":  {Value: "value6", ExpireAt: time.Time{}},
		"key7":  {Value: "value7", ExpireAt: time.Time{}},
		"key8":  {Value: "value8", ExpireAt: time.Time{}},
		"key9":  {Value: "value9", ExpireAt: time.Time{}},
		"key10": {Value: "value10", ExpireAt: time.Time{}},
	}
	getStateFunc := func() map[string]internal.KeyData {
		return state
	}
	setKeyDataFunc := func(key string, data internal.KeyData) {
		restoredState[key] = data
	}
	handleCommandFunc := func(command []byte) {
		cmd, err := internal.Decode(command)
		if err != nil {
			t.Error(err)
		}
		restoredState[cmd[1]] = internal.KeyData{Value: cmd[2], ExpireAt: time.Time{}}
	}

	preambleReadWriter := func() preamble.PreambleReadWriter {
		return nil
	}()
	appendReadWriter := func() log.AppendReadWriter {
		return nil
	}()

	engine, err := aof.NewAOFEngine(
		aof.WithClock(clock.NewClock()),
		aof.WithStrategy(strategy),
		aof.WithDirectory(directory),
		aof.WithStartRewriteFunc(startRewriteFunc),
		aof.WithFinishRewriteFunc(finishRewriteFunc),
		aof.WithGetStateFunc(getStateFunc),
		aof.WithSetKeyDataFunc(setKeyDataFunc),
		aof.WithHandleCommandFunc(handleCommandFunc),
		aof.WithPreambleReadWriter(preambleReadWriter),
		aof.WithAppendReadWriter(appendReadWriter),
	)
	if err != nil {
		t.Error(err)
	}

	// Log some commands to mutate the state
	preRewriteCommands := [][]string{
		{"SET", "key4", "value4"},
		{"SET", "key5", "value5"},
		{"SET", "key6", "value6"},
	}
	for _, command := range preRewriteCommands {
		state[command[1]] = internal.KeyData{Value: command[2], ExpireAt: time.Time{}}
		engine.QueueCommand(marshalRespCommand(command))
	}
	<-time.After(100 * time.Millisecond)

	// Trigger log rewrite
	if err = engine.RewriteLog(); err != nil {
		t.Error(err)
	}

	// Log some mode commands
	postRewriteCommands := [][]string{
		{"SET", "key7", "value7"},
		{"SET", "key8", "value8"},
		{"SET", "key9", "value9"},
		{"SET", "key10", "value10"},
	}
	for _, command := range postRewriteCommands {
		state[command[1]] = internal.KeyData{Value: command[2], ExpireAt: time.Time{}}
		engine.QueueCommand(marshalRespCommand(command))
	}
	<-time.After(100 * time.Millisecond)

	// Restore logs
	if err = engine.Restore(); err != nil {
		t.Error(err)
	}

	if len(wantRestoredState) != len(restoredState) {
		t.Errorf("expected restored state to be lenght %d, got %d", len(wantRestoredState), len(restoredState))
		for key, data := range restoredState {
			want, ok := wantRestoredState[key]
			if !ok {
				t.Errorf("could not find key %s in expected state state", key)
			}
			if want.Value != data.Value {
				t.Errorf("expected value %v for key %s, got %v", want.Value, key, data.Value)
			}
			if !want.ExpireAt.Equal(data.ExpireAt) {
				t.Errorf("expected expiry time of %v for key %s, got %v", want.ExpireAt, key, data.ExpireAt)
			}
		}
	}

	_ = os.RemoveAll(directory)
}
