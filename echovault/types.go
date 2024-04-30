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

package echovault

import (
	"context"
	"net"
)

// CommandKeyExtractionFuncResult specifies the keys accessed by the associated command or subcommand.
// ReadKeys is a string slice containing the keys that the commands read from.
// WriteKeys is a string slice containing the keys that the command writes to.
//
// These keys will typically be extracted from the command slice, but they can also be hardcoded.
type CommandKeyExtractionFuncResult struct {
	ReadKeys  []string
	WriteKeys []string
}

// CommandKeyExtractionFunc if the function that extracts the keys accessed by the command or subcommand.
type CommandKeyExtractionFunc func(cmd []string) (CommandKeyExtractionFuncResult, error)

// CommandHandlerFunc is the handler function for the command or subcommand.
//
// This function must return a byte slice containing a valid RESP2 response, or an error.
type CommandHandlerFunc func(params CommandHandlerFuncParams) ([]byte, error)

// CommandHandlerFuncParams contains the helper parameters passed to the command's handler by EchoVault.
//
// Command is the string slice command containing the command that triggered this handler.
//
// Connection is the TCP connection that triggered this command. In embedded mode, this will always be nil.
// Any TCP client that trigger the custom command will have its connection passed to the handler here.
//
// KeyExists returns true if the key passed to it exists in the store.
//
// CreateKeyAndLock creates the new key and immediately write locks it. If the key already exists, then
// it is simply write locked which makes this function safe to call even if the key already exists. Always call
// KeyUnlock when done after CreateKeyAndLock.
//
// KeyLock acquires a write lock for the specified key. If the lock is successfully acquired, the function will return
// (true, nil). Otherwise, it will return false and an error describing why the locking failed. Always call KeyUnlock
// when done after KeyLock.
//
// KeyUnlock releases the write lock for the specified key. Always call this after KeyLock otherwise the key will not be
// lockable by any future invocations of this command or other commands.
//
// KeyRLock acquires a read lock for the specified key. If the lock is successfully acquired, the function will return
// (true, nil). Otherwise, it will return false and an error describing why the locking failed. Always call KeyRUnlock
// when done after KeyRLock.
//
// KeyRUnlock releases the real lock for the specified key. Always call this after KeyRLock otherwise the key will not be
// write-lockable by any future invocations of this command or other commands.
//
// GetValue returns the value held at the specified key as an interface{}. Make sure to invoke KeyLock or KeyRLock on the
// key before GetValue to ensure thread safety.
//
// SetValue sets the value at the specified key. Make sure to invoke KeyLock on the key before
// SetValue to ensure thread safety.
type CommandHandlerFuncParams struct {
	Context          context.Context
	Command          []string
	Connection       *net.Conn
	KeyExists        func(ctx context.Context, key string) bool
	CreateKeyAndLock func(ctx context.Context, key string) (bool, error)
	KeyLock          func(ctx context.Context, key string) (bool, error)
	KeyUnlock        func(ctx context.Context, key string)
	KeyRLock         func(ctx context.Context, key string) (bool, error)
	KeyRUnlock       func(ctx context.Context, key string)
	GetValue         func(ctx context.Context, key string) interface{}
	SetValue         func(ctx context.Context, key string, value interface{}) error
}
