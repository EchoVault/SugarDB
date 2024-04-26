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

package types

import (
	"context"
	"net"
	"time"
)

type EchoVault interface {
	KeyLock(ctx context.Context, key string) (bool, error)
	KeyUnlock(ctx context.Context, key string)
	KeyRLock(ctx context.Context, key string) (bool, error)
	KeyRUnlock(ctx context.Context, key string)
	KeyExists(ctx context.Context, key string) bool
	CreateKeyAndLock(ctx context.Context, key string) (bool, error)
	GetValue(ctx context.Context, key string) interface{}
	SetValue(ctx context.Context, key string, value interface{}) error
	GetExpiry(ctx context.Context, key string) time.Time
	SetExpiry(ctx context.Context, key string, expire time.Time, touch bool)
	RemoveExpiry(ctx context.Context, key string)
	DeleteKey(ctx context.Context, key string) error
}

type PluginAccessKeys struct {
	ReadKeys  []string
	WriteKeys []string
}
type PluginKeyExtractionFunc func(cmd []string) (PluginAccessKeys, error)

type PluginHandlerFunc func(params PluginHandlerFuncParams) ([]byte, error)
type PluginHandlerFuncParams struct {
	Context          context.Context
	Command          []string
	Connection       *net.Conn
	KeyLock          func(ctx context.Context, key string) (bool, error)
	KeyUnlock        func(ctx context.Context, key string)
	KeyRLock         func(ctx context.Context, key string) (bool, error)
	KeyRUnlock       func(ctx context.Context, key string)
	KeyExists        func(ctx context.Context, key string) bool
	CreateKeyAndLock func(ctx context.Context, key string) (bool, error)
	GetValue         func(ctx context.Context, key string) interface{}
	SetValue         func(ctx context.Context, key string, value interface{}) error
	GetExpiry        func(ctx context.Context, key string) time.Time
	SetExpiry        func(ctx context.Context, key string, expire time.Time, touch bool)
	RemoveExpiry     func(ctx context.Context, key string)
	DeleteKey        func(ctx context.Context, key string) error
}
