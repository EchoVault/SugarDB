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
