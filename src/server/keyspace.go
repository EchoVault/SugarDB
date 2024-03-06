package server

import (
	"context"
	"errors"
	"github.com/echovault/echovault/src/utils"
	"slices"
	"strings"
	"sync"
	"time"
)

// KeyLock tries to acquire the write lock for the specified key every 5 milliseconds.
// If the context passed to the function finishes before the lock is acquired, an error is returned.
func (server *Server) KeyLock(ctx context.Context, key string) (bool, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	for {
		select {
		default:
			ok := server.keyLocks[key].TryLock()
			if ok {
				return true, nil
			}
		case <-ctx.Done():
			return false, context.Cause(ctx)
		}
		<-ticker.C
	}
}

func (server *Server) KeyUnlock(key string) {
	server.keyLocks[key].Unlock()
}

// KeyRLock tries to acquire the read lock for the specified key every few milliseconds.
// If the context passed to the function finishes before the lock is acquired, an error is returned.
func (server *Server) KeyRLock(ctx context.Context, key string) (bool, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	for {
		select {
		default:
			ok := server.keyLocks[key].TryRLock()
			if ok {
				return true, nil
			}
		case <-ctx.Done():
			return false, context.Cause(ctx)
		}
		<-ticker.C
	}
}

func (server *Server) KeyRUnlock(key string) {
	server.keyLocks[key].RUnlock()
}

func (server *Server) KeyExists(key string) bool {
	return server.keyLocks[key] != nil
}

// CreateKeyAndLock creates a new key lock and immediately locks it if the key does not exist.
// If the key exists, the existing key is locked.
func (server *Server) CreateKeyAndLock(ctx context.Context, key string) (bool, error) {
	if utils.IsMaxMemoryExceeded(server.Config) && server.Config.EvictionPolicy == utils.NoEviction {
		return false, errors.New("max memory reached, key not created")
	}

	server.keyCreationLock.Lock()
	defer server.keyCreationLock.Unlock()

	if !server.KeyExists(key) {
		keyLock := &sync.RWMutex{}
		keyLock.Lock()
		server.keyLocks[key] = keyLock
		return true, nil
	}

	return server.KeyLock(ctx, key)
}

// GetValue retrieves the current value at the specified key.
// The key must be read-locked before calling this function.
func (server *Server) GetValue(key string) interface{} {
	server.updateKeyInCache(key)
	return server.store[key]
}

// SetValue updates the value in the store at the specified key with the given value.
// If we're in not in cluster (i.e. in standalone mode), then the change count is incremented
// in the snapshot engine.
// This count triggers a snapshot when the threshold is reached.
// The key must be locked prior to calling this function.
func (server *Server) SetValue(_ context.Context, key string, value interface{}) error {
	if utils.IsMaxMemoryExceeded(server.Config) && server.Config.EvictionPolicy == utils.NoEviction {
		return errors.New("max memory reached, key value not set")
	}

	server.store[key] = value

	server.updateKeyInCache(key)

	if !server.IsInCluster() {
		server.SnapshotEngine.IncrementChangeCount()
	}

	return nil
}

// The SetKeyExpiry receiver function sets the expiry time of a key.
// The key parameter represents the key whose expiry time is to be set/updated.
// The expire parameter is the new expiry time.
// The touch parameter determines whether to update the keys access count on lfu eviction policy,
// or the access time on lru eviction policy.
// The key must be locked prior to calling this function.
func (server *Server) SetKeyExpiry(key string, expire time.Time, touch bool) {
	server.keyExpiry[key] = expire
	if touch {
		server.updateKeyInCache(key)
	}
}

// RemoveKeyExpiry is called by commands that remove key expiry (e.g. PERSIST).
// The key must be locked prior ro calling this function.
func (server *Server) RemoveKeyExpiry(key string) {
	server.keyExpiry[key] = time.Time{}
	switch {
	case slices.Contains([]string{utils.AllKeysLFU, utils.VolatileLFU}, server.Config.EvictionPolicy):
		server.lfuCache.Delete(key)
	case slices.Contains([]string{utils.AllKeysLRU, utils.VolatileLRU}, server.Config.EvictionPolicy):
		server.lruCache.Delete(key)
	}
}

// GetState creates a deep copy of the store map.
// It is used to retrieve the current state for persistence but can also be used for other
// functions that require a deep copy of the state.
// The copy only starts when there's no current copy in progress (represented by StateCopyInProgress atomic boolean)
// and when there's no current state mutation in progress (represented by StateMutationInProgress atomic boolean)
func (server *Server) GetState() map[string]interface{} {
	for {
		if !server.StateCopyInProgress.Load() && !server.StateMutationInProgress.Load() {
			server.StateCopyInProgress.Store(true)
			break
		}
	}
	data := make(map[string]interface{})
	for k, v := range server.store {
		data[k] = v
	}
	server.StateCopyInProgress.Store(false)
	return data
}

// updateKeyInCache updates either the key access count or the most recent access time in the cache
// depending on whether an LFU or LRU strategy was used.
func (server *Server) updateKeyInCache(key string) {
	switch strings.ToLower(server.Config.EvictionPolicy) {
	case utils.AllKeysLFU:
		server.lfuCache.Update(key)
	case utils.AllKeysLRU:
		server.lruCache.Update(key)
	case utils.VolatileLFU:
		if _, ok := server.keyExpiry[key]; ok {
			server.lfuCache.Update(key)
		}
	case utils.VolatileLRU:
		if _, ok := server.keyExpiry[key]; ok {
			server.lruCache.Update(key)
		}
	}
	// TODO: Check if memory usage is above max-memory. If it is, pop items from the cache until we get under the limit.
}
