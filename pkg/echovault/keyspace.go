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
	"errors"
	"fmt"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/pkg/constants"
	"log"
	"math/rand"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

// KeyLock tries to acquire the write lock for the specified key.
// If the context passed to the function finishes before the lock is acquired, an error is returned.
//
// If this functions is called on a node in a replication cluster, the key is only locked
// on that particular node.
func (server *EchoVault) KeyLock(ctx context.Context, key string) (bool, error) {
	// If context did not set deadline, set the default deadline
	var cancelFunc context.CancelFunc
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancelFunc = context.WithTimeoutCause(ctx, 250*time.Millisecond, fmt.Errorf("timeout for key %s", key))
		defer cancelFunc()
	}
	// Attempt to acquire the lock until lock is acquired or deadline is reached.
	for {
		select {
		default:
			if server.keyLocks[key] == nil {
				return false, fmt.Errorf("key %s not found", key)
			}
			ok := server.keyLocks[key].TryLock()
			if ok {
				return true, nil
			}
		case <-ctx.Done():
			return false, context.Cause(ctx)
		}
	}
}

// KeyUnlock releases the write lock for the specified key.
//
// If this functions is called on a node in a replication cluster, the key is only unlocked
// on that particular node.
func (server *EchoVault) KeyUnlock(_ context.Context, key string) {
	if _, ok := server.keyLocks[key]; ok {
		server.keyLocks[key].Unlock()
	}
}

// KeyRLock tries to acquire the read lock for the specified key.
// If the context passed to the function finishes before the lock is acquired, an error is returned.
//
// If this functions is called on a node in a replication cluster, the key is only locked
// on that particular node.
func (server *EchoVault) KeyRLock(ctx context.Context, key string) (bool, error) {
	// If context did not set deadline, set the default deadline
	var cancelFunc context.CancelFunc
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancelFunc = context.WithTimeoutCause(ctx, 250*time.Millisecond, fmt.Errorf("timeout for key %s", key))
		defer cancelFunc()
	}
	// Attempt to acquire the lock until lock is acquired or deadline is reached.
	for {
		select {
		default:
			if server.keyLocks[key] == nil {
				return false, fmt.Errorf("key %s not found", key)
			}
			ok := server.keyLocks[key].TryRLock()
			if ok {
				return true, nil
			}
		case <-ctx.Done():
			return false, context.Cause(ctx)
		}
	}
}

// KeyRUnlock releases the read lock for the specified key.
//
// If this functions is called on a node in a replication cluster, the key is only unlocked
// on that particular node.
func (server *EchoVault) KeyRUnlock(_ context.Context, key string) {
	if _, ok := server.keyLocks[key]; ok {
		server.keyLocks[key].RUnlock()
	}
}

// KeyExists returns true if the key exists in the store.
//
// If the key is volatile and expired, checking for its existence with KeyExists will trigger a key deletion and
// then return false. If the key is determined to be expired by KeyExists, it will be evicted across the entire
// replication cluster.
func (server *EchoVault) KeyExists(ctx context.Context, key string) bool {
	entry, ok := server.store[key]
	if !ok {
		return false
	}

	if entry.ExpireAt != (time.Time{}) && entry.ExpireAt.Before(server.clock.Now()) {
		if !server.isInCluster() {
			// If in standalone mode, delete the key directly.
			err := server.DeleteKey(ctx, key)
			if err != nil {
				log.Printf("keyExists: %+v\n", err)
			}
		} else if server.isInCluster() && server.raft.IsRaftLeader() {
			// If we're in a raft cluster, and we're the leader, send command to delete the key in the cluster.
			err := server.raftApplyDeleteKey(ctx, key)
			if err != nil {
				log.Printf("keyExists: %+v\n", err)
			}
		} else if server.isInCluster() && !server.raft.IsRaftLeader() {
			// Forward message to leader to initiate key deletion.
			// This is always called regardless of ForwardCommand config value
			// because we always want to remove expired keys.
			server.memberList.ForwardDeleteKey(ctx, key)
		}

		return false
	}

	return true
}

// CreateKeyAndLock creates a new key lock and immediately locks it if the key does not exist.
// If the key exists, the existing key is locked.
//
// If this functions is called on a node in a replication cluster, the key is only created/locked
// on that particular node.
func (server *EchoVault) CreateKeyAndLock(ctx context.Context, key string) (bool, error) {
	if internal.IsMaxMemoryExceeded(server.config.MaxMemory) && server.config.EvictionPolicy == constants.NoEviction {
		return false, errors.New("max memory reached, key not created")
	}

	server.keyCreationLock.Lock()
	defer server.keyCreationLock.Unlock()

	if !server.KeyExists(ctx, key) {
		// Create Lock
		keyLock := &sync.RWMutex{}
		keyLock.Lock()
		server.keyLocks[key] = keyLock
		// Create key entry
		server.store[key] = internal.KeyData{
			Value:    nil,
			ExpireAt: time.Time{},
		}
		return true, nil
	}

	return server.KeyLock(ctx, key)
}

// GetValue retrieves the current value at the specified key.
// The key must be read-locked before calling this function.
func (server *EchoVault) GetValue(ctx context.Context, key string) interface{} {
	if err := server.updateKeyInCache(ctx, key); err != nil {
		log.Printf("GetValue error: %+v\n", err)
	}
	return server.store[key].Value
}

// SetValue updates the value in the store at the specified key with the given value.
// If we're in not in cluster (i.e. in standalone mode), then the change count is incremented in the snapshot engine.
// This count triggers a snapshot when the threshold is reached.
// The key must be locked prior to calling this function.
func (server *EchoVault) SetValue(ctx context.Context, key string, value interface{}) error {
	if internal.IsMaxMemoryExceeded(server.config.MaxMemory) && server.config.EvictionPolicy == constants.NoEviction {
		return errors.New("max memory reached, key value not set")
	}

	server.store[key] = internal.KeyData{
		Value:    value,
		ExpireAt: server.store[key].ExpireAt,
	}

	err := server.updateKeyInCache(ctx, key)
	if err != nil {
		log.Printf("SetValue error: %+v\n", err)
	}

	if !server.isInCluster() {
		server.snapshotEngine.IncrementChangeCount()
	}

	return nil
}

// The GetExpiry function returns the expiry time associated with the provided key.
// The key must be read locked before calling this function.
func (server *EchoVault) GetExpiry(ctx context.Context, key string) time.Time {
	if err := server.updateKeyInCache(ctx, key); err != nil {
		log.Printf("GetKeyExpiry error: %+v\n", err)
	}
	return server.store[key].ExpireAt
}

// The SetExpiry receiver function sets the expiry time of a key.
// The key parameter represents the key whose expiry time is to be set/updated.
// The expireAt parameter is the new expiry time.
// The touch parameter determines whether to update the keys access count on lfu eviction policy,
// or the access time on lru eviction policy.
// The key must be locked prior to calling this function.
func (server *EchoVault) SetExpiry(ctx context.Context, key string, expireAt time.Time, touch bool) {
	server.store[key] = internal.KeyData{
		Value:    server.store[key].Value,
		ExpireAt: expireAt,
	}

	// If the slice of keys associated with expiry time does not contain the current key, add the key.
	server.keysWithExpiry.rwMutex.Lock()
	if !slices.Contains(server.keysWithExpiry.keys, key) {
		server.keysWithExpiry.keys = append(server.keysWithExpiry.keys, key)
	}
	server.keysWithExpiry.rwMutex.Unlock()

	// If touch is true, update the keys status in the cache.
	if touch {
		err := server.updateKeyInCache(ctx, key)
		if err != nil {
			log.Printf("SetKeyExpiry error: %+v\n", err)
		}
	}
}

// RemoveExpiry is called by commands that remove key expiry (e.g. PERSIST).
// The key must be locked prior ro calling this function.
func (server *EchoVault) RemoveExpiry(key string) {
	// Reset expiry time
	server.store[key] = internal.KeyData{
		Value:    server.store[key].Value,
		ExpireAt: time.Time{},
	}
	// Remove key from slice of keys associated with expiry
	server.keysWithExpiry.rwMutex.Lock()
	defer server.keysWithExpiry.rwMutex.Unlock()
	server.keysWithExpiry.keys = slices.DeleteFunc(server.keysWithExpiry.keys, func(k string) bool {
		return k == key
	})
}

// GetState creates a deep copy of the store map.
// It is used to retrieve the current state for persistence but can also be used for other
// functions that require a deep copy of the state.
// The copy only starts when there's no current copy in progress (represented by stateCopyInProgress atomic boolean)
// and when there's no current state mutation in progress (represented by stateMutationInProgress atomic boolean)
func (server *EchoVault) getState() map[string]interface{} {
	// Wait unit there's no state mutation or copy in progress before starting a new copy process.
	for {
		if !server.stateCopyInProgress.Load() && !server.stateMutationInProgress.Load() {
			server.stateCopyInProgress.Store(true)
			break
		}
	}
	data := make(map[string]interface{})
	for k, v := range server.store {
		data[k] = v
	}
	server.stateCopyInProgress.Store(false)
	return data
}

// DeleteKey removes the key from store, keyLocks and keyExpiry maps.
//
// If this functions is called on a node in a replication cluster, the key is only deleted
// on that particular node.
func (server *EchoVault) DeleteKey(ctx context.Context, key string) error {
	if _, err := server.KeyLock(ctx, key); err != nil {
		return fmt.Errorf("deleteKey error: %+v", err)
	}

	// Remove key expiry.
	server.RemoveExpiry(key)

	// Delete the key from keyLocks and store.
	delete(server.keyLocks, key)
	delete(server.store, key)

	// Remove the key from the cache.
	switch {
	case slices.Contains([]string{constants.AllKeysLFU, constants.VolatileLFU}, server.config.EvictionPolicy):
		server.lfuCache.cache.Delete(key)
	case slices.Contains([]string{constants.AllKeysLRU, constants.VolatileLRU}, server.config.EvictionPolicy):
		server.lruCache.cache.Delete(key)
	}

	log.Printf("deleted key %s\n", key)

	return nil
}

// updateKeyInCache updates either the key access count or the most recent access time in the cache
// depending on whether an LFU or LRU strategy was used.
func (server *EchoVault) updateKeyInCache(ctx context.Context, key string) error {
	// Only update cache when in standalone mode or when raft leader
	if server.isInCluster() || (server.isInCluster() && !server.raft.IsRaftLeader()) {
		return nil
	}
	// If max memory is 0, there's no max so no need to update caches
	if server.config.MaxMemory == 0 {
		return nil
	}
	switch strings.ToLower(server.config.EvictionPolicy) {
	case constants.AllKeysLFU:
		server.lfuCache.mutex.Lock()
		defer server.lfuCache.mutex.Unlock()
		server.lfuCache.cache.Update(key)
	case constants.AllKeysLRU:
		server.lruCache.mutex.Lock()
		defer server.lruCache.mutex.Unlock()
		server.lruCache.cache.Update(key)
	case constants.VolatileLFU:
		server.lfuCache.mutex.Lock()
		defer server.lfuCache.mutex.Unlock()
		if server.store[key].ExpireAt != (time.Time{}) {
			server.lfuCache.cache.Update(key)
		}
	case constants.VolatileLRU:
		server.lruCache.mutex.Lock()
		defer server.lruCache.mutex.Unlock()
		if server.store[key].ExpireAt != (time.Time{}) {
			server.lruCache.cache.Update(key)
		}
	}
	if err := server.adjustMemoryUsage(ctx); err != nil {
		return fmt.Errorf("updateKeyInCache: %+v", err)
	}
	return nil
}

// adjustMemoryUsage should only be called from standalone echovault or from raft cluster leader.
func (server *EchoVault) adjustMemoryUsage(ctx context.Context) error {
	// If max memory is 0, there's no need to adjust memory usage.
	if server.config.MaxMemory == 0 {
		return nil
	}
	// Check if memory usage is above max-memory.
	// If it is, pop items from the cache until we get under the limit.
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	// If we're using less memory than the max-memory, there's no need to evict.
	if memStats.HeapInuse < server.config.MaxMemory {
		return nil
	}
	// Force a garbage collection first before we start evicting key.
	runtime.GC()
	runtime.ReadMemStats(&memStats)
	if memStats.HeapInuse < server.config.MaxMemory {
		return nil
	}
	// We've done a GC, but we're still at or above the max memory limit.
	// Start a loop that evicts keys until either the heap is empty or
	// we're below the max memory limit.
	switch {
	case slices.Contains([]string{constants.AllKeysLFU, constants.VolatileLFU}, strings.ToLower(server.config.EvictionPolicy)):
		// Remove keys from LFU cache until we're below the max memory limit or
		// until the LFU cache is empty.
		server.lfuCache.mutex.Lock()
		defer server.lfuCache.mutex.Unlock()
		for {
			// Return if cache is empty
			if server.lfuCache.cache.Len() == 0 {
				return fmt.Errorf("adjsutMemoryUsage -> LFU cache empty")
			}

			key := server.lfuCache.cache.Pop().(string)
			if !server.isInCluster() {
				// If in standalone mode, directly delete the key
				if err := server.DeleteKey(ctx, key); err != nil {
					return fmt.Errorf("adjustMemoryUsage -> LFU cache eviction: %+v", err)
				}
			} else if server.isInCluster() && server.raft.IsRaftLeader() {
				// If in raft cluster, send command to delete key from cluster
				if err := server.raftApplyDeleteKey(ctx, key); err != nil {
					return fmt.Errorf("adjustMemoryUsage -> LFU cache eviction: %+v", err)
				}
			}

			// Run garbage collection
			runtime.GC()
			// Return if we're below max memory
			runtime.ReadMemStats(&memStats)
			if memStats.HeapInuse < server.config.MaxMemory {
				return nil
			}
		}
	case slices.Contains([]string{constants.AllKeysLRU, constants.VolatileLRU}, strings.ToLower(server.config.EvictionPolicy)):
		// Remove keys from th LRU cache until we're below the max memory limit or
		// until the LRU cache is empty.
		server.lruCache.mutex.Lock()
		defer server.lruCache.mutex.Unlock()
		for {
			// Return if cache is empty
			if server.lruCache.cache.Len() == 0 {
				return fmt.Errorf("adjsutMemoryUsage -> LRU cache empty")
			}

			key := server.lruCache.cache.Pop().(string)
			if !server.isInCluster() {
				// If in standalone mode, directly delete the key.
				if err := server.DeleteKey(ctx, key); err != nil {
					return fmt.Errorf("adjustMemoryUsage -> LRU cache eviction: %+v", err)
				}
			} else if server.isInCluster() && server.raft.IsRaftLeader() {
				// If in cluster mode and the node is a cluster leader,
				// send command to delete the key from the cluster.
				if err := server.raftApplyDeleteKey(ctx, key); err != nil {
					return fmt.Errorf("adjustMemoryUsage -> LRU cache eviction: %+v", err)
				}
			}

			// Run garbage collection
			runtime.GC()
			// Return if we're below max memory
			runtime.ReadMemStats(&memStats)
			if memStats.HeapInuse < server.config.MaxMemory {
				return nil
			}
		}
	case slices.Contains([]string{constants.AllKeysRandom}, strings.ToLower(server.config.EvictionPolicy)):
		// Remove random keys until we're below the max memory limit
		// or there are no more keys remaining.
		for {
			// If there are no keys, return error
			if len(server.keyLocks) == 0 {
				err := errors.New("no keys to evict")
				return fmt.Errorf("adjustMemoryUsage -> all keys random: %+v", err)
			}
			// Get random key
			idx := rand.Intn(len(server.keyLocks))
			for key, _ := range server.keyLocks {
				if idx == 0 {
					if !server.isInCluster() {
						// If in standalone mode, directly delete the key
						if err := server.DeleteKey(ctx, key); err != nil {
							return fmt.Errorf("adjustMemoryUsage -> all keys random: %+v", err)
						}
					} else if server.isInCluster() && server.raft.IsRaftLeader() {
						if err := server.raftApplyDeleteKey(ctx, key); err != nil {
							return fmt.Errorf("adjustMemoryUsage -> all keys random: %+v", err)
						}
					}
					// Run garbage collection
					runtime.GC()
					// Return if we're below max memory
					runtime.ReadMemStats(&memStats)
					if memStats.HeapInuse < server.config.MaxMemory {
						return nil
					}
				}
				idx--
			}
		}
	case slices.Contains([]string{constants.VolatileRandom}, strings.ToLower(server.config.EvictionPolicy)):
		// Remove random keys with an associated expiry time until we're below the max memory limit
		// or there are no more keys with expiry time.
		for {
			// Get random volatile key
			server.keysWithExpiry.rwMutex.RLock()
			idx := rand.Intn(len(server.keysWithExpiry.keys))
			key := server.keysWithExpiry.keys[idx]
			server.keysWithExpiry.rwMutex.RUnlock()

			if !server.isInCluster() {
				// If in standalone mode, directly delete the key
				if err := server.DeleteKey(ctx, key); err != nil {
					return fmt.Errorf("adjustMemoryUsage -> volatile keys random: %+v", err)
				}
			} else if server.isInCluster() && server.raft.IsRaftLeader() {
				if err := server.raftApplyDeleteKey(ctx, key); err != nil {
					return fmt.Errorf("adjustMemoryUsage -> volatile keys randome: %+v", err)
				}
			}

			// Run garbage collection
			runtime.GC()
			// Return if we're below max memory
			runtime.ReadMemStats(&memStats)
			if memStats.HeapInuse < server.config.MaxMemory {
				return nil
			}
		}
	default:
		return nil
	}
}

// evictKeysWithExpiredTTL is a function that samples keys with an associated TTL
// and evicts keys that are currently expired.
// This function will sample 20 keys from the list of keys with an associated TTL,
// if the key is expired, it will be evicted.
// This function is only executed in standalone mode or by the raft cluster leader.
func (server *EchoVault) evictKeysWithExpiredTTL(ctx context.Context) error {
	// Only execute this if we're in standalone mode, or raft cluster leader.
	if server.isInCluster() && !server.raft.IsRaftLeader() {
		return nil
	}

	server.keysWithExpiry.rwMutex.RLock()

	// Sample size should be the configured sample size, or the size of the keys with expiry,
	// whichever one is smaller.
	sampleSize := int(server.config.EvictionSample)
	if len(server.keysWithExpiry.keys) < sampleSize {
		sampleSize = len(server.keysWithExpiry.keys)
	}
	keys := make([]string, sampleSize)

	deletedCount := 0
	thresholdPercentage := 20

	var idx int
	var key string
	for i := 0; i < len(keys); i++ {
		for {
			// Retry retrieval of a random key until we find a key that is not already in the list of sampled keys.
			idx = rand.Intn(len(server.keysWithExpiry.keys))
			key = server.keysWithExpiry.keys[idx]
			if !slices.Contains(keys, key) {
				keys[i] = key
				break
			}
		}
	}
	server.keysWithExpiry.rwMutex.RUnlock()

	// Loop through the keys and delete them if they're expired
	for _, k := range keys {
		if _, err := server.KeyRLock(ctx, k); err != nil {
			continue
		}

		// If the current key is not expired, skip to the next key
		if server.store[k].ExpireAt.After(server.clock.Now()) {
			server.KeyRUnlock(ctx, k)
			continue
		}

		// Delete the expired key
		deletedCount += 1
		server.KeyRUnlock(ctx, k)
		if !server.isInCluster() {
			if err := server.DeleteKey(ctx, k); err != nil {
				return fmt.Errorf("evictKeysWithExpiredTTL -> standalone delete: %+v", err)
			}
		} else if server.isInCluster() && server.raft.IsRaftLeader() {
			if err := server.raftApplyDeleteKey(ctx, k); err != nil {
				return fmt.Errorf("evictKeysWithExpiredTTL -> cluster delete: %+v", err)
			}
		}
	}

	// If sampleSize is 0, there's no need to calculate deleted percentage.
	if sampleSize == 0 {
		log.Println("no keys to sample, skipping eviction")
		return nil
	}

	log.Printf("%d keys sampled, %d keys deleted\n", sampleSize, deletedCount)

	// If the deleted percentage is over 20% of the sample size, execute the function again immediately.
	if (deletedCount/sampleSize)*100 >= thresholdPercentage {
		log.Printf("deletion ratio (%d percent) reached threshold (%d percent), sampling again\n",
			(deletedCount/sampleSize)*100, thresholdPercentage)
		return server.evictKeysWithExpiredTTL(ctx)
	}

	return nil
}

func presetValue(server *EchoVault, key string, value interface{}) {
	_, _ = server.CreateKeyAndLock(server.context, key)
	_ = server.SetValue(server.context, key, value)
	server.KeyUnlock(server.context, key)
}

func presetKeyData(server *EchoVault, key string, data internal.KeyData) {
	_, _ = server.CreateKeyAndLock(server.context, key)
	defer server.KeyUnlock(server.context, key)
	_ = server.SetValue(server.context, key, data.Value)
	server.SetExpiry(server.context, key, data.ExpireAt, false)
}
