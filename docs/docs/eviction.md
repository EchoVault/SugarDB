---
sidebar_position: 3
---

# Eviction

### Memory Limit

The memory limit can be set using the `--max-memory` config flag. This flag accepts a parsable memory value (e.g 100mb, 16gb). If the limit set is 0, then no memory limit is imposed. The default value is 0.

### Passive eviction

In passive eviction, the expired key is not deleted immediately after the expiry time. The key will remain in the store until the next time it is accessed. When attempting to access an expired key, that is when the key is deleted.

### Active eviction

Echovault will run a background goroutine that samples a set of volatile keys at a given interval. Any keys that are found to be expired will be deleted. If 20% or more of the sampled keys are deleted, then the process will immediately begin again. Otherwise, wait for the given interval until the next round of sampling/eviction. The default number of keys sampled is 20, and the default interval for sampling is 100 milliseconds. These can be configured using the `--eviction-sample` and `--eviction-interval` flags.

### Eviction Policies

Eviction policy can be set using the --eviction-policy flag. The following options are available.

<b>noeviction:</b><br/>
This policy does not evict any keys. When max memory is reached, all new write commands will be rejected until keys are manually deleted by the user.

<b>allkeys-lfu:</b><br/>
With this policy, all keys are considered for eviction when the max memory is reached. When max memory is reached, the least frequently accessed keys will be evicted until the memory usage is under the memory limit.

<b>allkeys-lru:</b><br/>
This policy will consider all keys for eviction when max memory is reached. The least recently accessed keys will be deleted one by one until we are below the memory limit.

<b>allkeys-random:</b><br/>
Evict random keys until we're below the max memory limit.

<b>volatile-lfu:</b><br/>
With this policy, only keys with an associated expiry time will be evicted to adhere to the memory limit. When the memory limit is exceeded, volatile keys will be evicted starting from the least frequently used until we are below the memory limit or are out of volatile keys to evict.

<b>volatile-lru:</b><br/>
With this policy, only keys with an associated expiry time will be evicted to adhere to the memory limit. When the memory limit is exceeded, volatile keys will be evicted starting from the list recently used until we are below the memory limit or are out of volatile keys to evict.

<b>volatile-random:</b><br/>
Evict random volatile keys until we're below the memory limit, or we're out of volatile keys to evict.
