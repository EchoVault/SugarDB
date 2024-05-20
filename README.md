[![Go](https://github.com/EchoVault/EchoVault/workflows/Go/badge.svg)]() 
[![Go Report Card](https://goreportcard.com/badge/github.com/echovault/echovault)](https://goreportcard.com/report/github.com/echovault/echovault)
[![codecov](https://codecov.io/gh/EchoVault/EchoVault/graph/badge.svg?token=CHWTW0IUNV)](https://codecov.io/gh/EchoVault/EchoVault)
<br/>
[![GitHub Release](https://img.shields.io/github/v/release/EchoVault/EchoVault)]()
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
<br/>
[![Go Reference](https://pkg.go.dev/badge/github.com/echovault/echovault.svg)](https://pkg.go.dev/github.com/echovault/echovault)
<br/>
<hr/>

<img alt="echovault_logo" src="./images/EchoVault GitHub Cover.png" width="5062" />

# What is EchoVault?

EchoVault is a highly configurable, distributed, in-memory data store and cache implemented in Go.
It can be imported as a Go library or run as an independent service.

EchoVault aims to provide a rich set of data structures and functions for
manipulating data in memory. These data structures include, but are not limited to:
Lists, Sets, Sorted Sets, Hashes, and much more to come soon.

EchoVault provides a persistence layer for increased reliability. Both Append-Only files 
and snapshots can be used to persist data in the disk for recovery in case of unexpected shutdowns.

Replication is a core feature of EchoVault and is implemented using the RAFT algorithm, 
allowing you to create a fault-tolerant cluster of EchoVault nodes to improve reliability.
If you do not need a replication cluster, you can always run EchoVault
in standalone mode and have a fully capable single node.

EchoVault aims to not only be a server but to be importable to existing 
projects to enhance them with EchoVault features, this 
capability is always being worked on and improved.

# Features

Features offered by EchoVault include:

1) TLS and mTLS support for multiple server and client RootCAs.
2) Replication cluster support using the RAFT algorithm.
3) ACL Layer for user Authentication and Authorization.
4) Distributed Pub/Sub functionality with consumer groups.
5) Sets, Sorted Sets, Hashes, Lists and more.
6) Persistence layer with Snapshots and Append-Only files.
7) Key Eviction Policies.
8) Command extension via shared object files.
9) Command extension via embedded API.

We are working hard to add more features to EchoVault to make it
much more powerful. Features in the roadmap include:

1) Sharding
2) Streams
3) Transactions
4) Bitmap
5) HyperLogLog
6) Lua Modules
7) JSON
8) Improved Observability
   

# Usage (Embedded)

Install EchoVault with: `go get github.com/echoVault/echoVault`.
Run `go mod tidy` to pull all of EchoVault's dependencies.

Here's an example of using EchoVault as an embedded library.
You can access all of EchoVault's commands using an ergonomic API.

```go
func main() {
	server, err := echovault.NewEchoVault()

	if err != nil {
		log.Fatal(err)
	}

	_, _ = server.Set("key", "Hello, world!", echovault.SETOptions{})
	
	v, _ := server.Get("key")
	fmt.Println(v) // Hello, world!

	wg := sync.WaitGroup{}

	// Subscribe to multiple EchoVault channels.
	readMessage := server.Subscribe("subscriber1", "channel_1", "channel_2", "channel_3")
	wg.Add(1)
	go func() {
		wg.Done()
		for {
			message := readMessage()
			fmt.Printf("EVENT: %s, CHANNEL: %s, MESSAGE: %s\n", message[0], message[1], message[2])
		}
	}()
	wg.Wait()

	wg.Add(1)
	go func() {
		for i := 1; i <= 3; i++ {
			// Simulating delay.
			<-time.After(1 * time.Second)
			// Publish message to each EchoVault channel.
			_, _ = server.Publish(fmt.Sprintf("channel_%d", i), "Hello!")
		}
		wg.Done()
	}()
	wg.Wait()

	// (Optional): Listen for TCP connections on this EchoVault instance.
	server.Start()
}
```

An embedded EchoVault instance can still be part of a cluster, and the changes triggered 
from the API will be consistent across the cluster.

# Usage (Client-Server) 

### Homebrew

To install via homebrew, run:
1) `brew tap echovault/echovault`
2) `brew install echovault/echovault/echovault`

Once installed, you can run the server with the following command:
`echovault --bind-addr=localhost --data-dir="path/to/persistence/directory"`

### Binaries

You can download the binaries by clicking on a release tag and downloading
the binary for your system.

# Clients

EchoVault uses RESP, which makes it compatible with existing 
Redis clients.

<hr />

<a href="https://echovault.io/docs/intro" target="_blank">Documentation</a>

<hr />

# Supported Commands

## ACL
* [AUTH](https://echovault.io/docs/commands/acl/auth)
* [ACL CAT](https://echovault.io/docs/commands/acl/acl_cat)
* [ACL DELUSER](https://echovault.io/docs/commands/acl/acl_deluser)
* [ACL GETUSER](https://echovault.io/docs/commands/acl/acl_getuser)
* [ACL LIST](https://echovault.io/docs/commands/acl/acl_list)
* [ACL LOAD](https://echovault.io/docs/commands/acl/acl_load)
* [ACL SAVE](https://echovault.io/docs/commands/acl/acl_save)
* [ACL SETUSER](https://echovault.io/docs/commands/acl/acl_setuser)
* [ACL USERS](https://echovault.io/docs/commands/acl/acl_users)
* [ACL WHOAMI](https://echovault.io/docs/commands/acl/acl_whoami)

## ADMIN
* [COMMAND COUNT](https://echovault.io/docs/commands/admin/command_count)
* [COMMAND LIST](https://echovault.io/docs/commands/admin/command_list)
* [COMMANDS](https://echovault.io/docs/commands/admin/commands)
* [LASTSAVE](https://echovault.io/docs/commands/admin/lastsave)
* [MODULE LIST](https://echovault.io/docs/commands/admin/module_list)
* [MODULE LOAD](https://echovault.io/docs/commands/admin/module_load)
* [MODULE UNLOAD](https://echovault.io/docs/commands/admin/module_unload)
* [REWRITEAOF](https://echovault.io/docs/commands/admin/rewriteaof)
* [SAVE](https://echovault.io/docs/commands/admin/save)

## CONNECTION
* [PING](https://echovault.io/docs/commands/connection/ping)

## GENERIC
* [DEL](https://echovault.io/docs/commands/generic/del)
* [EXPIRE](https://echovault.io/docs/commands/generic/expire)
* [EXPIRETIME](https://echovault.io/docs/commands/generic/expiretime)
* [GET](https://echovault.io/docs/commands/generic/get)
* [MGET](https://echovault.io/docs/commands/generic/mget)
* [MSET](https://echovault.io/docs/commands/generic/mset)
* [PERSIST](https://echovault.io/docs/commands/generic/persist)
* [PEXPIRE](https://echovault.io/docs/commands/generic/pexpire)
* [PEXPIRETIME](https://echovault.io/docs/commands/generic/pexpiretime)
* [PTTL](https://echovault.io/docs/commands/generic/pttl)
* [SET](https://echovault.io/docs/commands/generic/set)
* [TTL](https://echovault.io/docs/commands/generic/ttl)

## HASH
* [HDEL](https://echovault.io/docs/commands/hash/hdel)
* [HEXISTS](https://echovault.io/docs/commands/hash/hexists)
* [HGET](https://echovault.io/docs/commands/hash/hget)
* [HGETALL](https://echovault.io/docs/commands/hash/hgetall)
* [HINCRBY](https://echovault.io/docs/commands/hash/hincrby)
* [HINCRBYFLOAT](https://echovault.io/docs/commands/hash/hincrbyfloat)
* [HKEYS](https://echovault.io/docs/commands/hash/hkeys)
* [HLEN](https://echovault.io/docs/commands/hash/hlen)
* [HRANDFIELD](https://echovault.io/docs/commands/hash/hrandfield)
* [HSET](https://echovault.io/docs/commands/hash/hset)
* [HSETNX](https://echovault.io/docs/commands/hash/hsetnx)
* [HSTRLEN](https://echovault.io/docs/commands/hash/hstrlen)
* [HVALS](https://echovault.io/docs/commands/hash/hvals)

## LIST
* [LINDEX](https://echovault.io/docs/commands/list/lindex)
* [LLEN](https://echovault.io/docs/commands/list/llen)
* [LMOVE](https://echovault.io/docs/commands/list/lmove)
* [LPOP](https://echovault.io/docs/commands/list/lpop)
* [LPUSH](https://echovault.io/docs/commands/list/lpush)
* [LPUSHX](https://echovault.io/docs/commands/list/lpushx)
* [LRANGE](https://echovault.io/docs/commands/list/lrange)
* [LREM](https://echovault.io/docs/commands/list/lrem)
* [LSET](https://echovault.io/docs/commands/list/lset)
* [LTRIM](https://echovault.io/docs/commands/list/ltrim)
* [RPOP](https://echovault.io/docs/commands/list/rpop)
* [RPUSH](https://echovault.io/docs/commands/list/rpush)
* [RPUSHX](https://echovault.io/docs/commands/list/rpushx)

## PUBSUB
* [PSUBSCRIBE](https://echovault.io/docs/commands/pubsub/psubscribe)
* [PUBLISH](https://echovault.io/docs/commands/pubsub/publish)
* [PUBSUB CHANNELS](https://echovault.io/docs/commands/pubsub/pubsub_channels)
* [PUBSUB NUMPAT](https://echovault.io/docs/commands/pubsub/pubsub_numpat)
* [PUBSUB NUMSUB](https://echovault.io/docs/commands/pubsub/pubsub_numsub)
* [PUNSUBSCRIBE](https://echovault.io/docs/commands/pubsub/punsubscribe)
* [SUBSCRIBE](https://echovault.io/docs/commands/pubsub/subscribe)
* [UNSUBSCRIBE](https://echovault.io/docs/commands/pubsub/unsubscribe)

## SET
* [SADD](https://echovault.io/docs/commands/set/sadd)
* [SCARD](https://echovault.io/docs/commands/set/scard)
* [SDIFF](https://echovault.io/docs/commands/set/sdiff)
* [SDIFFSTORE](https://echovault.io/docs/commands/set/sdiffstore)
* [SINTER](https://echovault.io/docs/commands/set/sinter)
* [SINTERCARD](https://echovault.io/docs/commands/set/sintercard)
* [SINTERSTORE](https://echovault.io/docs/commands/set/sinterstore)
* [SISMEMBER](https://echovault.io/docs/commands/set/sismember)
* [SMEMBERS](https://echovault.io/docs/commands/set/smembers)
* [SMISMEMBER](https://echovault.io/docs/commands/set/smismember)
* [SMOVE](https://echovault.io/docs/commands/set/smove)
* [SPOP](https://echovault.io/docs/commands/set/spop)
* [SRANDMEMBER](https://echovault.io/docs/commands/set/srandmember)
* [SREM](https://echovault.io/docs/commands/set/srem)
* [SUNION](https://echovault.io/docs/commands/set/sunion)
* [SUNIONSTORE](https://echovault.io/docs/commands/set/sunionstore)

## SORTED SET
* [ZADD](https://echovault.io/docs/commands/sorted_set/zadd)
* [ZCARD](https://echovault.io/docs/commands/sorted_set/zcard)
* [ZCOUNT](https://echovault.io/docs/commands/sorted_set/zcount)
* [ZDIFF](https://echovault.io/docs/commands/sorted_set/zdiff)
* [ZDIFFSTORE](https://echovault.io/docs/commands/sorted_set/zdiffstore)
* [ZINCRBY](https://echovault.io/docs/commands/sorted_set/zincrby)
* [ZINTER](https://echovault.io/docs/commands/sorted_set/zinter)
* [ZINTERSTORE](https://echovault.io/docs/commands/sorted_set/zinterstore)
* [ZLEXCOUNT](https://echovault.io/docs/commands/sorted_set/zlexcount)
* [ZMPOP](https://echovault.io/docs/commands/sorted_set/zmpop)
* [ZMSCORE](https://echovault.io/docs/commands/sorted_set/zmscore)
* [ZPOPMAX](https://echovault.io/docs/commands/sorted_set/zpopmax)
* [ZPOPMIN](https://echovault.io/docs/commands/sorted_set/zpopmin)
* [ZRANDMEMBER](https://echovault.io/docs/commands/sorted_set/zrandmember)
* [ZRANGE](https://echovault.io/docs/commands/sorted_set/zrange)
* [ZRANGESTORE](https://echovault.io/docs/commands/sorted_set/zrangestore)
* [ZRANK](https://echovault.io/docs/commands/sorted_set/zrank)
* [ZREM](https://echovault.io/docs/commands/sorted_set/zrem)
* [ZREMRANGEBYLEX](https://echovault.io/docs/commands/sorted_set/zremrangebylex)
* [ZREMRANGEBYRANK](https://echovault.io/docs/commands/sorted_set/zremrangebyrank)
* [ZREMRANGEBYSCORE](https://echovault.io/docs/commands/sorted_set/zremrangebyscore)
* [ZREVRANK](https://echovault.io/docs/commands/sorted_set/zrevrank)
* [ZSCORE](https://echovault.io/docs/commands/sorted_set/zscore)
* [ZUNION](https://echovault.io/docs/commands/sorted_set/zunion)
* [ZUNIONSTORE](https://echovault.io/docs/commands/sorted_set/zunionstore)

## STRING

* [GETRANGE](https://echovault.io/docs/commands/string/getrange)
* [SETRANGE](https://echovault.io/docs/commands/string/setrange)
* [STRLEN](https://echovault.io/docs/commands/string/strlen)
* [SUBSTR](https://echovault.io/docs/commands/string/substr)




