[![Go](https://github.com/EchoVault/SugarDB/workflows/Go/badge.svg)]() 
[![Go Report Card](https://goreportcard.com/badge/github.com/echovault/echovault)](https://goreportcard.com/report/github.com/echovault/echovault)
[![codecov](https://codecov.io/gh/EchoVault/SugarDB/graph/badge.svg?token=CHWTW0IUNV)](https://codecov.io/gh/EchoVault/SugarDB)
<br/>
[![Go Reference](https://pkg.go.dev/badge/github.com/echovault/echovault.svg)](https://pkg.go.dev/github.com/echovault/sugardb)
[![GitHub Release](https://img.shields.io/github/v/release/EchoVault/SugarDB)]()
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
<br/>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go) 
[![Discord](https://img.shields.io/discord/1211815152291414037?label=Discord&labelColor=%237289da)](https://discord.com/invite/JrG4kPrF8v)
<br/>

<hr/>

# Table of Contents
1. [What is SugarDB](#what-is-sugardb)
2. [Features](#features)
3. [Usage (Embedded)](#usage-embedded)
4. [Usage (Client-Server)](#usage-client-server)
   1. [Homebrew](#usage-homebrew)
   2. [Docker](#usage-docker)
   3. [GitHub Container Registry](#usage-container-registry)
   4. [Binaries](#usage-binaries)
5. [Clients](#clients)
6. [Benchmarks](#benchmarks)
7. [Commands](#commands)
   1. [ACL](#commands-acl)
   2. [ADMIN](#commands-admin)
   3. [CONNECTION](#commands-connection)
   4. [GENERIC](#commands-generic)
   5. [HASH](#commands-hash)
   6. [LIST](#commands-list)
   7. [PUBSUB](#commands-pubsub)
   8. [SET](#commands-set)
   9. [SORTED SET](#commands-sortedset)
   10. [STRING](#commands-string)

<a name="what-is-sugardb"></a>
# What is SugarDB?

SugarDB is a highly configurable, distributed, in-memory data store and cache implemented in Go.
It can be imported as a Go library or run as an independent service.

SugarDB aims to provide a rich set of data structures and functions for
manipulating data in memory. These data structures include, but are not limited to:
Lists, Sets, Sorted Sets, Hashes, and much more to come soon.

SugarDB provides a persistence layer for increased reliability. Both Append-Only files 
and snapshots can be used to persist data in the disk for recovery in case of unexpected shutdowns.

Replication is a core feature of SugarDB and is implemented using the RAFT algorithm, 
allowing you to create a fault-tolerant cluster of SugarDB nodes to improve reliability.
If you do not need a replication cluster, you can always run SugarDB
in standalone mode and have a fully capable single node.

SugarDB aims to not only be a server but to be importable to existing 
projects to enhance them with SugarDB features, this 
capability is always being worked on and improved.

<a name="features"></a>
# Features

Features offered by SugarDB include:

1) TLS and mTLS support for multiple server and client RootCAs.
2) Replication cluster support using the RAFT algorithm.
3) ACL Layer for user Authentication and Authorization.
4) Distributed Pub/Sub functionality.
5) Sets, Sorted Sets, Hashes, Lists and more.
6) Persistence layer with Snapshots and Append-Only files.
7) Key Eviction Policies.
8) Command extension via shared object files.
9) Command extension via embedded API.
10) Multi-database support for key namespacing.

We are working hard to add more features to SugarDB to make it
much more powerful. Features in the roadmap include:

1) Sharding
2) Streams
3) Transactions
4) Bitmap
5) HyperLogLog
6) Lua Modules
7) JSON
8) Improved Observability
   

<a name="usage-embedded"></a>
# Usage (Embedded)

Install SugarDB with: `go get github.com/echovault/sugardb`.

Here's an example of using SugarDB as an embedded library.
You can access all of SugarDB's commands using an ergonomic API.

```go
func main() {
  server, err := sugardb.NewSugarDB()
  if err != nil {
    log.Fatal(err)
  }

  _, _, _ = server.Set("key", "Hello, SugarDB!", sugardb.SETOptions{})

  v, _ := server.Get("key")
  fmt.Println(v) // Hello, SugarDB!

  // (Optional): Listen for TCP connections on this SugarDB instance.
  server.Start()
}
```

An embedded SugarDB instance can still be part of a cluster, and the changes triggered 
from the API will be consistent across the cluster.

<a name="usage-client-server"></a>
# Usage (Client-Server) 

<a name="usage-homebrew"></a>
### Homebrew

To install via homebrew, run:
1) `brew tap echovault/sugardb`
2) `brew install echovault/echovault/sugardb`

Once installed, you can run the server with the following command:
`sugardb --bind-addr=localhost --data-dir="path/to/persistence/directory"`

<a name="usage-docker"></a>
### Docker

`docker pull echovault/sugardb`

The full list of tags can be found [here](https://hub.docker.com/r/echovault/sugardb/tags).

<a name="usage-container-registry"></a>
### Container Registry

`docker pull ghcr.io/echovault/sugardb`

The full list of tags can be found [here](https://github.com/EchoVault/SugarDB/pkgs/container/sugardb).

<a name="usage-binaries"></a>
### Binaries

You can download the binaries by clicking on a release tag and downloading
the binary for your system.

<a name="clients"></a>
# Clients

SugarDB uses RESP, which makes it compatible with existing 
Redis clients.

<a name="benchmarks"></a>
# Benchmarks
To compare command performance with Redis, benchmarks can be run with: 

`make benchmark`

Prerequisites:
- `brew install redis` to run the Redis server and benchmark script
- `brew tap echovault/sugardb` & `brew install echovault/echovault/sugardb` to run the SugarDB Client-Server

Benchmark script options:
- `make benchmark use_local_server=true` runs on your local SugarDB Client-Server
- `make benchmark commands=ping,set,get...` runs the benchmark script on the specified commands

<a name="commands"></a>
# Supported Commands

<a name="commands-acl"></a>
## ACL
* [ACL CAT](https://sugardb.io/docs/commands/acl/acl_cat)
* [ACL DELUSER](https://sugardb.io/docs/commands/acl/acl_deluser)
* [ACL GETUSER](https://sugardb.io/docs/commands/acl/acl_getuser)
* [ACL LIST](https://sugardb.io/docs/commands/acl/acl_list)
* [ACL LOAD](https://sugardb.io/docs/commands/acl/acl_load)
* [ACL SAVE](https://sugardb.io/docs/commands/acl/acl_save)
* [ACL SETUSER](https://sugardb.io/docs/commands/acl/acl_setuser)
* [ACL USERS](https://sugardb.io/docs/commands/acl/acl_users)
* [ACL WHOAMI](https://sugardb.io/docs/commands/acl/acl_whoami)

<a name="commands-admin"></a>
## ADMIN
* [COMMAND COUNT](https://sugardb.io/docs/commands/admin/command_count)
* [COMMAND LIST](https://sugardb.io/docs/commands/admin/command_list)
* [COMMANDS](https://sugardb.io/docs/commands/admin/commands)
* [LASTSAVE](https://sugardb.io/docs/commands/admin/lastsave)
* [MODULE LIST](https://sugardb.io/docs/commands/admin/module_list)
* [MODULE LOAD](https://sugardb.io/docs/commands/admin/module_load)
* [MODULE UNLOAD](https://sugardb.io/docs/commands/admin/module_unload)
* [REWRITEAOF](https://sugardb.io/docs/commands/admin/rewriteaof)
* [SAVE](https://sugardb.io/docs/commands/admin/save)

<a name="commands-connection"></a>
## CONNECTION
* [AUTH](https://sugardb.io/docs/commands/connection/auth)
* [ECHO](https://sugardb.io/docs/commands/connection/echo)
* [HELLO](https://sugardb.io/docs/commands/connection/hello)
* [PING](https://sugardb.io/docs/commands/connection/ping)
* [SELECT](https://sugardb.io/docs/commands/connection/select)
* [SWAPDB](https://sugardb.io/docs/commands/connection/swapdb)

<a name="commands-generic"></a>
## GENERIC
* [COPY](https://sugardb.io/docs/commands/generic/copy)
* [DECR](https://sugardb.io/docs/commands/generic/decr)
* [DECRBY](https://sugardb.io/docs/commands/generic/decrby)
* [DEL](https://sugardb.io/docs/commands/generic/del)
* [EXPIRE](https://sugardb.io/docs/commands/generic/expire)
* [EXPIRETIME](https://sugardb.io/docs/commands/generic/expiretime)
* [FLUSHALL](https://sugardb.io/docs/commands/generic/flushall)
* [FLUSHDB](https://sugardb.io/docs/commands/generic/flushdb)
* [GET](https://sugardb.io/docs/commands/generic/get)
* [GETDEL](https://sugardb.io/docs/commands/generic/getdel)
* [GETEX](https://sugardb.io/docs/commands/generic/get)
* [INCR](https://sugardb.io/docs/commands/generic/incr)
* [INCRBY](https://sugardb.io/docs/commands/generic/incrby)
* [INCRBYFLOAT](https://sugardb.io/docs/commands/generic/incrbyfloat)
* [MGET](https://sugardb.io/docs/commands/generic/mget)
* [MOVE](https://sugardb.io/docs/commands/generic/move)
* [MSET](https://sugardb.io/docs/commands/generic/mset)
* [OBJECTFREQ](https://sugardb.io/docs/commands/generic/objectfreq)
* [OBJECTIDLETIME](https://sugardb.io/docs/commands/generic/objectidletime)
* [PERSIST](https://sugardb.io/docs/commands/generic/persist)
* [PEXPIRE](https://sugardb.io/docs/commands/generic/pexpire)
* [PEXPIREAT](https://sugardb.io/docs/commands/generic/pexpireat)
* [PEXPIRETIME](https://sugardb.io/docs/commands/generic/pexpiretime)
* [PTTL](https://sugardb.io/docs/commands/generic/pttl)
* [RANDOMKEY](https://sugardb.io/docs/commands/generic/randomkey)
* [RENAME](https://sugardb.io/docs/commands/generic/rename)
* [SET](https://sugardb.io/docs/commands/generic/set)
* [TTL](https://sugardb.io/docs/commands/generic/ttl)
* [TYPE](https://sugardb.io/docs/commands/generic/type)


<a name="commands-hash"></a>
## HASH
* [HDEL](https://sugardb.io/docs/commands/hash/hdel)
* [HEXISTS](https://sugardb.io/docs/commands/hash/hexists)
* [HGET](https://sugardb.io/docs/commands/hash/hget)
* [HGETALL](https://sugardb.io/docs/commands/hash/hgetall)
* [HINCRBY](https://sugardb.io/docs/commands/hash/hincrby)
* [HINCRBYFLOAT](https://sugardb.io/docs/commands/hash/hincrbyfloat)
* [HKEYS](https://sugardb.io/docs/commands/hash/hkeys)
* [HLEN](https://sugardb.io/docs/commands/hash/hlen)
* [HMGET](https://sugardb.io/docs/commands/hash/hmget)
* [HRANDFIELD](https://sugardb.io/docs/commands/hash/hrandfield)
* [HSET](https://sugardb.io/docs/commands/hash/hset)
* [HSETNX](https://sugardb.io/docs/commands/hash/hsetnx)
* [HSTRLEN](https://sugardb.io/docs/commands/hash/hstrlen)
* [HVALS](https://sugardb.io/docs/commands/hash/hvals)

<a name="commands-list"></a>
## LIST
* [LINDEX](https://sugardb.io/docs/commands/list/lindex)
* [LLEN](https://sugardb.io/docs/commands/list/llen)
* [LMOVE](https://sugardb.io/docs/commands/list/lmove)
* [LPOP](https://sugardb.io/docs/commands/list/lpop)
* [LPUSH](https://sugardb.io/docs/commands/list/lpush)
* [LPUSHX](https://sugardb.io/docs/commands/list/lpushx)
* [LRANGE](https://sugardb.io/docs/commands/list/lrange)
* [LREM](https://sugardb.io/docs/commands/list/lrem)
* [LSET](https://sugardb.io/docs/commands/list/lset)
* [LTRIM](https://sugardb.io/docs/commands/list/ltrim)
* [RPOP](https://sugardb.io/docs/commands/list/rpop)
* [RPUSH](https://sugardb.io/docs/commands/list/rpush)
* [RPUSHX](https://sugardb.io/docs/commands/list/rpushx)

<a name="commands-pubsub"></a>
## PUBSUB
* [PSUBSCRIBE](https://sugardb.io/docs/commands/pubsub/psubscribe)
* [PUBLISH](https://sugardb.io/docs/commands/pubsub/publish)
* [PUBSUB CHANNELS](https://sugardb.io/docs/commands/pubsub/pubsub_channels)
* [PUBSUB NUMPAT](https://sugardb.io/docs/commands/pubsub/pubsub_numpat)
* [PUBSUB NUMSUB](https://sugardb.io/docs/commands/pubsub/pubsub_numsub)
* [PUNSUBSCRIBE](https://sugardb.io/docs/commands/pubsub/punsubscribe)
* [SUBSCRIBE](https://sugardb.io/docs/commands/pubsub/subscribe)
* [UNSUBSCRIBE](https://sugardb.io/docs/commands/pubsub/unsubscribe)

<a name="commands-set"></a>
## SET
* [SADD](https://sugardb.io/docs/commands/set/sadd)
* [SCARD](https://sugardb.io/docs/commands/set/scard)
* [SDIFF](https://sugardb.io/docs/commands/set/sdiff)
* [SDIFFSTORE](https://sugardb.io/docs/commands/set/sdiffstore)
* [SINTER](https://sugardb.io/docs/commands/set/sinter)
* [SINTERCARD](https://sugardb.io/docs/commands/set/sintercard)
* [SINTERSTORE](https://sugardb.io/docs/commands/set/sinterstore)
* [SISMEMBER](https://sugardb.io/docs/commands/set/sismember)
* [SMEMBERS](https://sugardb.io/docs/commands/set/smembers)
* [SMISMEMBER](https://sugardb.io/docs/commands/set/smismember)
* [SMOVE](https://sugardb.io/docs/commands/set/smove)
* [SPOP](https://sugardb.io/docs/commands/set/spop)
* [SRANDMEMBER](https://sugardb.io/docs/commands/set/srandmember)
* [SREM](https://sugardb.io/docs/commands/set/srem)
* [SUNION](https://sugardb.io/docs/commands/set/sunion)
* [SUNIONSTORE](https://sugardb.io/docs/commands/set/sunionstore)

<a name="commands-sortedset"></a>
## SORTED SET
* [ZADD](https://sugardb.io/docs/commands/sorted_set/zadd)
* [ZCARD](https://sugardb.io/docs/commands/sorted_set/zcard)
* [ZCOUNT](https://sugardb.io/docs/commands/sorted_set/zcount)
* [ZDIFF](https://sugardb.io/docs/commands/sorted_set/zdiff)
* [ZDIFFSTORE](https://sugardb.io/docs/commands/sorted_set/zdiffstore)
* [ZINCRBY](https://sugardb.io/docs/commands/sorted_set/zincrby)
* [ZINTER](https://sugardb.io/docs/commands/sorted_set/zinter)
* [ZINTERSTORE](https://sugardb.io/docs/commands/sorted_set/zinterstore)
* [ZLEXCOUNT](https://sugardb.io/docs/commands/sorted_set/zlexcount)
* [ZMPOP](https://sugardb.io/docs/commands/sorted_set/zmpop)
* [ZMSCORE](https://sugardb.io/docs/commands/sorted_set/zmscore)
* [ZPOPMAX](https://sugardb.io/docs/commands/sorted_set/zpopmax)
* [ZPOPMIN](https://sugardb.io/docs/commands/sorted_set/zpopmin)
* [ZRANDMEMBER](https://sugardb.io/docs/commands/sorted_set/zrandmember)
* [ZRANGE](https://sugardb.io/docs/commands/sorted_set/zrange)
* [ZRANGESTORE](https://sugardb.io/docs/commands/sorted_set/zrangestore)
* [ZRANK](https://sugardb.io/docs/commands/sorted_set/zrank)
* [ZREM](https://sugardb.io/docs/commands/sorted_set/zrem)
* [ZREMRANGEBYLEX](https://sugardb.io/docs/commands/sorted_set/zremrangebylex)
* [ZREMRANGEBYRANK](https://sugardb.io/docs/commands/sorted_set/zremrangebyrank)
* [ZREMRANGEBYSCORE](https://sugardb.io/docs/commands/sorted_set/zremrangebyscore)
* [ZREVRANK](https://sugardb.io/docs/commands/sorted_set/zrevrank)
* [ZSCORE](https://sugardb.io/docs/commands/sorted_set/zscore)
* [ZUNION](https://sugardb.io/docs/commands/sorted_set/zunion)
* [ZUNIONSTORE](https://sugardb.io/docs/commands/sorted_set/zunionstore)

<a name="commands-string"></a>
## STRING
* [APPEND](https://sugardb.io/docs/commands/string/append)
* [GETRANGE](https://sugardb.io/docs/commands/string/getrange)
* [SETRANGE](https://sugardb.io/docs/commands/string/setrange)
* [STRLEN](https://sugardb.io/docs/commands/string/strlen)
* [SUBSTR](https://sugardb.io/docs/commands/string/substr)
