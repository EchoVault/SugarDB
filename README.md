[![Go](https://github.com/EchoVault/EchoVault/workflows/Go/badge.svg)]() 
[![Go Report Card](https://goreportcard.com/badge/github.com/echovault/echovault)](https://goreportcard.com/report/github.com/echovault/echovault)
[![codecov](https://codecov.io/gh/EchoVault/EchoVault/graph/badge.svg?token=CHWTW0IUNV)](https://codecov.io/gh/EchoVault/EchoVault)
<br/>
[![Go Reference](https://pkg.go.dev/badge/github.com/echovault/echovault.svg)](https://pkg.go.dev/github.com/echovault/echovault)
[![GitHub Release](https://img.shields.io/github/v/release/EchoVault/EchoVault)]()
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
<br/>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go) 
[![Discord](https://img.shields.io/discord/1211815152291414037?label=Discord&labelColor=%237289da)](https://discord.com/invite/JrG4kPrF8v)
<br/>

<hr/>

# Table of Contents
1. [What is EchoVault](#what-is-echovault)
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

<a name="what-is-echovault"></a>
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

<a name="features"></a>
# Features

Features offered by EchoVault include:

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
   

<a name="usage-embedded"></a>
# Usage (Embedded)

Install EchoVault with: `go get github.com/echovault/echovault`.

Here's an example of using EchoVault as an embedded library.
You can access all of EchoVault's commands using an ergonomic API.

```go
func main() {
  server, err := echovault.NewEchoVault()

  if err != nil {
    log.Fatal(err)
  }

  _, _, _ = server.Set("key", "Hello, world!", echovault.SETOptions{})

  v, _ := server.Get("key")
  fmt.Println(v) // Hello, world!

  // (Optional): Listen for TCP connections on this EchoVault instance.
  server.Start()
}
```

An embedded EchoVault instance can still be part of a cluster, and the changes triggered 
from the API will be consistent across the cluster.

<a name="usage-client-server"></a>
# Usage (Client-Server) 

<a name="usage-homebrew"></a>
### Homebrew

To install via homebrew, run:
1) `brew tap echovault/echovault`
2) `brew install echovault/echovault/echovault`

Once installed, you can run the server with the following command:
`echovault --bind-addr=localhost --data-dir="path/to/persistence/directory"`

<a name="usage-docker"></a>
### Docker

`docker pull echovault/echovault`

The full list of tags can be found [here](https://hub.docker.com/r/echovault/echovault/tags).

<a name="usage-container-registry"></a>
### Container Registry

`docker pull ghcr.io/echovault/echovault`

The full list of tags can be found [here](https://github.com/EchoVault/EchoVault/pkgs/container/echovault).

<a name="usage-binaries"></a>
### Binaries

You can download the binaries by clicking on a release tag and downloading
the binary for your system.

<a name="clients"></a>
# Clients

EchoVault uses RESP, which makes it compatible with existing 
Redis clients.

<a name="benchmarks"></a>
# Benchmarks
The following benchmark only applies to the TCP client-server mode.

Hardware: MacBook Pro 14in, M1 chip, 16GB RAM, 8 Cores <br/>
Command: `redis-benchmark -h localhost -p 7480 -q -t ping,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,zpopmin,lrange,mset` <br/>
Result: 
```
PING_INLINE: 89285.71 requests per second, p50=0.247 msec                   
PING_MBULK: 85543.20 requests per second, p50=0.239 msec                   
SET: 65573.77 requests per second, p50=0.455 msec                   
GET: 79176.56 requests per second, p50=0.295 msec                   
INCR: 68870.52 requests per second, p50=0.439 msec                   
LPUSH: 27601.44 requests per second, p50=1.567 msec                   
RPUSH: 61842.92 requests per second, p50=0.519 msec                   
LPOP: 58548.01 requests per second, p50=0.567 msec                   
RPOP: 68681.32 requests per second, p50=0.439 msec                   
SADD: 67613.25 requests per second, p50=0.479 msec                   
HSET: 56561.09 requests per second, p50=0.599 msec                   
ZPOPMIN: 70972.32 requests per second, p50=0.359 msec                   
LPUSH (needed to benchmark LRANGE): 26434.05 requests per second, p50=1.623 msec                   
LRANGE_100 (first 100 elements): 26939.66 requests per second, p50=1.263 msec                   
LRANGE_300 (first 300 elements): 5081.82 requests per second, p50=9.095 msec                    
LRANGE_500 (first 500 elements): 2554.87 requests per second, p50=18.191 msec                   
LRANGE_600 (first 600 elements): 1903.96 requests per second, p50=24.607 msec                   
MSET (10 keys): 56022.41 requests per second, p50=0.463 msec 
```

<a name="commands"></a>
# Supported Commands

<a name="commands-acl"></a>
## ACL
* [ACL CAT](https://echovault.io/docs/commands/acl/acl_cat)
* [ACL DELUSER](https://echovault.io/docs/commands/acl/acl_deluser)
* [ACL GETUSER](https://echovault.io/docs/commands/acl/acl_getuser)
* [ACL LIST](https://echovault.io/docs/commands/acl/acl_list)
* [ACL LOAD](https://echovault.io/docs/commands/acl/acl_load)
* [ACL SAVE](https://echovault.io/docs/commands/acl/acl_save)
* [ACL SETUSER](https://echovault.io/docs/commands/acl/acl_setuser)
* [ACL USERS](https://echovault.io/docs/commands/acl/acl_users)
* [ACL WHOAMI](https://echovault.io/docs/commands/acl/acl_whoami)

<a name="commands-admin"></a>
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

<a name="commands-connection"></a>
## CONNECTION
* [AUTH](https://echovault.io/docs/commands/connection/auth)
* [HELLO](https://echovault.io/docs/commands/connection/hello)
* [PING](https://echovault.io/docs/commands/connection/ping)
* [SELECT](https://echovault.io/docs/commands/connection/select)
* [SWAPDB](https://echovault.io/docs/commands/connection/swapdb)

<a name="commands-generic"></a>
## GENERIC
* [DECR](https://echovault.io/docs/commands/generic/decr)
* [DECRBY](https://echovault.io/docs/commands/generic/decrby)
* [DEL](https://echovault.io/docs/commands/generic/del)
* [EXPIRE](https://echovault.io/docs/commands/generic/expire)
* [EXPIRETIME](https://echovault.io/docs/commands/generic/expiretime)
* [FLUSHALL](https://echovault.io/docs/commands/generic/flushall)
* [FLUSHDB](https://echovault.io/docs/commands/generic/flushdb)
* [GET](https://echovault.io/docs/commands/generic/get)
* [INCR](https://echovault.io/docs/commands/generic/incr)
* [INCRBY](https://echovault.io/docs/commands/generic/incrby)
* [MGET](https://echovault.io/docs/commands/generic/mget)
* [MSET](https://echovault.io/docs/commands/generic/mset)
* [PERSIST](https://echovault.io/docs/commands/generic/persist)
* [PEXPIRE](https://echovault.io/docs/commands/generic/pexpire)
* [PEXPIRETIME](https://echovault.io/docs/commands/generic/pexpiretime)
* [PTTL](https://echovault.io/docs/commands/generic/pttl)
* [RENAME](https://echovault.io/docs/commands/generic/rename)
* [SET](https://echovault.io/docs/commands/generic/set)
* [TTL](https://echovault.io/docs/commands/generic/ttl)

<a name="commands-hash"></a>
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

<a name="commands-list"></a>
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

<a name="commands-pubsub"></a>
## PUBSUB
* [PSUBSCRIBE](https://echovault.io/docs/commands/pubsub/psubscribe)
* [PUBLISH](https://echovault.io/docs/commands/pubsub/publish)
* [PUBSUB CHANNELS](https://echovault.io/docs/commands/pubsub/pubsub_channels)
* [PUBSUB NUMPAT](https://echovault.io/docs/commands/pubsub/pubsub_numpat)
* [PUBSUB NUMSUB](https://echovault.io/docs/commands/pubsub/pubsub_numsub)
* [PUNSUBSCRIBE](https://echovault.io/docs/commands/pubsub/punsubscribe)
* [SUBSCRIBE](https://echovault.io/docs/commands/pubsub/subscribe)
* [UNSUBSCRIBE](https://echovault.io/docs/commands/pubsub/unsubscribe)

<a name="commands-set"></a>
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

<a name="commands-sortedset"></a>
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

<a name="commands-string"></a>
## STRING
* [GETRANGE](https://echovault.io/docs/commands/string/getrange)
* [SETRANGE](https://echovault.io/docs/commands/string/setrange)
* [STRLEN](https://echovault.io/docs/commands/string/strlen)
* [SUBSTR](https://echovault.io/docs/commands/string/substr)




