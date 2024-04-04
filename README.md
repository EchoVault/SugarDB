[![Go](https://github.com/EchoVault/EchoVault/workflows/Go/badge.svg)]() 
[![Go Report Card](https://goreportcard.com/badge/github.com/echovault/echovault)](https://goreportcard.com/report/github.com/echovault/echovault)
[![codecov](https://codecov.io/gh/EchoVault/EchoVault/graph/badge.svg?token=CHWTW0IUNV)](https://codecov.io/gh/EchoVault/EchoVault)
<br/>
[![GitHub Release](https://img.shields.io/github/v/release/EchoVault/EchoVault)]()
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
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

Some key features offered by EchoVault include:

1) TLS and mTLS support with support for multiple server and client RootCAs
2) Replication cluster support using RAFT algorithm
3) ACL Layer for user Authentication and Authorization
4) Distributed Pub/Sub functionality with consumer groups
5) Sets, Sorted Sets, Hashes
6) Persistence layer with Snapshots and Append-Only files
7) Key Eviction Policies

We are working hard to add more features to EchoVault to make it
much more powerful. Features in the roadmap include:

1) Streams
2) Transactions
3) Bitmap
4) HyperLogLog
5) Lua Modules
6) JSON
7) Improved Observability

# Usage (Embedded)

Install EchoVault with: `go get github.com/echoVault/echoVault`.
Run `go mod tidy` to pull all of EchoVault's dependencies.

Here's an example of using EchoVault as an embedded library.
You can access all of EchoVault's commands using an ergonomic API.

```go
func main() {
	server, err := echovault.NewEchoVault(
		echovault.WithConfig(config.DefaultConfig()),
		echovault.WithCommands(commands.All()),
	)

	if err != nil {
		log.Fatal(err)
	}

	_, _ = server.SET("key", "Hello, world!", echovault.SETOptions{})

	v, _ := server.GET("key")

	fmt.Println(v) // Hello, world!

	wg := sync.WaitGroup{}

	// Subscribe to multiple EchoVault channels.
	readMessage := server.SUBSCRIBE("subscriber1", "channel_1", "channel_2", "channel_3")
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
			_, _ = server.PUBLISH(fmt.Sprintf("channel_%d", i), "Hello!")
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

Next, [install the client via homebrew](https://github.com/EchoVault/EchoVault-CLI).

### Binaries

You can download the binaries by clicking on a release tag and downloading
the binary for your system.

Checkout the [configuration section](#configuration) for the possible configuration
flags.

# Clients

EchoVault uses RESP, which makes it compatible with existing 
Redis clients.

# Development Setup

Pre-requisites:
1) Go
2) Docker
3) Docker Compose
4) x86_64-linux-musl-gcc cross-compile toolchain as the development image is built for an Alpine container

Steps:
1) Clone the repository.
2) If you're on MacOS, you can run `make build && docker-compose up --build` to build the project and spin up the development docker container.
3) If you're on another OS, you will have to use `go build` with the relevant flags for your system.

# Table of Contents
1. [Configuration](#configuration)
2. [Eviction](#eviction)
3. [Contribution](#contribution)

# Configuration

EchoVault is highly configurable. It provides the following configuration options to you:

Flag: `--config`<br/>
Type: `string/path`<br/>
Description: The file path for the server configuration. A JSON or YAML file can be used for server configuration. You can combine CLI flags and config files, but remember that config files override CLI flags. The config file will be prioritised if you have the same config option in the CLI flags and the config file.

Flag: `--port`<br/>
Type: `integer`<br/>
Description: The port on which to listen to client connections. The default is `7480`.

Flag: `--bind-addr`<br/>
Type: `string`<br/>
Description: Specify the IP address to which the listener is bound.

Flag: `--require-pass`<br/>
Type: `boolean`<br/>
Description: Determines whether the server should require a password for the default user before allowing commands. The default is `false`. If this option is provided, it must be accompanied by the `--password` config.

Flag: `--password`<br/>
Type: `string`<br/>
Description: The password used to authorize the default user to run commands. This flag should be provided alongside the `--require-pass` flag.

Flag: `--tls`<br/>
Type: `boolean`<br/>
Description: A TLS connection with a client is required. The default is `false`.

Flag: `mtls`<br/>
Type: `boolean`<br/>
Description: Require mTLS connection with client. It is useful when the client and the server need to verify each other. If `--tls` and `mtls` are provided, `--mtls` will take higher priority. The default is `false`.

Flag: `--cert-key-pair`<br/>
Type: `string`<br/>
Description: The cert/key pair used by the server to authenticate itself to the client when using TLS or mTLS. This flag can be provided multiple times with multiple cert/key pairs. This is a comma-separated string in the following format: `<path-to-cert>,<path-to-key>`,

Flag: `--client-ca`<br/>
Type: `string`<br/>
Description: The path to the RootCA that is used to verify client certs when the `--mtls` flag is provided to enable verifying the client. This flag can be passed multiple times with paths to several client RootCAs.

Flag: `--server-id`<br/>
Type: `string`<br/>
Description: If this node is part of a raft replication cluster, then this flag provides the server ID to use within the cluster configuration. This ID must be unique to all the other nodes' IDs in the cluster.

Flag: `--join-addr`<br/>
Type: `string`<br/>
Description: When adding a node to a replication cluster, this is the address and port of any cluster member. The current node will use this to request permission to join the cluster. The format of this flag is `<ip-address>:<memberlist-port>`.

Flag: `--raft-port`<br/>
Type: `integer`<br/>
Description: If starting a node in a raft replication cluster, this port will be used for communication between nodes on the raft layer. The default is `7481`.

Flag: `--memberlist-port`<br/>
Type: `integer`<br/>
Description. If starting a node in a replication cluster, this port is used for communication between nodes on the memberlist layer. The default is `7946`.

Flag: `--in-memory`<br/>
Type: `boolean`<br/>
Description: When starting a node in a raft replication cluster, this directs the raft layer to store logs and snapshots in memory. It is only recommended in test mode. The default is `false`.

Flag: `--data-dir`<br/>
Type: `string`<br/>
Description: The directory for storing Append-Only Logs, Write Ahead Logs, and Snapshots. The default is `/var/lib/echovault`

Flag: `--bootstrap-cluster`<br/>
Type: `boolean`<br/>
Description: Whether to initialize a new replication cluster with this node as the leader. The default is `false`.

Flag: `--acl-config`<br/>
Type: `string`<br/>
Description: The file path for the ACL layer config file. The ACL configuration file can be a YAML or JSON file.

Flag: `--snapshot-threshold`<br/>
Type: `integer`<br/>
Description: The number of write commands required to trigger a snapshot. The default is `1,000`

Flag: `--snapshot-interval`<br/>
Type: `string`<br/>
Description: The interval between snapshots. You can provide a parseable time format such as `30m45s` or `1h45m`. The default is 5 minutes.

Flag: `--restore-snapshot`<br/>
Type: `boolean`<br/>
Description: Determines whether to restore from a snapshot on startup. The default is `false`.

Flag: `--restore-aof`<br/>
Type: `boolean`<br/>
Description: This flag determines whether to restore from an aof file on startup. If both this flag and `--restore-snapshot` are provided, this flag will take higher priority.

Flag: `--forward-commands`<br/>
Type: `boolean`<br/>
Description: This flag allows you to send write commands to any node in the cluster. The node will forward the command to the cluster leader. When this is false, write commands can only be accepted by the leader. The default is `false`.

Flag: `--max-memory`<br/>
Type: `string`<br/>
Examples: "200mb", "8gb", "1tb"<br/>
Description: The maximum memory usage that EchoVault should observe. Once this limit is reached, the chosen key eviction strategy is triggered. The default is no limit.

Flag: `--eviction-policy`<br/>
Type: `string`<br/>
Description: This flag allows you to choose the key eviction strategy when the maximum memory is reached. The flag accepts the following options:<br/>
1) noeviction - Do not evict any keys even when max-memory is exceeded. All new write operations will be rejected. This is the default eviction strategy.
2) allkeys-lfu - Evict the least frequently used keys when max-memory is exceeded.
3) allkeys-lru - Evict the least recently used keys when max-memory is exceeded.
4) volatile-lfu - Evict the least frequently used keys with an expiration when max-memory is exceeded.
5) volatile-lru - Evict the least recently used keys with an expiration when max-memory is exceeded.
6) allkeys-random - Evict random keys until we get under the max-memory limit when max-memory is exceeded.
7) volatile-random - Evict random keys with an expiration when max-memory is exceeded.

Flag: `--eviction-sample`<br/>
Type: `integer`<br/>
Description: An integer specifying the number of keys to sample when checking for expired keys. By default, EchoVault will sample 20 keys. The sampling is repeated if the number of expired keys found exceeds 20%.

Flag: `--eviction-interval`<br/>
Type: `string`<br/>
Example: "10s", "5m30s", "100ms"<br/>
Description: The interval between each sampling of keys to evict. By default, this happens every 100 milliseconds.

# Eviction

### Memory Limit
The memory limit can be set using the --max-memory config flag. This flag accepts a parsable memory value (e.g 100mb, 16gb). If the limit set is 0, then no memory limit is imposed. The default value is 0.

### Passive eviction
In passive eviction, the expired key is not deleted immediately once the expiry time is reached. The key will remain in the store until the next time it is accessed. When attempting to access an expired key, that is when the keys is deleted.

### Active eviction
Echovault will run a background goroutine that samples a set of volatile keys at a given interval. Any keys that are found to be expired will be deleted. If 20% or more of the sampled keys are deleted, then the process will immediately begin again. Otherwise, wait for the given interval until the next round of sampling/eviction. The default number of keys sampled is 20 and the default interval for sampling is 100 milliseconds. These can be configured using the --eviction-sample and --eviction-interval flags respectively.

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

# Contribution

Contributions are welcome! If you're interested in contributing,
feel free to clone the repository and submit a Pull Request.

Join the [Discord server](https://discord.gg/vt45CKfF) if you'd like to discuss your contribution and/or
be a part of the community.
