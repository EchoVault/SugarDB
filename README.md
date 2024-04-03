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
2) Replication clustering using RAFT algorithm
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

# Usage

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


# Installing

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

### Configuration
Checkout the [configuration wiki page](https://github.com/EchoVault/EchoVault/wiki/Configuration) for the possible configuration
flags

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

# Contribution

Contributions are welcome! If you're interested in contributing,
feel free to clone the repository and submit a Pull Request.

Join the [Discord server](https://discord.gg/vt45CKfF) if you'd like to discuss your contribution and/or
be a part of the community.
