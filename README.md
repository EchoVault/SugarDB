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

# Documentation

https://echovault.io/docs/intro




