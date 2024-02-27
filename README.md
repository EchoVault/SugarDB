[![Go](https://github.com/EchoVault/EchoVault/workflows/Go/badge.svg)]() 
[![Go Report Card](https://goreportcard.com/badge/github.com/echovault/echovault)](https://goreportcard.com/report/github.com/echovault/echovault)
[![Go Coverage](https://github.com/EchoVault/EchoVault/wiki/coverage.svg)](https://raw.githack.com/wiki/EchoVault/EchoVault/coverage.html)
[![GitHub Release](https://img.shields.io/github/v/release/EchoVault/EchoVault)]()
<br/>
[![License: GPL v2](https://img.shields.io/badge/License-GPL_v2-blue.svg)](https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html)
<br/>
[![Discord](https://img.shields.io/discord/1211815152291414037?style=flat&logo=discord&link=https%3A%2F%2Fdiscord.gg%2Fvt45CKfF)](https://discord.gg/vt45CKfF)

<hr/>

<img alt="echovault_logo" src="./images/EchoVault GitHub Cover.png" width="5062" />

# What is EchoVault?

EchoVault is a highly configurable, distributed, in-memory data store and cache implemented in Go.
EchoVault aims to provide a rich set of data structures and functions for
manipulating data in memory, these data structures include, but are not limited to:
lists, sets, sorted sets, hashes, with much more to come in the near future.

EchoVault provides a persistence layer for increased reliability. Both Append-Only files 
and snapshots can be used to persist data to disk for recovery in case of unexpected shutdowns.

Replication is a core feature of EchoVault and is implemented using the RAFT algorithm, 
allowing you to create a fault-tolerant cluster of EchoVault nodes to improve reliability.
If you do not need a replication cluster, you can always run EchoVault
in standalone mode and have a fully capable single node.

EchoVault aims to not only be a server, but to be importable into existing 
projects in order to enhance them with EchoVault features, this 
capability is always being worked on and improved.

Speed and reliability are top priorities of Echovault, as a result,
we're always working to improve these 2 characteristics. While we might
not be there yet, we are consistently working to make gains
in this area.

# Features

Some key features offered by EchoVault include:

1) TLS and mTLS support with support for multiple server and client RootCAs
2) Replication clustering using RAFT algorithm
3) ACL Layer for user Authentication and Authorization
4) Distributed Pub/Sub functionality with consumer groups
5) Sets, Sorted Sets, Hashes
6) Persistence layer with Snapshots and Append-Only files

We are working hard to add more features to EchoVault to make it
much more powerful. Features in the roadmap include:

1) Eviction Policies to reduce memory footprint
2) Encryption for Snapshot and AOF files
3) Streams
4) Transactions
5) Bitmap
6) HyperLogLog
7) JSON
8) Improved Observability

# Installing

### Homebrew

To install via homebrew, run:
1) `brew tap echovault/echovault`
2) `brew install echovault/echovault/echovault`

Once installed, you can run the server with the following command:
`echovault --bind-addr=localhost --data-dir="path/to/persistence/direcotry"`

Next, [install the client via homebrew](https://github.com/EchoVault/EchoVault-CLI).

### Binaries

You can download the binaries by clicking the on a release tag and downloading
the binary for your system.

### Configuration
Checkout the [configuration wiki page](https://github.com/EchoVault/EchoVault/wiki/Configuration) for the possible configuration
flags

# Clients

- [CLI Client](https://github.com/EchoVault/EchoVault-CLI)
- Go (Coming Soon)
- JavaScript/TypeScript (Coming Soon)
- Java (Coming Soon)
- C# (Coming Soon)

# Development Setup

Pre-requisites:
1) Go
2) Docker
3) Docker Compose
4) x86_64-linux-musl-gcc cross-compile toolchain as the development image is build for an alpine container

Steps:
1) Clone the repository.
2) If you're on MacOS, you can run `make buld && docker-compose up --build` to build the project and spin up the development docker container.
3) If you're on another OS, you will have to use `go build` with the relevant flags for your system.

# Contribution

Contributions are welcome! If you're interested in contributing,
feel free to clone the repository and submit a Pull Request.

Join the [Discord server](https://discord.gg/vt45CKfF) if you'd like to discuss your contribution and/or
be a part of the community.
