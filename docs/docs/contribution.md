---
sidebar_position: 4
---

# Contribution

Contributions are welcome! If you're interested in contributing,
feel free to clone the repository and submit a Pull Request.

Join the [discord server](https://discord.gg/JrG4kPrF8v) if you'd like to discuss your contribution and/or be a part of the community.

# Development Setup

Pre-requisites:

1. Go
2. Docker
3. Docker Compose
4. x86_64-linux-musl-gcc cross-compile toolchain as the development image is built for an Alpine container

Steps:

1. Clone the repository.
2. If you're on MacOS, you can run `make run` to build the project and spin up the development docker container.
3. If you're on another OS, you will have to use `go build` with the relevant flags for your system.
