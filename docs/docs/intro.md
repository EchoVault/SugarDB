---
sidebar_position: 1
---

# Getting started

## Embedded

Install SugarDB with: `go get github.com/echovault/sugardb`.

Here's an example of using SugarDB as an embedded library.
You can access all of SugarDB's commands using an ergonomic API.

```go
func main() {
  server, err := db.NewSugarDB()

  if err != nil {
    log.Fatal(err)
  }

  _, _, _ = server.Set("key", "Hello, world!", db.SETOptions{})

  v, _ := server.Get("key")
  fmt.Println(v) // Hello, world!

  // (Optional): Listen for TCP connections on this SugarDB instance.
  server.Start()
}
```

An embedded SugarDB instance can still be part of a cluster, and the changes triggered 
from the API will be consistent across the cluster.

If you want to configure the SugarDB instance, you can modify retrieve the default config and 
update its properties to suit your requirements.

```go
conf := db.DefaultConfig()
conf.ServerID = "ServerInstance1"

server, err := db.NewSugarDB(
  db.WithConfig(conf),
)

if err != nil {
  log.Fatal(err)
}
```

For more information on the available configuration values, 
check out the <a href="/docs/configuration">configuration page</a>.

You can also pass in a custom context using the `WithContext` option.

```go
ctx := context.WithValue(context.Background(), "name", "default")

server, err := db.NewSugarDB(
  db.WithContext(ctx),
)

if err != nil {
  log.Fatal(err)
}
```

## Client-Server

### Homebrew

To install via homebrew, run:
1) `brew tap echovault/echovault`
2) `brew install echovault/echovault/echovault`

Once installed, you can run the server with the following command:
`echovault --bind-addr=localhost --data-dir="path/to/persistence/directory"`

### Docker

`docker pull echovault/sugardb`

The full list of tags can be found [here](https://hub.docker.com/r/echovault/sugardb/tags).

### Container Registry

`docker pull ghcr.io/echovault/sugardb`

The full list of tags can be found [here](https://github.com/EchoVault/SugarDB/pkgs/container/echovault).

### Binaries

You can download the binaries by clicking on a release tag and downloading
the binary for your system.

### Clients

SugarDB uses RESP, which makes it compatible with existing Redis clients.
