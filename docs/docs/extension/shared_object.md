# Shared Object Files

EchoVault allows you to extend its list of commands using shared object files. You can write Go scripts that are compiled in plugin mode to achieve this.

## Creating a Module

To demonstrate the creation of a new module, we will create a plugin that adds a command with the keyword `Module.Set`. The command will have the format `Module.Set key <int>`. It's parameters will be a key to write to and an integer value.

Below is an example of the Go plugin script:

```go
package main

import (
  "context"
  "fmt"
  "strconv"
  "strings"
)

// The command keyword.
var Command string = "Module.Set"

// The list of categories this command belongs to.
// You can use built-in categories or your own custom categories.
var Categories []string = []string{"write", "fast"}

// The command's description.
var Description string = `(Module.Set key value)
This module stores the given value at the specified key. The value must be an integer`

// Whether the command should be synced across all nodes in a raft cluster.
// This is ignores in standalone mode.
var Sync bool = true

// The key extraction function.
func KeyExtractionFunc(
  cmd []string,   // The command slice (e.g []string{"Module.Set", "key1", "10"}).
  args ...string, // Args passed from module loading.
) (
  // Slice of keys the command handler will read from, extracted from the command slice.
  readKeys []string,
  // Slice of keys the command handler will write to, extracted from the command slice.
  writeKeys []string,
  // Error from key extraction handler.
  err error,
) {
  if len(cmd) != 3 {
    return nil, nil, fmt.Errorf("wrong no of args for %s command", strings.ToLower(Command))
  }
  return []string{}, cmd[1:2], nil
}

// The command's handler function.
func HandlerFunc(
  // Context passed from the EchoVault instance.
  ctx context.Context,
  // The command slice (e.g []string{"Module.Set", "key1", "10"}).
  command []string,
  // keysExist checks whether the keys exist in the store.
  // Returns a map with each key pointing to a corresponding boolean value
  // that states if the key exists.
  keysExist func(ctx context.Context, keys []string) map[string]bool,
  // getValues retrieves the values from the provided keys from the store.
  // Returns a map with each key pointing to the corresponding value.
  // If a key does not exist, its value will be nil.
  getValues func(ctx context.Context, keys []string) map[string]interface{},
  // setValues sets the values for each key in the store with the corresponding
  // value. If the value exists in the store, it is overwritten. If it does
  // not exist, it is created with the new value.
  setValues func(ctx context.Context, entries map[string]interface{}) error,
  // The arguments passed when the command is loaded.
  args ...string,
) (
  []byte, // Byte slice containing raw RESP response.
  error,
) {

  _, writeKeys, err := KeyExtractionFunc(command, args...)
  if err != nil {
    return nil, err
  }
  key := writeKeys[0]

  value, err := strconv.ParseInt(command[2], 10, 64)
  if err != nil {
    return nil, err
  }

  err = setValues(ctx, map[string]interface{}{key: value})
  if err != nil {
    return nil, err
  }

  return []byte("+OK\r\n"), nil
}
```

### Compiling Module File

Compiling plugins can be quite tricky due to Golang's plugin system. Make sure that the environment variables you set when compiling the module match the ones used when compiling EchoVault.

If you're using the official docker images, you can reference the <a target="_blank" href="https://github.com/EchoVault/EchoVault/blob/main/Dockerfile.dev">`Dockerfile.dev`</a> amd <a target="_blank" href="https://github.com/EchoVault/EchoVault/blob/main/Dockerfile.prod">`Dockerfile.prod`</a> files for reference on which flags you should use.

If you're building EchoVault from source, make sure the environment variables for the plugin and EchoVault compilation match.

Pass the -buildmode=plugin flag when compiling the plugin and the -o flag to specify a .so output file. Here's an example of a command to compile a plugin for the dev alpine docker image:

```
CGO_ENABLED=1 CC=gcc GOOS=linux GOARCH=amd64 go build -buildmode=plugin -o module_set.so module_set.go
```

## Loading Module

You can load modules in 3 ways:

### 1. At startup with the `--loadmodule` flag.

Upon startup you can provide the flag `--loadmodule="<path>/<to>/<module>.so"`. This is the path to the module's .so file. You can pass this flag multiple times to load multiple modules on startup.

### 2. At runtime with the `MODULE LOAD` command.

You can load modules dynamically at runtime using the `MODULE LOAD` command as follows:

```
MODULE LOAD <path>/<to>/<module>.so
```

This command only takes one path so if you have multiple modules to load, You will have to load them one at a time.

### 3. At runtime the the `LoadModule` method.

You can load a module .so file dynamically at runtime using the <a target="_blank" href="https://pkg.go.dev/github.com/echovault/echovault@v0.10.1/echovault#EchoVault.LoadModule">`LoadModule`</a> method in the embedded API.

```go
err = server.LoadModule("<path>/<to>/<module>.so")
```

### Loading Module with Args

You might have notices the `args ...string` variadic parameter when creating a module. This a list of args that are passed to the module's key extraction and handler functions.

The values passed here are established once when loading the module, and the same values will be passed to the respective functions everytime the command is executed.

If you don't provide any args, an empty slice will be passed in the args parameter. Otehrwise, a slice containing your defined args will be used.

To load a module with args using the embedded API:

```go
err = server.LoadModule("<path>/<to>/<module>.so", "list", "of", "args")
```

To load a module with args using the `MODULE LOAD` command:

```
MODULE LOAD <path>/<to>/<module>.so "list" "of" "args"
```

NOTE: You cannot pass args when loading modules at startup with the `--loadmodule` flag.

## List Modules

You can list the current modules loaded in the EchoVault instance using both the Client-Server and embedded APIs.

To check the loaded modules using the embedded API, use the <a target="_blank" href="https://pkg.go.dev/github.com/echovault/echovault@v0.10.1/echovault#EchoVault.ListModules">`ListModules`</a> method:

```go
modules := server.ListModules()
```

This method returns a string slice containing all the loaded modules in the EchoVault instance.

You can also list the loaded modules over the TCP API using the `MODULE LIST` command.

Here's an example response of the loaded modules:

```
 1) "acl"
 2) "admin"
 3) "connection"
 4) "generic"
 5) "hash"
 6) "list"
 7) "pubsub"
 8) "set"
 9) "sortedset"
10) "string"
11) "./modules/module_set/module_set.so"
```

Notice that the modules loaded from .so files have their respective file names as the module name.

## Execute Module Command

Here's an example of executing the `Module.Set` command with the embedded API:

Here's an example of executing the COPYDEFAULT custom command that we created previously:

```go
// Execute the custom COPYDEFAULT command
res, err := server.ExecuteCommand("Module.Set", "key1", "10")
if err != nil {
  fmt.Println(err)
} else {
  fmt.Println(string(res))
}
```

Here's how we would exectute the same command over the TCP client-server interface:

```
Module.Set key1 10
```

## Unload Module

You can unload modules from the EchoVault instance using both the embedded and TCP APIs.

Here's an example of unloading a module using the embedded API:

```go
// Unload custom module
server.UnloadModule("./modules/module_set/module_set.so")
// Unload built-in module
server.UnloadModule("sortedset")
```

Here's an example of unloading a module using the TCP interface:

```
MODULE UNLOAD ./modules/module_set/module_set.so
```

When unloading a module, the name should be equal to what's returned from the `ListModules` method or the `ModuleList` command.

## Important considerations

When loading external plugins to EchoVault in cluster mode, make sure to load the modules in all of the cluster's nodes. Otherwise, replication will fail as some nodes will not be able to handle the module's commands during replication.
