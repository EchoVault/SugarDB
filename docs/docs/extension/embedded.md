# Embedded

EchoVault allows you to programmetically extend its list of commands ar runtime.

The <a target="_blank" href="https://pkg.go.dev/github.com/echovault/echovault@v0.10.1/echovault#EchoVault.AddCommand">`AddCommand`</a> method allows you to extend the EchoVault server by adding new commands and subcommands.

Each command can have its own handler and key extraction logic. This method ensures that commands are unique within the server and properly integrated with the existing command handling infrastructure.

## Method Definition

```go
func (server *EchoVault) AddCommand(command CommandOptions) error
```

## Parameters

- `command` - An instance of <a target="_blank" href="https://pkg.go.dev/github.com/echovault/echovault@v0.10.1/echovault#CommandOptions">`CommandOptions`</a> which provides the specification of the command to be added.

## Errors

- `"command <command> already exists"` - If a command with the same name as the provided command already exists in the server.

## Explanation

The `AddCommand` method performs the following steps:

1. **Command Uniqueness Check**: It checks if the command already exists in the server. If it does, it returns an error.
2. **Command Addition**:
   - **Without Subcommands**: If the command does not have subcommands, it adds the command directly to the server's command list.
   - **With Subcommands**: If the command has subcommands, it initializes a new command structure and iterates through the provided subcommands to add them to the server's command list.

## Execute Custom Commands

### Adding a Command without Subcommands

In this example, we will be adding a command `COPYDEFAULT` that reads the value from the first key and
copies it into the second key only if both keys exist.

If the first key does not exist, return an error. If the second key does not exist, the key will be created with the string value 'default'.

The command will have the following format: `COPYDEFAULT key1 key2`.

```go
// Define the key extraction function
func myKeyExtractionFunc(cmd []string) (echovault.CommandKeyExtractionFuncResult, error) {
  if len(cmd) != 3 {
    return echovault.CommandKeyExtractionFuncResult{}, errors.New("command must be length 3")
  }
  if cmd[1] == cmd[2] {
    return echovault.CommandKeyExtractionFuncResult{}, errors.New("keys must be different")
  }
  return echovault.CommandKeyExtractionFuncResult{
    ReadKeys:  []string{cmd[1]},
    WriteKeys: []string{cmd[2]},
  }, nil
}

// Define the command handler function
func myCommandHandler(params echovault.CommandHandlerFuncParams) ([]byte, error) {
  // Extract keys
  keys, err := myKeyExtractionFunc(params.Command)
  if err != nil {
    return nil, err
  }

  // Get the write and read keys.
  readKey, writeKey := keys.ReadKeys[0], keys.WriteKeys[0]

  keysExist := params.KeysExist(params.Context, []string{writeKey, readKey})

  // If readKey does not exist, return an error.
  if !keysExist[readKey] {
    return nil, fmt.Errorf("%s does not exist", readKey)
  }

  // If writeKey does not exist, set "default" value at the key.
  if !keysExist[writeKey] {
    err = params.SetValues(params.Context, map[string]interface{}{writeKey: "default"})
    return []byte("+OK\r\n"), err
  }

  // Set the value from readKey to writeKey.
  err = params.SetValues(params.Context, map[string]interface{}{
    writeKey: params.GetValues(params.Context, []string{readKey})[readKey],
  })
  return []byte("+OK\r\n"), err
}

func main() {
  server, err := echovault.NewEchoVault()
  if err != nil {
    log.Fatal(err)
  }

  _, _ = server.MSet(map[string]string{
    "key1": "value1",
    "key2": "value2",
  })

  // Define the command options
  command := echovault.CommandOptions{
    Command:    "COPYDEFAULT",             // Command keyword
    Module:     "generic",                 // Add command to generic module, can be a new custom module.
    Categories: []string{"write", "fast"}, // Can be custom categories here.
    Description: `(COPYDEFAULT key1 key2)
Copies the value from key1 to key2. If key1 does not exist, an error is returned. If key1 exists but key2
does not, the value "default" will be stored at key2. If both keys exist, the value from key1 will be copied to key2.`,
    Sync:              true,
    KeyExtractionFunc: myKeyExtractionFunc,
    HandlerFunc:       myCommandHandler,
  }

  // Add the command.
  err = server.AddCommand(command)
  if err != nil {
    fmt.Println("Error adding command:", err)
  } else {
    fmt.Println("Command added successfully")
  }
}
```

### Adding a Command with Subcommands

You can add a command with a list of subcommands by defining them in the `SubCommand` property
of `CommandOptions`.

```go
// Define the key extraction function for subcommands
func mySubCommandKeyExtractionFunc(cmd []string) (echovault.CommandKeyExtractionFuncResult, error) {
  return echovault.CommandKeyExtractionFuncResult{
    ReadKeys:  []string{"subkey1"},
    WriteKeys: []string{"subkey2"},
  }, nil
}

// Define the subcommand handler function
func mySubCommandHandler(params echovault.CommandHandlerFuncParams) ([]byte, error) {
  fmt.Println("Subcommand executed:", strings.Join(params.Command, " "))
  return []byte("+OK\r\n"), nil
}

func main() {
  server, err := echovault.NewEchoVault()
  if err != nil {
    log.Fatal(err)
  }

  // Define the subcommands
  subCommands := []echovault.SubCommandOptions{
    {
      Command:           "SUB1",
      Module:            "mymodule",
      Categories:        []string{"subcategory1"},
      Description:       "This is subcommand 1",
      Sync:              false,
      KeyExtractionFunc: mySubCommandKeyExtractionFunc,
      HandlerFunc:       mySubCommandHandler,
    },
    {
      Command:           "SUB2",
      Module:            "mymodule",
      Categories:        []string{"subcategory2"},
      Description:       "This is subcommand 2",
      Sync:              true,
      KeyExtractionFunc: mySubCommandKeyExtractionFunc,
      HandlerFunc:       mySubCommandHandler,
    },
  }

  // Define the main command options
  command := echovault.CommandOptions{
    Command:     "MYCOMMAND",
    Module:      "mymodule",
    Categories:  []string{"category1"},
    Description: "This is a sample command with subcommands",
    Sync:        true,
    SubCommand:  subCommands,
  }

  // Add the command to the server
  err := server.AddCommand(command)
  if err != nil {
    fmt.Println("Error adding command:", err)
  } else {
    fmt.Println("Command with subcommands added successfully")
  }
}
```

Although the example above shows subcommands that share a handler and key extraction function, in practice, each subcommand should provide its own unique key extraction and handler functions.

Note: If you provide a command handler for the top, level command, it will be ignored. Whenever
a command has subcommands, EchoVault will try to look for subcommands that match the second element
of the subcommand slice. If a subcommand cannot be found, an error is returned.

## Executing Custom Commands

You can use the custom command using the `ExecuteCommand` method. The method has the following definition:

```go
func (server *EchoVault) ExecuteCommand(command ...string) ([]byte, error)
```

It accepts a command of varying length to accomodate any custom command. The command passed is case insensitive. So "COPYDEFAULT" is considered the same as "copydefault".

The returned values are:

1. A byte slice containing the raw RESP returned from the custom command handler.
2. The error returned from the command handler in RESP error format.

### Execute Command without Subcommands

Here's an example of executing the COPYDEFAULT custom command that we created previously:

```go
// Set the values for key1 and key2
_, _ = server.MSet(map[string]string{
  "key1": "value1",
  "key2": "value2",
})

// Execute the custom COPYDEFAULT command
res, err := server.ExecuteCommand("COPYDEFAULT", "key1", "key2")
if err != nil {
  fmt.Println(err)
} else {
  fmt.Println(string(res))
}

// Execute COPYDEFAULT command with lower case parameters
res, err := server.ExecuteCommand("copydefault", "key1", "key2")
if err != nil {
  fmt.Println(err)
} else {
  fmt.Println(string(res))
}
```

### Execute Command with Subcommands

Example of executing custom subcommands created previously:

```go
// Execute subcommand 1
res, err := server.ExecuteCommand("MYCOMMAND", "SUB1")
if err != nil {
  fmt.Println(err)
} else {
  fmt.Println(string(res))
}

// Execute subcommand 2
res, err := server.ExecuteCommand("mycommand", "sub2")
if err != nil {
  fmt.Println(err)
} else {
  fmt.Println(string(res))
}
```

### Execute in TCP client

You can also execute programmatically added commands with a Redis client over TCP such as redis-cli. An example of executing the COPYDEFAULT commands looks as follows:

```
> COPYDEFAULT key1 key2
```

To execute one of the subcommands:

```
> MYCOMMAND SUB1
```

## Removing Commands

You can remove commands using the `RemoveCommand` method. This methods does not only remove programmatically added commands but any commands loaded into the EchoVault instance. Including built-in commands and commands loaded from shared object files.

The method has the following signature:

```go
func (server *EchoVault) RemoveCommand(command ...string)
```

It accepts a command or subcommand to remove. If you'd like to remove an entire command, including all it's subcommands, you can pass only the command name. If you'd like to remove a particular subcommand but retain the command and it's other subcommands, then you must pass the names of command and the subcommand you'd like to delete.

### Remove Command with no Subcommandsa

Example demonstrating how to remove the "COPYDEFAULT" command created previously.

```go
server.RemoveCommand("COPYDEFAULT")
```

### Remove a Subcommand

To remove the "SUB1" subcommand of the "MYCOMMAND" command, you can pass the following parameters:

```go
server.RemoveCommand("MYCOMMAND", "SUB1")
```

This leaves the "MYCOMMAND" command and "SUB2" subcommand available for execution.

### Remove an entire Command with Multiple Subcommands

If you'd like to remove the entirety of "MYCOMMAND" along with all its subcommands, you can pass the top-level command name as follows:

```go
server.RemoveCommand("MYCOMMAND")
```

### Example

## Important considerations

Programmatically extending EchoVault like this brings some challenges:

- If you're running in cluster mode, you have to make sure the custom command is added to all the nodes and that the command's key extraction and handler function implementations are exactly identical. Otherwise, the cluster will not be able to accurately sync the command's side effects across the cluster.
- When removing commands programmetically, you must make sure to remove the commands accross the entire cluster otherwise, the nodes with the missing command will not be able to replicate the command's side effects.

Due to the reasons above, it's recommended that programmatically adding/removing commands should be done in standalone mode. It can be done in a cluster, but you must be careful and take into account the considerations above.
