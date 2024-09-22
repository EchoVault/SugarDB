---
sidebar_position: 7
---

# Access Control List

Access Control Lists enable you to add a layer of security to the SugarDB server or cluster. You can create users with associated rules and require clients to authorize before executing commands on the server.

SugarDB creates a default user upon startup. You can see this user by executing the following command:

```
> ACL LIST
1) "default on +@all +all %RW~* +&*"
```

The default user is enabled, and has access to all categories, commands, keys and pub/sub channels. Connections are associated with user by default.

You can configure the default user to require a passwords by using the following configuration options:

- `--require-pass` forces the SugarDB server to require a user to authenticate itself using a password and/or username.

- `--password` attaches the provided password to the default user.

## Authorization

The TCP client can authenticate itself using the `AUTH` command:

`AUTH <username> <password>` tries to authenticate the TCP connection with the provided username and password.

`AUTH <password>` tries to authenticate the TCP connection with the default user and the provided passsword.

Authorization is not supported in embedded mode. When an SugarDB instance is embedded, it autimatically has access to all the commands exposed by the API.

## Configuration files

You can configure ACL Rules by passing the path to the config file to the `--acl-config=<path/to/config/file>` flag. The configuration file can be either a YAML or JSON file.

### YAML Config example

```yaml
- Username: "user1"
  Enabled: true
  NoPassword: false
  NoKeys: false
  Passwords:
    - PasswordType: "plaintext"
      PasswordValue: "password1"
    - PasswordType: "SHA256"
      PasswordValue: "6cf615d5bcaac778352a8f1f3360d23f02f34ec182e259897fd6ce485d7870d4"
  IncludedCategories: ["*"]
  ExcludedCategories: []
  IncludedReadKeys: ["*"]
  IncludedWriteKeys: ["*"]
  IncludedPubSubChannels: ["*"]
  ExcludedPubSubChannels: []

- Username: "user2"
  Enabled: true
  NoPassword: false
  NoKeys: false
  Passwords:
    - PasswordType: "plaintext"
      PasswordValue: "password4"
    - PasswordType: "SHA256"
      PasswordValue: "8b2c86ea9cf2ea4eb517fd1e06b74f399e7fec0fef92e3b482a6cf2e2b092023"
  IncludedCategories: ["hash", "set", "sortedset", "list", "generic"]
  ExcludedCategories: []
  IncludedReadKeys: ["*"]
  IncludedWriteKeys: ["*"]
  IncludedPubSubChannels: ["user:channel:*"]
  ExcludedPubSubChannels: ["admin:channel:*"]
```

### JSON Config example

```json
[
  {
    "Username": "user1",
    "Enabled": true,
    "NoPassword": false,
    "NoKeys": false,
    "Passwords": [
      {
        "PasswordType": "plaintext",
        "PasswordValue": "password1"
      },
      {
        "PasswordType": "SHA256",
        "PasswordValue": "6cf615d5bcaac778352a8f1f3360d23f02f34ec182e259897fd6ce485d7870d4"
      }
    ],
    "IncludedCategories": ["*"],
    "ExcludedCategories": [],
    "IncludedReadKeys": ["*"],
    "IncludedWriteKeys": ["*"],
    "IncludedPubSubChannels": ["*"],
    "ExcludedPubSubChannels": []
  },
  {
    "Username": "user2",
    "Enabled": true,
    "NoPassword": false,
    "NoKeys": false,
    "Passwords": [
      {
        "PasswordType": "plaintext",
        "PasswordValue": "password4"
      },
      {
        "PasswordType": "SHA256",
        "PasswordValue": "8b2c86ea9cf2ea4eb517fd1e06b74f399e7fec0fef92e3b482a6cf2e2b092023"
      }
    ],
    "IncludedCategories": ["hash", "set", "sortedset", "list", "generic"],
    "ExcludedCategories": [],
    "IncludedReadKeys": ["*"],
    "IncludedWriteKeys": ["*"],
    "IncludedPubSubChannels": ["user:channel:*"],
    "ExcludedPubSubChannels": ["admin:channel:*"]
  }
]
```

## ACL rules

ACL rules allow you to add new user profiles and set fine-grained rules that determine what clients can do on the server.

The default user's rules are very permissive so if you want to restrict access, you will have to explicitly configure ACL rules. The default user can be configured too.

### Enable and disable users

- `on` - Enable this user. A TCP connection can authenticate as this user.
- `off` - Disable this user. It's impossible to authenticate as thsi user.

### Allow and disallow categories

- `+@all` - Allow this user to access all categories (aliased by `allCategories` and `+@*`). This overrides all other category access rules.
- `-@all` - Block this user from accessing any categories (aliased by `-@*`, and `nocommands`). This overrides all other category access rules.
- `+@<category>` - Allow this user to access the specified category. If updating an existing user, then this category will be added to the list of categories they are allowed to access.
- `-@<category>` - Block the user from accessing this specific category. If updating an existing user, then this category is removed from the list of categories the user is allowed to access.

If both `+@all` and `-@all` are specified, whichever one is specified last will take effect.

The `nocommands` flag will apply the `-@all` rule.

### Allow and disallow commands

- `+all` - Allow this user to execute all commands (aliased by `allCommands`). This overrides all other command access rules.
- `-all` - Block this user from executing any commands. This overrides all other command access rules.
- `+<command>` - Allow the user to access the specified command. In order to allow the user to access only a specific subcommand, you can use `+<command>|<subcommand>`.
- `-<command>` - Block this user from executing any commands. In order to allow the user to access only a specific subscommand, you can user `-<command>|<subcommand>`.

If both `+all` and `-all` are specified, whichever one is specified last will take effect.

The `nocommands` flag will apply the `-all` rule.

### Allow and disallow access to keys

By default, SugarDB allows each user to read and write to all keys. If you'd like to control what keys users have access to and what they can do with those keys, you can make use of the following options:

- `%RW~*` - Allow this user to read and write all keys on the SugarDB instance (aliased by `allKeys`).
- `%RW~<key>` - Allow this user to read and write to the specified key. This option accepts a glob pattern for the key which allows you to restrict certain key patterns.
- `%W~*` - Allow the user to write to all keys.
- `%W~<key>` - Block the user from writing to any keys except the one specified. A glob pattern can be used in place of the key.
- `%R~*` - Allow the user to read from all the keys.
- `%R~<key>` - Block the user from reading any keys except the one specified. A glob pattern can be used in place of the key.

### Allow and disallow Pub/Sub channels

- `+&*` - Allow this user to access all pub/sub channels (aliased by `allChannels`).
- `-&*` - Block this user from accessing any of the pub/sub channels.
- `+&<channel>` - Allow this user to access the specied channel. This rule accepts a glob pattern (e.g. "channel\*").
- `-&<channel>` - Block this user from accessing the specied channel. This rule accepts a glob pattern (e.g. "channel\*").

If both `+&*` and `-&*` are specified, the one specified last will take effect.

### Add and remove passwords

By default users have no password and require no password to authenticate against them except when the `--require-pass` configuration is `true`. You can add and remove passwords associated with a user using the following options:

- `><password>` - Adds the plaintext password to the list of passwords associated with the user.
- `<<password>` - Removes the plaintext password from the list of passwords associated with the user.
- `#<hash>` - Adds the hash to the list of passwords associated with the user. The hash must be a SHA256 hash. When the user is being authenticated, they provide a plaintext passwords and the passwords will be compared with the user's plaintext passwords. If no match is found, the password's SHA256 hash is compared with the list of password hashes associated with the user.
- `!<hash>` - Removes the SHA256 hash from the list of passwords hashes associated with the user.

### Reset the user

You can pass certain flags to make sweeping updates to a user's ACL rules. These flags often reset the granular rules specified above.

- `nopass` - Deletes all the user's associated passwords. Future TCP connections will not need to provide a password to authenticate against this user.
- `resetpass` - Deletes all the user's associated passwords, but does not set the `nopass` flag to true.
- `nocommands` - Blocks the user from executing any commands.
- `resetkeys` - Blocks the user from accesssing any keys for both reads and writes (aliased by `nokeys`).
- `resetchannels` - Allows the user to access all pub/sub channels.

## Examples

For examples on how to create and update ACL users and their rules, checkout out the `ACL SETUSER` command documentation.
