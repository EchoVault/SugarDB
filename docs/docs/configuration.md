---
sidebar_position: 2
---

# Configuration

SugarDB is highly configurable. It provides the following configuration options to you:

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
Description: When adding a node to a replication cluster, this is the address and port of any cluster member. The current node will use this to request permission to join the cluster. The format of this flag is `<target-server-id>/<target-ip>:<target-port>`.

Flag: `--discovery-port`<br/>
Type: `integer`<br/>
Description. If starting a node in a replication cluster, this port is used for communication between nodes on the memberlist layer. The default is `7946`.

Flag: `--in-memory`<br/>
Type: `boolean`<br/>
Description: When starting a node in a raft replication cluster, this directs the raft layer to store logs and snapshots in memory. It is only recommended in test mode. The default is `false`.

Flag: `--data-dir`<br/>
Type: `string`<br/>
Description: The directory for storing Append-Only Logs, Write Ahead Logs, and Snapshots. The default is `/var/lib/`

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

Flag: `--aof-sync-strategy`<br/>
Type: `string`<br/>
Description: How often to flush the file contents written to append only file.
The options are `always` for syncing on each command, `everysec` to sync every second, and `no` to leave it up to the os.

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
Description: The maximum memory usage that SugarDB should observe. Once this limit is reached, the chosen key eviction strategy is triggered. The default is no limit.

Flag: `--eviction-policy`<br/>
Type: `string`<br/>
Description: This flag allows you to choose the key eviction strategy when the maximum memory is reached. The flag accepts the following options:<br/>

1. noeviction - Do not evict any keys even when max-memory is exceeded. All new write operations will be rejected. This is the default eviction strategy.
2. allkeys-lfu - Evict the least frequently used keys when max-memory is exceeded.
3. allkeys-lru - Evict the least recently used keys when max-memory is exceeded.
4. volatile-lfu - Evict the least frequently used keys with an expiration when max-memory is exceeded.
5. volatile-lru - Evict the least recently used keys with an expiration when max-memory is exceeded.
6. allkeys-random - Evict random keys until we get under the max-memory limit when max-memory is exceeded.
7. volatile-random - Evict random keys with an expiration when max-memory is exceeded.

Flag: `--eviction-sample`<br/>
Type: `integer`<br/>
Description: An integer specifying the number of keys to sample when checking for expired keys. By default, SugarDB will sample 20 keys. The sampling is repeated if the number of expired keys found exceeds 20%.

Flag: `--eviction-interval`<br/>
Type: `string`<br/>
Example: "10s", "5m30s", "100ms"<br/>
Description: The interval between each sampling of keys to evict. By default, this happens every 100 milliseconds.

Flag: `--loadmodule`<br/>
Type: `string/path`<br/>
Example: "path/to/module.so"<br/>
Description: The full file path to the .so file to load into SugarDB to extend its commands. This flag can be specified multiple times to load multiple plugins.
