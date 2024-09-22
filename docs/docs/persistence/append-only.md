---
sidebar_position: 2
---

# Append-Only File

SugarDB offers an append-only log file which keeps track of every write command. The log can be configured to trigger a compaction once a certain threshold of write commands is reached.

## How it works

Whenever a write command is executed, the command is logged in an append-only log file. Once a configured threshold of write commands is reached, the log file is compacted using a snapshot of the current data and then a fresh append-only log file is started.

On restoration of data, SugarDB will first load the data from the snapshot, and then replay all the write commands from the latest log file. If there is not snapshot, it will simply replay the write commands in the log file.

To restore data from the AOF file, set the `--restore-aof` configuration flag to `true` when starting an SugarDB instance. Make sure to set the `--data-dir` to the folder containing the AOF file so SugarDB knows where to load the file from.

You can also trigger a manual compaction of the AOF file using the `REWRITEAOF` command.

## File sync

The append-only file strategy allows you to configure how often the file is flushed to disk. You can configure this using the `--aof-sync-strategy` flag. The valid options are:

- `everysec` - Sync the file every second. This is the default sync strategy.
- `always` - Sync the file with each write command that is logged.
- `no` - Do not sync the file manually, instead, let the OS kernel handle the file syncing whenever it deems fit.

<b>NOTE:</b> The behaviour described above is only relevant when running a standalone node. Logging and log-compaction in a replication cluster is handled through the `hashicorp/raft` package in the replication layer. At the moment, this is backed by `boltdb`, although there are plans to replace the boltdb dependency with the same append-only engine used by standalone nodes.
