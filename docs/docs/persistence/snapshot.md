---
sidebar_position: 1
---

# Snapshot

SugarDB can take periodic snapshots of the current data and store it on disk. There are 2 configuration values used to configure the snapshot behaviour:

- `--snapshot-threshold` - The number of write commands before a snapshot is triggered. The default number is 1,000 write commands.
- `--snapshot-interval` - The interval between snapshots. It accepts a parseable time format such as `30m45s` or `1h45m`. The default is 5 minutes.

To restore data from a snapshot, set the `--restore-snapshot` configuration flag to `true` when starting a new SugarDB instance. Make sure to set the `--data-dir` to the folder containing the snapshot file so SugarDB knows where to load the file from.

You can trigger a snapshot manually using the `SAVE` command.

When both of these configuration options are set, the snapshot is triggered by whichever one is reached first since the instance's initialization or the last snapshot.
