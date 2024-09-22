---
sidebar_position: 5
---

# Persistence

SugarDB stores data in-memory but allows you to persist the data on disk. This offers a way to recover data upon restarting an instance.

There are 2 strategies for persisting data to disk:

- [Append-Only Files](./append-only)
- [Snapshots](./snapshot)

<b>NOTE:</b> In standalon mode, if both Append-Only and Snapshot strategies are configured, the append-only strategy will be used.
