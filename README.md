> **⚠️ Experimental — a Gas City research build.** A fork of k3s-io/kine with a Dolt-backed [beads](https://github.com/gastownhall/beads) datastore driver (`--datastore-endpoint beads://…`): unmodified k3s on a store that keeps history, with branch and transaction control. Release candidate — see [`beads-v0.1.0-rc.1`](https://github.com/gascity/kine/releases/tag/beads-v0.1.0-rc.1). The API surface and capabilities are still being explored, and the binary is ~4× upstream (embedded Dolt). Not for production. The driver lives on the `beads-rc` branch.

Kine (Kine is not etcd)
=======================

Kine is an etcdshim that provides an etcd-compatible API on top of various backend datastores:
- SQL ([SQLite](/pkg/drivers/sqlite), [MySQL/MariaDB](/pkg/drivers/mysql), [Postgres](/pkg/drivers/pgsql))
- [Memory](/pkg/drivers/memory) (ephemeral btree)
- [NATS](/pkg/drivers/nats) (embedded or external JetStream)
- [T4](/pkg/drivers/t4) (standalone or with S3)

## Features
- Can be ran standalone so any k8s (not just K3s) can use Kine
- Implements the subset of the etcd V3 API required by the Kubernetes API server - not intendend to be usable as a generic etcd server
  - [Lease](https://pkg.go.dev/go.etcd.io/etcd/client/v3#Lease): Grant
  - [Watcher](https://pkg.go.dev/go.etcd.io/etcd/client/v3#Watcher): Watch, RequestProgress, Close
  - [KV](https://pkg.go.dev/go.etcd.io/etcd/client/v3#KV): Put, Get, GetStream, Delete, Compact, Txn
  - [Cluster](https://pkg.go.dev/go.etcd.io/etcd/client/v3#Cluster): MemberList
  - [Maintenance](https://pkg.go.dev/go.etcd.io/etcd/client/v3#Maintenance): Status

See an [example](/examples/minimal.md).

## High Availability

If you want to run multiple Kine instances (or multiple K3s server nodes, using the Kine embedded in K3s) you must use an external SQL database, or NATS, or T4.
* The SQLite and Memory backends support only a single Kine instance.
* The SQLite backend does not support use of shared-cache mode without WAL journal.
* The SQLite backend does not support use of temporary or in-memory databases; use the Memory backend instead.
* The SQLite backend does not not support running multiple instances against a shared DB file; either on a single host, or on shared storage mounted by multiple hosts.

## Developer Documentation

A high level flow diagram and overview of code structure is available at [docs/flow.md](/docs/flow.md).
