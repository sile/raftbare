raftbare
========

Differences with the paper
--------------------------

### Terminology

- Role


### Optimizations

- Term log entry (instead of no-op entry)
- Log index (0 based index with a first sentinel entry)
- AppendEntriesRPC reply (log entry ref)
   - Add last_entry instead of success
- Removed next_index in favor of ...
- Sequence numbers in some messages (instead of strict RPC semantics)
- InstallSnapshotRPC

### FAQ (or out of scope of this library)

- `Node::start()` after the local storage is cleared (but still in the cluster members)
- Join the same node to multiple clusters

## Coverage

```console
$ rustup component add llvm-tools-preview
$ cargo install cargo-llvm-cov

$ cargo llvm-cov
$ cargo llvm-cov --open
```
