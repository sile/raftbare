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
