# Lattice Examples

## Cluster Example

This example demonstrates a 3-node Raft cluster with a KV store client.

### Running the Example

```bash
cargo run --example cluster
```

This will:
1. Start 3 Raft nodes on ports 50051, 50151, 50251
2. Each node exposes a KV service on ports 50152, 50252, 50352
3. A client will connect and perform operations (PUT, GET, DELETE)

### Expected Output

You should see:
- Each node starting and connecting to peers
- Raft loop messages
- Client operations being executed
- Consensus being reached across nodes

### Cleanup

The example creates log files in `./data/`. To clean up:

```bash
rm -rf ./data
```
