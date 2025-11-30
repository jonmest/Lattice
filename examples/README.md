# Examples

## Cluster Demo

The `cluster.rs` example spins up a real 3-node Raft cluster on your local machine and runs client operations against it. This isn't a simulation—these are actual nodes talking over gRPC, running elections, and replicating state.

### What You'll See

```bash
cargo run --example cluster
```

Watch it:
1. **Bootstrap** - Three nodes start up on different ports
2. **Discovery** - Nodes connect to peers (with retries because timing)
3. **Election** - Nodes timeout and elect a leader
4. **Client ops** - PUT, GET, DELETE operations go through consensus
5. **Replication** - State gets replicated across the cluster

### The Details

Each node runs:
- **Raft server** on ports 50051, 50151, 50251
- **KV service** on ports 50152, 50252, 50352

The client tries each KV endpoint until it finds one that works. In a real deployment, you'd put a load balancer in front or use client-side leader discovery.

### Output Breakdown

```
=== Starting 3-Node Lattice Cluster ===

Failed to connect to peer [::1]:50151: transport error
```
Don't panic—this is expected. Node 1 starts before Node 2 exists. Distributed systems are about handling this gracefully.

```
[Node 127.0.0.1:50051] Starting Raft loop
[Node 127.0.0.1:50051] Raft server listening on [::1]:50051
```
Node is live and running the Raft state machine.

```
Connected to KV service at http://[::1]:50152
  PUT key1 = value1
  ✓ PUT successful
```
Client successfully wrote through consensus. This went through:
1. Client → Node 1 KV service
2. Node 1 → Log append
3. Leader → Replicate to followers
4. Followers → Acknowledge
5. Leader → Commit + apply
6. Response → Client

### Cleanup

The example creates log files in `./data/`:

```bash
rm -rf ./data
```

### Modifying the Example

Want to experiment? Try:
- **Add more nodes** - Extend `node_configs` vec
- **Simulate failures** - Add random delays or crashes
- **Benchmark throughput** - Loop the client operations and time it
- **Network partitions** - Use `iptables` to block ports

The code is straightforward—read it, modify it, break it, fix it.

---

*This example proves the system actually works. Unit tests are great, but there's nothing like watching actual nodes reach consensus in real time.*
