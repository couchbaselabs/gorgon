# Gorgon 

A distributed [Jepsen](https://jepsen.io)-like testing framework to test consistency claims of 
distributed databases under different kinds of workloads and nemeses. 

## Design Overview 

Gorgon runs as a set of distributed nodes modelled as docker containers with
the control node actively orchestrating the workflow and worker nodes forming the 
distributed database cluster.
Both control node and each of the worker nodes build and run the same binary 
in different modes - run and rpc. 

### Control Node (run mode)
The control node creates runners and invokes the workloads and various nemeses 
on the worker nodes, to which it connects via jRPC.
It is also responsible for initiating the database cluster on the worker nodes.
It drives the entire testing process and uses the created runners that execute entire 
workloads, which are individual testing scenarios combining a model and multiple
generators.
Generators produce and handle the lifecycle of normal and nemesis operations.
The control process then applies these operations on the cluster via the runner's 
clients and records the details of these operations in a history.
A checker is then used to analyze the test's history for correctness on 
consistency models and generate visualization reports validating the result of
the tests  

### Worker Nodes (rpc mode)
The worker nodes run the database cluster and create and listen on RPC servers
to accept operations from the control node. They then apply these operations 
and nemesis instructions on the database.
It registers RPC handlers for normal client operations and failure 
injection. 
It also manages database client creation that eventually performs the database
operation on the database 


## Usage 
### Docker setup 
The containers are self-contained:
-  Go compiler and toolchain are included in the Dockerfile builds
-  All Go dependencies are pre-packaged (deps.tgz, src.tgz)
-  Source code is bundled into the containers
-  Docker Compose orchestrates the entire cluster (control + worker nodes)

### Configuration Setup  

Workloads with their respective configurations are within the workloads directory under docker/gorgon_couchbase/control.

Framework level configuration options such as:
- nodes in the cluster  
- workload duration  
- concurrency (number of clients performing simultaneous operations)  

among others, these are passed with gorgon-prefixed flags like `-gorgon-nodes`, `-gorgon-workload-duration`, and `-gorgon-concurrency`.

Database settings such as username and password and other database-specific configuration such as replica count, durabilty levels etc are passed as cli options -user, -pass, -replicas, -durability etc

### Workload Match Pattern

The `-gorgon-match` flag selects which scenarios to run using a wildcard pattern. Runner names follow the format `<db>~<generator1>~<generator2>...`, so:
- `'*'` matches all workloads including the baseline
- `'*~*~*'` matches only workloads with at least two generators (i.e., nemesis scenarios)
- `'*~*Failover*'` matches only failover-related workloads

## Workloads & Nemeses

Each workload combines a baseline operation generator with zero or more nemesis generators that inject faults during the test. The available workloads are:

| Workload | Description |
|---|---|
| GetSet | Baseline key-value reads and writes with no faults |
| GetSet + Kill | Kills the `memcached` process on a node and observes recovery |
| GetSet + Network Partition | Partitions the cluster network while keeping the web UI port (8091) accessible |
| GetSet + Graceful Failover & Full Recovery | Gracefully fails over a node, then recovers it with a full recovery and rebalance |
| GetSet + Hard Failover & Full Recovery | Hard fails over a node, then recovers it with a full recovery and rebalance |

Failover and recovery are scheduled relative to the workload duration: failover fires at 25% and recovery at 75%, so the cluster runs in a degraded state for the middle 50% of the test.

## Consistency Checking & Output

After each workload completes, the recorded operation history is checked for correctness:

1. **Linearizability** is verified first using [Porcupine](https://github.com/anishathalye/porcupine). The history is partitioned by key to reduce state explosion.
2. If linearizability fails, **sequential consistency** is checked as a weaker fallback.

Violations produce `.html` visualization files that show the conflicting operations. All logs and visualizations are bundled into `files.tgz` at the end of the run.

## Project Structure

```
src/
  gorgon/                  # Framework core (database-agnostic)
    cmd/                   # CLI entry point, runner, worker logic
    checkers/              # Sequential consistency checker & visualization
    generators/            # Built-in instruction generators (GetSet)
    nemeses/               # Built-in nemeses (Kill, Network Partition)
    jrpc/                  # Authenticated JSON-RPC transport
    rpcs/                  # RPC service definitions for client, iptables, kill
    workloads/             # Workload construction helpers
    splitmix/              # Deterministic random number generator
    wildcard/              # Wildcard pattern matching for -gorgon-match
  gorgon_couchbase/        # Couchbase KV implementation
    kv/                    # Client, cluster setup, failover/recovery nemesis
    main.go                # Binary entry point, flag registration
docker/
  gorgon_couchbase/        # Docker Compose setup
    control/               # Control node Dockerfile, run script, workload configs
    node/                  # Worker node Dockerfile, init and run scripts
```

## Starting Tests

From `docker/gorgon_couchbase/`, run:

```bash
./up.sh
```

`up.sh` invokes `make`, which runs the `all` target of the Makefile. This regenerates `compose.yaml` (the cluster topology) and packages the latest source and dependencies into tarballs that are bundled into the container images. It then runs `docker compose up` to spin up the control node and worker nodes.

If the image definition has changed (e.g. Dockerfile or dependencies updated), pass `--build` to force a rebuild of the images before starting the containers:

```bash
./up.sh --build
```

Without `--build`, `docker compose up` may start containers from stale cached images even if the source or image definition has changed.