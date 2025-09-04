# crdbpool-tester

A small Go CLI to exercise CockroachDB connectivity and pooling using github.com/authzed/crdbpool. It spins up separate reader and writer pools and runs concurrent workloads to validate pool behavior, node health tracking, and retry logic.

## Features
- Separate reader and writer pgx pools created via crdbpool.NewRetryPool
- Shared node health checker with background polling
- Concurrent reader workload (SELECT now()) and writer workload (DDL once, then UPSERT returning ts)
- Configurable iterations, timeouts, pool sizes, sleep intervals, and per-iteration concurrency
- Context-aware cancellation and error propagation via errgroup
- Safe logging (redacts DSN credentials; prints host/db/user only)

## Requirements
- Go 1.25+
- CockroachDB (or Postgres-compatible target) reachable from your machine
- Environment variable: DATABASE_URL (Postgres-style DSN)

Examples of DATABASE_URL:
- postgres://user:pass@localhost:26257/defaultdb?sslmode=disable
- postgresql://user:pass@crdb.example.com:26257/dbname?sslmode=require

## Quick start
```bash
# Install deps and build
go mod tidy
go build ./...

# Export your connection string
export DATABASE_URL='postgres://user:pass@localhost:26257/defaultdb?sslmode=disable'

# Run with defaults
go run .

# Run with custom settings
go run . \
  --iterations 200 \
  --timeout 2m \
  --reader-max-conns 16 \
  --writer-max-conns 6 \
  --reader-sleep 25ms \
  --writer-sleep 25ms \
  --reader-conc 4 \
  --writer-conc 2
```

## CLI flags
- -i, --iterations: number of iterations for reader and writer workloads (default: 1000)
- -t, --timeout: overall workload timeout (e.g., 30s, 2m, 1h; default: 5m)
- -r, --reader-max-conns: max connections for the reader pool (default: 12)
- -w, --writer-max-conns: max connections for the writer pool (default: derived as ~1/3 of reader, min 1)
- --reader-sleep: sleep between reader batches (default: 50ms)
- --writer-sleep: sleep between writer batches (default: 50ms)
- --reader-conc: number of concurrent reader queries per iteration (default: 1)
- --writer-conc: number of concurrent writer queries per iteration (default: 1)

Short forms:
- -i, -t, -r, -w, -rs, -ws are supported as short forms for iterations, timeout, reader-max-conns, writer-max-conns, reader-sleep, writer-sleep respectively.

## Behavior
- Reader: runs N concurrent SELECT now() queries per iteration, logs DB time, and sleeps.
- Writer: ensures table tmp_crush exists once, then runs N concurrent UPSERTs on a constant key returning the new timestamp per iteration, and sleeps. CockroachDB will serialize conflicting upserts; use distinct IDs if you want conflict-free parallelism.
- Both honor context deadlines and stop early on first error.

## Development
- Format, vet, build:
```bash
go fmt ./...
go vet ./...
go build ./...
```
- Run:
```bash
go run .
```
- Tests: none currently. Add *_test.go files and run with `go test ./...`.

## Notes
- Writer max conns default to roughly one third of the reader pool to reduce write pressure unless explicitly set.
- Health polling interval and retry settings are defined as constants in main.go.
- This tool does not log full DSNs to avoid leaking credentials; it logs host/db/user only.
