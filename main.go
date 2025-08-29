package main

import (
"context"
"fmt"
"log"
"os"
"time"

"github.com/authzed/spicedb/internal/datastore/crdb/pool"
)

func main() {
connString := os.Getenv("CRDB_CONN_STRING")
if connString == "" {
log.Fatal("CRDB_CONN_STRING env var must be set (e.g.,
postgresql://user:pass@host:26257/defaultdb?sslmode=disable)")
}

// Context with a reasonable timeout for setup and queries
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

// Configure pool options (tweak as needed for your repro)
opts := pool.Options{
// Reasonable small pool for a repro
MaxConns:        5,
MinConns:        1,
ConnMaxLifetime: 30 * time.Minute,
ConnMaxIdleTime: 5 * time.Minute,
HealthCheckPeriod: 30 * time.Second,
// If the package has additional fields you care about, set them here
}

// Create the pool
p, err := pool.New(ctx, connString, opts)
if err != nil {
log.Fatalf("failed to create pool: %v", err)
}
defer p.Close()

// Acquire a connection
conn, release, err := p.Acquire(ctx)
if err != nil {
log.Fatalf("failed to acquire connection: %v", err)
}
defer release()

// Run a trivial query to verify connectivity
