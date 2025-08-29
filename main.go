package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	crdbpool "github.com/authzed/crdbpool/pkg"
)

func mustParseConfig(dsn string) *pgxpool.Config {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		fmt.Println("error parsing config:", err)
		os.Exit(1)
	}
	return cfg
}

func main() {
	var (
		itersShort int
		itersLong int
		timeoutShort time.Duration
		timeoutLong time.Duration
		readerShort int
		readerLong int
		writerShort int
		writerLong int
		readerSleepShort time.Duration
		readerSleepLong time.Duration
		writerSleepShort time.Duration
		writerSleepLong time.Duration
	)
	flag.IntVar(&itersShort, "i", 0, "short for --iterations: number of iterations for reader and writer workloads")
	flag.IntVar(&itersLong, "iterations", 0, "number of iterations for reader and writer workloads")
	flag.DurationVar(&timeoutShort, "t", 0, "short for --timeout: overall workload timeout (e.g., 30s, 2m, 1h)")
	flag.DurationVar(&timeoutLong, "timeout", 0, "overall workload timeout (e.g., 30s, 2m, 1h)")
	flag.IntVar(&readerShort, "r", 0, "short for --reader-max-conns: max connections for reader pool")
	flag.IntVar(&readerLong, "reader-max-conns", 0, "max connections for reader pool")
	flag.IntVar(&writerShort, "w", 0, "short for --writer-max-conns: max connections for writer pool (if 0, derived as 1/3 of reader)")
	flag.IntVar(&writerLong, "writer-max-conns", 0, "max connections for writer pool (if 0, derived as 1/3 of reader)")
	flag.DurationVar(&readerSleepShort, "rs", 0, "short for --reader-sleep: sleep between reader iterations (e.g., 50ms)")
	flag.DurationVar(&readerSleepLong, "reader-sleep", 0, "sleep between reader iterations (e.g., 50ms)")
	flag.DurationVar(&writerSleepShort, "ws", 0, "short for --writer-sleep: sleep between writer iterations (e.g., 50ms)")
	flag.DurationVar(&writerSleepLong, "writer-sleep", 0, "sleep between writer iterations (e.g., 50ms)")
	flag.Parse()

	// defaults
	iterations := 1000
	timeout := 5 * time.Minute
	readerMax := 12
	writerMax := 0 // 0 means derive from reader
	readerSleep := 50 * time.Millisecond
	writerSleep := 50 * time.Millisecond

	if itersLong > 0 {
		iterations = itersLong
	} else if itersShort > 0 {
		iterations = itersShort
	}
	if timeoutLong > 0 {
		timeout = timeoutLong
	} else if timeoutShort > 0 {
		timeout = timeoutShort
	}
	if readerLong > 0 {
		readerMax = readerLong
	} else if readerShort > 0 {
		readerMax = readerShort
	}
	if writerLong > 0 {
		writerMax = writerLong
	} else if writerShort > 0 {
		writerMax = writerShort
	}
	if readerSleepLong > 0 {
		readerSleep = readerSleepLong
	} else if readerSleepShort > 0 {
		readerSleep = readerSleepShort
	}
	if writerSleepLong > 0 {
		writerSleep = writerSleepLong
	} else if writerSleepShort > 0 {
		writerSleep = writerSleepShort
	}

	fmt.Println("config:",
		"iterations=", iterations,
		"timeout=", timeout,
		"reader-max-conns=", readerMax,
		"writer-max-conns=", func() int { if writerMax>0 { return writerMax } ; return int((readerMax+2)/3) }(),
		"reader-sleep=", readerSleep,
		"writer-sleep=", writerSleep,
	)

	ctx := context.Background()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		fmt.Println("DATABASE_URL is required")
		os.Exit(1)
	}

	// Health tracker shared by both pools
	ht, err := crdbpool.NewNodeHealthChecker(dsn)
	if err != nil {
		fmt.Println("error creating health tracker:", err)
		os.Exit(1)
	}
	ctxPoll, cancelPoll := context.WithCancel(ctx)
	defer cancelPoll()
	go ht.Poll(ctxPoll, 5*time.Second)

	// Base config from DSN
	baseCfg := mustParseConfig(dsn)

	// Reader pool: larger pool
	readerCfg := *baseCfg
	readerCfg.MaxConns = int32(readerMax)
	readerPool, err := crdbpool.NewRetryPool(ctx, "reader", &readerCfg, ht, 3, 200*time.Millisecond)
	if err != nil {
		fmt.Println("error creating reader pool:", err)
		os.Exit(1)
	}
	defer readerPool.Close()

	// Writer pool: default to 1/3 of reader connections (at least 1) unless specified
	writerCfg := *baseCfg
	if writerMax > 0 {
		writerCfg.MaxConns = int32(writerMax)
	} else {
		writerCfg.MaxConns = readerCfg.MaxConns / 3
		if writerCfg.MaxConns < 1 {
			writerCfg.MaxConns = 1
		}
	}
	writerPool, err := crdbpool.NewRetryPool(ctx, "writer", &writerCfg, ht, 3, 200*time.Millisecond)
	if err != nil {
		fmt.Println("error creating writer pool:", err)
		os.Exit(1)
	}
	defer writerPool.Close()

	// Concurrent demo workload
	ctxRun, cancelRun := context.WithTimeout(ctx, timeout)
	defer cancelRun()
	fmt.Println("starting concurrent workload with", iterations, "iterations and", timeout, "timeout")

	done := make(chan struct{})
	errCh := make(chan error, 2)

	go func() { // reader goroutine
		defer func() { done <- struct{}{} }()
		fmt.Println("[reader] goroutine started")
		for i := 0; i < iterations; i++ {
			select {
			case <-ctxRun.Done():
				fmt.Println("[reader] context done:", ctxRun.Err())
				return
			default:
			}
			if err := readerPool.QueryRowFunc(ctxRun, func(ctx context.Context, row pgx.Row) error {
				var now time.Time
				if err := row.Scan(&now); err != nil {
					return err
				}
				fmt.Println("[reader] ping", i+1, "DB time:", now)
				return nil
			}, "select now()" ); err != nil {
				errCh <- fmt.Errorf("reader: %w", err)
				return
			}
			time.Sleep(readerSleep)
		}
		fmt.Println("[reader] done")
	}()

	go func() { // writer goroutine
		defer func() { done <- struct{}{} }()
		fmt.Println("[writer] goroutine started")
		fmt.Println("[writer] ensuring table exists")
		if err := writerPool.QueryFunc(ctxRun, func(ctx context.Context, rows pgx.Rows) error { return nil }, "create table if not exists tmp_crush(id int primary key, ts timestamptz)" ); err != nil {
			errCh <- fmt.Errorf("writer DDL: %w", err)
			return
		}
		for i := 0; i < iterations; i++ {
			select {
			case <-ctxRun.Done():
				fmt.Println("[writer] context done:", ctxRun.Err())
				return
			default:
			}
			var ts time.Time
			if err := writerPool.QueryRowFunc(ctxRun, func(ctx context.Context, row pgx.Row) error {
				return row.Scan(&ts)
			}, "insert into tmp_crush (id, ts) values (1, now()) on conflict (id) do update set ts = now() returning ts" ); err != nil {
				errCh <- fmt.Errorf("writer upsert: %w", err)
				return
			}
			fmt.Println("[writer] upsert ok, ts:", ts)
			time.Sleep(writerSleep)
		}
		fmt.Println("[writer] done")
	}()

	// wait for both to finish or first error
	completed := 0
	for completed < 2 {
		select {
		case <-done:
			completed++
		case err := <-errCh:
			fmt.Println("error:", err)
			cancelRun()
		}
	}
	fmt.Println("workload complete")
}
