package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	crdbpool "github.com/authzed/crdbpool/pkg"
)

const (
	defaultIterations     = 1000
	defaultTimeout        = 5 * time.Minute
	defaultReaderMaxConns = 12
	defaultWriterSleep    = 50 * time.Millisecond
	defaultReaderSleep    = 50 * time.Millisecond
	defaultConcurrency    = 1
	healthPollInterval    = 5 * time.Second
	retryAttempts         = 3
	retryBackoff          = 200 * time.Millisecond
	sqlNow                = "select now()"
	sqlEnsureTable        = "create table if not exists tmp_crush(id int primary key, ts timestamptz)"
	sqlUpsertReturningTS  = "insert into tmp_crush (id, ts) values (1, now()) on conflict (id) do update set ts = now() returning ts"
)

type Config struct {
	Iterations  int
	Timeout     time.Duration
	ReaderMax   int
	WriterMax   int // 0 => derive from ReaderMax (1/3, min 1)
	ReaderSleep time.Duration
	WriterSleep time.Duration
	ReaderConc  int
	WriterConc  int
	DSN         string
}

func parseFlags() Config {
	var (
		itersShort       int
		itersLong        int
		timeoutShort     time.Duration
		timeoutLong      time.Duration
		readerShort      int
		readerLong       int
		writerShort      int
		writerLong       int
		readerSleepShort time.Duration
		readerSleepLong  time.Duration
		writerSleepShort time.Duration
		writerSleepLong  time.Duration
		readerConc       int
		writerConc       int
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
	flag.IntVar(&readerConc, "reader-conc", 0, "number of concurrent reader queries per iteration")
	flag.IntVar(&writerConc, "writer-conc", 0, "number of concurrent writer queries per iteration")
	flag.Parse()

	cfg := Config{
		Iterations:  defaultIterations,
		Timeout:     defaultTimeout,
		ReaderMax:   defaultReaderMaxConns,
		WriterMax:   0,
		ReaderSleep: defaultReaderSleep,
		WriterSleep: defaultWriterSleep,
		ReaderConc:  defaultConcurrency,
		WriterConc:  defaultConcurrency,
		DSN:         os.Getenv("DATABASE_URL"),
	}
	if itersLong > 0 {
		cfg.Iterations = itersLong
	} else if itersShort > 0 {
		cfg.Iterations = itersShort
	}
	if timeoutLong > 0 {
		cfg.Timeout = timeoutLong
	} else if timeoutShort > 0 {
		cfg.Timeout = timeoutShort
	}
	if readerLong > 0 {
		cfg.ReaderMax = readerLong
	} else if readerShort > 0 {
		cfg.ReaderMax = readerShort
	}
	if writerLong > 0 {
		cfg.WriterMax = writerLong
	} else if writerShort > 0 {
		cfg.WriterMax = writerShort
	}
	if readerSleepLong > 0 {
		cfg.ReaderSleep = readerSleepLong
	} else if readerSleepShort > 0 {
		cfg.ReaderSleep = readerSleepShort
	}
	if writerSleepLong > 0 {
		cfg.WriterSleep = writerSleepLong
	} else if writerSleepShort > 0 {
		cfg.WriterSleep = writerSleepShort
	}
	if readerConc > 0 {
		cfg.ReaderConc = readerConc
	}
	if writerConc > 0 {
		cfg.WriterConc = writerConc
	}
	return cfg
}

func validateConfig(cfg *Config) error {
	if cfg.DSN == "" {
		return errors.New("DATABASE_URL is required")
	}
	if cfg.Iterations <= 0 {
		return fmt.Errorf("iterations must be > 0 (got %d)", cfg.Iterations)
	}
	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be > 0 (got %s)", cfg.Timeout)
	}
	if cfg.ReaderMax <= 0 {
		return fmt.Errorf("reader-max-conns must be > 0 (got %d)", cfg.ReaderMax)
	}
	if cfg.ReaderConc <= 0 || cfg.WriterConc <= 0 {
		return fmt.Errorf("concurrency must be > 0 (reader=%d writer=%d)", cfg.ReaderConc, cfg.WriterConc)
	}
	return nil
}

func mustParsePoolConfig(dsn string) *pgxpool.Config {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("parse config: %v", err)
	}
	cfg.ConnConfig.Tracer = simpleTracer{}
	return cfg
}

func deriveWriterMax(readerMax int, writerMax int) int32 {
	if writerMax > 0 {
		return int32(writerMax)
	}
	wm := readerMax / 3
	if wm < 1 {
		wm = 1
	}
	return int32(wm)
}

func redactedDSNInfo(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "<invalid dsn>"
	}
	host := u.Host
	db := u.Path
	user := ""
	if u.User != nil {
		user = u.User.Username()
	}
	return fmt.Sprintf("host=%s db=%s user=%s", host, db, user)
}

func run(ctx context.Context, cfg Config) error {
	log.Printf("config: iterations=%d timeout=%s reader-max-conns=%d writer-max-conns=%d reader-sleep=%s writer-sleep=%s reader-conc=%d writer-conc=%d dsn(%s)",
		cfg.Iterations, cfg.Timeout, cfg.ReaderMax, func() int {
			if cfg.WriterMax > 0 {
				return cfg.WriterMax
			}
			return (cfg.ReaderMax + 2) / 3
		}(), cfg.ReaderSleep, cfg.WriterSleep, cfg.ReaderConc, cfg.WriterConc, redactedDSNInfo(cfg.DSN))

	baseCfg := mustParsePoolConfig(cfg.DSN)

	ht, err := crdbpool.NewNodeHealthChecker(cfg.DSN)
	if err != nil {
		return fmt.Errorf("create health tracker: %w", err)
	}
	ctxPoll, cancelPoll := context.WithCancel(ctx)
	defer cancelPoll()
	go ht.Poll(ctxPoll, healthPollInterval)

	readerCfg := *baseCfg
	readerCfg.MaxConns = int32(cfg.ReaderMax)
	readerCfg.ConnConfig.Tracer = baseCfg.ConnConfig.Tracer
	readerPool, err := crdbpool.NewRetryPool(ctx, "reader", &readerCfg, ht, retryAttempts, retryBackoff)
	if err != nil {
		return fmt.Errorf("create reader pool: %w", err)
	}
	defer readerPool.Close()

	writerCfg := *baseCfg
	writerCfg.MaxConns = deriveWriterMax(cfg.ReaderMax, cfg.WriterMax)
	writerCfg.ConnConfig.Tracer = baseCfg.ConnConfig.Tracer
	writerPool, err := crdbpool.NewRetryPool(ctx, "writer", &writerCfg, ht, retryAttempts, retryBackoff)
	if err != nil {
		return fmt.Errorf("create writer pool: %w", err)
	}
	defer writerPool.Close()

	ctxRun, cancelRun := context.WithTimeout(ctx, cfg.Timeout)
	defer cancelRun()
	log.Printf("starting concurrent workload with %d iterations and %s timeout", cfg.Iterations, cfg.Timeout)

	g, gctx := errgroup.WithContext(ctxRun)

	g.Go(func() error { // reader
		log.Printf("[reader] goroutine started")
		for i := 0; i < cfg.Iterations; i++ {
			select {
			case <-gctx.Done():
				log.Printf("[reader] context done: %v", gctx.Err())
				return gctx.Err()
			default:
			}
			// run cfg.ReaderConc concurrent SELECT now()
			grp, qctx := errgroup.WithContext(gctx)
			for j := 0; j < cfg.ReaderConc; j++ {
				grp.Go(func() error {
					err := readerPool.QueryRowFunc(qctx, func(ctx context.Context, row pgx.Row) error {
						var now time.Time
						if err := row.Scan(&now); err != nil {
							return err
						}
						log.Printf("[reader] ping %d DB time: %s", i+1, now.UTC().Format(time.RFC3339Nano))
						return nil
					}, sqlNow)
					if err != nil {
						log.Printf("[reader] query error: %v", err)
					}
					return nil
				})
			}
			if err := grp.Wait(); err != nil {
				log.Printf("[reader] batch error: %v (continuing)", err)
			}
			select {
			case <-gctx.Done():
				return gctx.Err()
			case <-time.After(cfg.ReaderSleep):
			}
		}
		log.Printf("[reader] done")
		return nil
	})

	g.Go(func() error { // writer
		log.Printf("[writer] goroutine started")
		log.Printf("[writer] ensuring table exists")
		if err := writerPool.QueryFunc(gctx, func(ctx context.Context, rows pgx.Rows) error { return nil }, sqlEnsureTable); err != nil {
			return fmt.Errorf("writer DDL: %w", err)
		}
		for i := 0; i < cfg.Iterations; i++ {
			select {
			case <-gctx.Done():
				log.Printf("[writer] context done: %v", gctx.Err())
				return gctx.Err()
			default:
			}
			grp, qctx := errgroup.WithContext(gctx)
			for j := 0; j < cfg.WriterConc; j++ {
				grp.Go(func() error {
					var ts time.Time
					if err := writerPool.QueryRowFunc(qctx, func(ctx context.Context, row pgx.Row) error { return row.Scan(&ts) }, sqlUpsertReturningTS); err != nil {
						log.Printf("[writer] query error: %v", err)
						return nil
					}
					log.Printf("[writer] upsert ok, ts: %s", ts.UTC().Format(time.RFC3339Nano))
					return nil
				})
			}
			if err := grp.Wait(); err != nil {
				log.Printf("[writer] batch error: %v (continuing)", err)
			}
			select {
			case <-gctx.Done():
				return gctx.Err()
			case <-time.After(cfg.WriterSleep):
			}
		}
		log.Printf("[writer] done")
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}
	log.Printf("workload complete")
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cfg := parseFlags()
	if err := validateConfig(&cfg); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}
