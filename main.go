package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	crdbpool "github.com/authzed/crdbpool/pkg"
)

func main() {
	ctx := context.Background()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		fmt.Println("DATABASE_URL is required")
		os.Exit(1)
	}

	ht, err := crdbpool.NewNodeHealthChecker(dsn)
	if err != nil {
		fmt.Println("error creating health tracker:", err)
		os.Exit(1)
	}
	ctxPoll, cancelPoll := context.WithCancel(ctx)
	defer cancelPoll()
	go ht.Poll(ctxPoll, 5*time.Second)

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		fmt.Println("error parsing config:", err)
		os.Exit(1)
	}

	rp, err := crdbpool.NewRetryPool(ctx, "example", cfg, ht, 3, 200*time.Millisecond)
	if err != nil {
		fmt.Println("error creating retry pool:", err)
		os.Exit(1)
	}
	defer rp.Close()

	for i := 0; i < 100; i++ {
		err = rp.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
			var now time.Time
			if err := row.Scan(&now); err != nil {
				return err
			}
			fmt.Println("ping", i+1, "DB time:", now)
			return nil
		}, "select now()")
		if err != nil {
			fmt.Println("query error:", err)
			os.Exit(1)
		}
	}
}
