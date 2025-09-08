package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type simpleTracer struct{}

type traceStartKey struct{}

type traceStart struct {
	Start time.Time
}

func (t simpleTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	addr := safeRemoteAddr(conn)
	args := safeArgs(data.Args)
	log.Printf("[pgx] start sql=%q args=%s conn=%s", oneLine(data.SQL), args, addr)
	return context.WithValue(ctx, traceStartKey{}, traceStart{Start: time.Now()})
}

func (t simpleTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	v := ctx.Value(traceStartKey{})
	ts, _ := v.(traceStart)
	dur := time.Since(ts.Start)
	addr := safeRemoteAddr(conn)
	if data.Err != nil {
		log.Printf("[pgx] end   sql=%q dur=%s err=%v conn=%s", oneLine(data.CommandTag.String()), dur, data.Err, addr)
		return
	}
	log.Printf("[pgx] end   tag=%q rows=%d dur=%s conn=%s", data.CommandTag.String(), data.CommandTag.RowsAffected(), dur, addr)
}

func oneLine(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	return s
}

func safeArgs(args []any) string {
	vals := make([]string, len(args))
	for i, a := range args {
		b, err := json.Marshal(a)
		if err != nil {
			vals[i] = "<unmarshalable>"
			continue
		}
		vals[i] = string(b)
	}
	return "[" + strings.Join(vals, ", ") + "]"
}

func safeRemoteAddr(conn *pgx.Conn) string {
	if conn == nil {
		return "<nil>"
	}
	nc := conn.PgConn().Conn()
	if nc == nil {
		return "<nil>"
	}
	t := nc.RemoteAddr()
	if t == nil {
		return "<nil>"
	}
	if _, ok := t.(*net.TCPAddr); ok {
		return t.String()
	}
	return "<remote>"
}
