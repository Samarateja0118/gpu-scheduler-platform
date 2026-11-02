// Command scheduler runs the GPU scheduler control plane.
//
// Configuration is by environment variable:
//
//	LISTEN_ADDR      default :8080
//	DATABASE_URL     Postgres URL; empty keeps state in memory only
//	REDIS_ADDR       Redis host:port for the intake stream; empty uses an in-process queue
//	TENANTS_FILE     JSON file with [{"name","guaranteed","max","pool"}]
//	API_TOKEN        bearer token required for mutating calls (optional)
//	NODE_TIMEOUT     heartbeat expiry, default 15s
//	ORDERING         priority | fifo            (default priority)
//	PLACEMENT        bestfit | firstfit | spread (default bestfit)
//	PREEMPTION       true | false               (default true)
//	ENFORCE_QUOTA    true | false               (default true)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/controlplane"
	"github.com/Samarateja0118/gpu-scheduler-platform/internal/metrics"
	"github.com/Samarateja0118/gpu-scheduler-platform/internal/queue"
	"github.com/Samarateja0118/gpu-scheduler-platform/internal/scheduler"
	"github.com/Samarateja0118/gpu-scheduler-platform/internal/store"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envBool(k string, def bool) bool {
	v, err := strconv.ParseBool(env(k, strconv.FormatBool(def)))
	if err != nil {
		return def
	}
	return v
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := scheduler.DefaultConfig()
	cfg.Ordering = scheduler.Ordering(env("ORDERING", string(cfg.Ordering)))
	cfg.Placement = scheduler.Placement(env("PLACEMENT", string(cfg.Placement)))
	cfg.Preemption = envBool("PREEMPTION", cfg.Preemption)
	cfg.EnforceQuota = envBool("ENFORCE_QUOTA", cfg.EnforceQuota)
	nodeTimeout, err := time.ParseDuration(env("NODE_TIMEOUT", "15s"))
	if err != nil {
		return err
	}

	var tenants []scheduler.Tenant
	if f := os.Getenv("TENANTS_FILE"); f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &tenants); err != nil {
			return err
		}
	}

	var st store.Store = store.Nop{}
	if url := os.Getenv("DATABASE_URL"); url != "" {
		pg, err := store.OpenPostgres(ctx, url)
		if err != nil {
			return err
		}
		defer pg.Close()
		st = pg
	}
	var q queue.Queue = queue.NewMemory(10000)
	pingQueue := func(context.Context) error { return nil }
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		rq, err := queue.NewRedis(addr)
		if err != nil {
			return err
		}
		q, pingQueue = rq, rq.Ping
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)
	sched := scheduler.New(cfg, nil)
	srv := controlplane.New(controlplane.Options{NodeTimeout: nodeTimeout, APIToken: os.Getenv("API_TOKEN")}, sched, st, q, m)
	if err := srv.Restore(ctx, tenants); err != nil {
		return err
	}

	ready := func(ctx context.Context) error {
		if err := st.Ping(ctx); err != nil {
			return err
		}
		return pingQueue(ctx)
	}
	hs := &http.Server{Addr: env("LISTEN_ADDR", ":8080"), Handler: srv.Handler(reg, ready), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", hs.Addr, "policy", cfg)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	done := make(chan struct{})
	go func() { srv.Run(ctx); close(done) }()

	select {
	case err := <-errc:
		stop()
		<-done
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = hs.Shutdown(sctx)
	<-done
	slog.Info("shut down cleanly")
	return nil
}
