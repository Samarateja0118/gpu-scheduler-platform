// Package queue is the job intake path. API handlers publish validated
// submissions; the scheduler consumes them, admits them and only then
// acknowledges. With Redis Streams this gives at-least-once intake that
// survives a scheduler restart (unacknowledged entries are redelivered),
// and admission is idempotent on job ID so redelivery is harmless.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/scheduler"
)

// Handler admits one job. Returning an error leaves the entry
// unacknowledged so it is retried.
type Handler func(ctx context.Context, j scheduler.Job) error

// Queue is an intake queue.
type Queue interface {
	Publish(ctx context.Context, j scheduler.Job) error
	// Consume blocks until ctx is done.
	Consume(ctx context.Context, h Handler) error
	// Depth returns the number of entries not yet acknowledged.
	Depth(ctx context.Context) (int64, error)
}

// ------------------------------------------------------------ in-memory

// Memory is a channel-backed queue for local runs and tests.
type Memory struct{ ch chan scheduler.Job }

func NewMemory(size int) *Memory { return &Memory{ch: make(chan scheduler.Job, size)} }

func (m *Memory) Publish(ctx context.Context, j scheduler.Job) error {
	select {
	case m.ch <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return errors.New("intake queue full")
	}
}

func (m *Memory) Consume(ctx context.Context, h Handler) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case j := <-m.ch:
			if err := h(ctx, j); err != nil {
				slog.Error("intake handler", "job", j.ID, "err", err)
			}
		}
	}
}

func (m *Memory) Depth(context.Context) (int64, error) { return int64(len(m.ch)), nil }

// ---------------------------------------------------------------- redis

const (
	stream = "gpusched:intake"
	group  = "scheduler"
)

// Redis uses a stream with one consumer group.
type Redis struct {
	c        *redis.Client
	consumer string
	maxLen   int64
}

func NewRedis(addr string) (*Redis, error) {
	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var err error
	for {
		if err = c.Ping(ctx).Err(); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(2 * time.Second):
		}
	}
	err = c.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return nil, err
	}
	host, _ := os.Hostname()
	return &Redis{c: c, consumer: host, maxLen: 100000}, nil
}

func (r *Redis) Publish(ctx context.Context, j scheduler.Job) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return r.c.XAdd(ctx, &redis.XAddArgs{Stream: stream, MaxLen: r.maxLen, Approx: true, Values: map[string]any{"job": b}}).Err()
}

func (r *Redis) Consume(ctx context.Context, h Handler) error {
	// First drain entries delivered to any consumer but never acked (a
	// previous scheduler pod that died mid-admission), then new ones.
	if err := r.claimStale(ctx, h, 0); err != nil && ctx.Err() == nil {
		slog.Error("claim stale intake entries", "err", err)
	}
	lastClaim := time.Now()
	for ctx.Err() == nil {
		if time.Since(lastClaim) > 30*time.Second {
			// Retry entries whose handler failed earlier.
			if err := r.claimStale(ctx, h, 30*time.Second); err != nil && ctx.Err() == nil {
				slog.Error("claim stale intake entries", "err", err)
			}
			lastClaim = time.Now()
		}
		res, err := r.c.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: r.consumer, Streams: []string{stream, ">"}, Count: 100, Block: 2 * time.Second}).Result()
		if errors.Is(err, redis.Nil) || ctx.Err() != nil {
			continue
		}
		if err != nil {
			slog.Error("xreadgroup", "err", err)
			time.Sleep(time.Second)
			continue
		}
		for _, s := range res {
			r.handle(ctx, s.Messages, h)
		}
	}
	return nil
}

func (r *Redis) claimStale(ctx context.Context, h Handler, minIdle time.Duration) error {
	start := "0-0"
	for {
		msgs, next, err := r.c.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: stream, Group: group, Consumer: r.consumer, MinIdle: minIdle, Start: start, Count: 100}).Result()
		if err != nil {
			return err
		}
		r.handle(ctx, msgs, h)
		if next == "0-0" || len(msgs) == 0 {
			return nil
		}
		start = next
	}
}

func (r *Redis) handle(ctx context.Context, msgs []redis.XMessage, h Handler) {
	for _, m := range msgs {
		raw, _ := m.Values["job"].(string)
		var j scheduler.Job
		if err := json.Unmarshal([]byte(raw), &j); err != nil {
			slog.Error("bad intake entry, dropping", "id", m.ID, "err", err)
			r.c.XAck(ctx, stream, group, m.ID)
			continue
		}
		if err := h(ctx, j); err != nil {
			slog.Error("intake handler, will retry", "job", j.ID, "err", err)
			continue
		}
		r.c.XAck(ctx, stream, group, m.ID)
	}
}

func (r *Redis) Depth(ctx context.Context) (int64, error) {
	p, err := r.c.XPending(ctx, stream, group).Result()
	if err != nil {
		return 0, err
	}
	groups, err := r.c.XInfoGroups(ctx, stream).Result()
	if err != nil {
		return p.Count, nil
	}
	lag := int64(0)
	for _, g := range groups {
		if g.Name == group && g.Lag > 0 {
			lag = g.Lag
		}
	}
	return p.Count + lag, nil
}

func (r *Redis) Ping(ctx context.Context) error { return r.c.Ping(ctx).Err() }
