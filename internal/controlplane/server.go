// Package controlplane wires the scheduler to its HTTP API, the Redis
// intake queue, Postgres persistence and Prometheus metrics.
package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/metrics"
	"github.com/Samarateja0118/gpu-scheduler-platform/internal/queue"
	"github.com/Samarateja0118/gpu-scheduler-platform/internal/scheduler"
	"github.com/Samarateja0118/gpu-scheduler-platform/internal/store"
)

// Options configure the control plane.
type Options struct {
	NodeTimeout   time.Duration // heartbeat expiry
	CycleInterval time.Duration // max time between scheduling passes
	ForgetAfter   time.Duration // drop finished jobs from memory after this
	APIToken      string        // bearer token for mutating calls; empty disables auth
}

// Server is the control plane.
type Server struct {
	opts  Options
	sched *scheduler.Scheduler
	store store.Store
	queue queue.Queue
	m     *metrics.Metrics
	kick  chan struct{}
	pers  chan scheduler.Event

	mu        sync.Mutex
	knownNode map[string]scheduler.Node
}

// New builds a server. Call Restore before Run.
func New(opts Options, s *scheduler.Scheduler, st store.Store, q queue.Queue, m *metrics.Metrics) *Server {
	if opts.NodeTimeout == 0 {
		opts.NodeTimeout = 15 * time.Second
	}
	if opts.CycleInterval == 0 {
		opts.CycleInterval = time.Second
	}
	if opts.ForgetAfter == 0 {
		opts.ForgetAfter = 10 * time.Minute
	}
	srv := &Server{opts: opts, sched: s, store: st, queue: q, m: m,
		kick: make(chan struct{}, 1), pers: make(chan scheduler.Event, 50000), knownNode: map[string]scheduler.Node{}}
	s.OnEvent(func(e scheduler.Event) {
		m.Observe(e)
		// Blocking send: if Postgres falls 50k events behind, scheduling
		// stalls rather than silently losing durable state.
		srv.pers <- e
	})
	return srv
}

// Restore rebuilds scheduler state from the store and applies the
// configured tenants (config wins over stored values).
func (s *Server) Restore(ctx context.Context, tenants []scheduler.Tenant) error {
	stored, err := s.store.Tenants(ctx)
	if err != nil {
		return err
	}
	for _, t := range append(stored, tenants...) {
		if err := s.sched.UpsertTenant(t); err != nil {
			return err
		}
	}
	for _, t := range tenants {
		if err := s.store.UpsertTenant(ctx, t); err != nil {
			return err
		}
	}
	nodes, err := s.store.Nodes(ctx)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		s.sched.RestoreNode(n)
		s.knownNode[n.Name] = n
	}
	jobs, err := s.store.ActiveJobs(ctx)
	if err != nil {
		return err
	}
	running := 0
	for _, j := range jobs {
		s.sched.Restore(j)
		if j.State == scheduler.JobRunning {
			running++
		}
	}
	slog.Info("restored state", "tenants", len(stored), "nodes", len(nodes), "activeJobs", len(jobs), "running", running)
	return nil
}

// Run starts background loops and blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) {
	var wg sync.WaitGroup
	loop := func(f func()) { wg.Add(1); go func() { defer wg.Done(); f() }() }
	loop(func() { s.persistLoop(ctx) })
	loop(func() {
		if err := s.queue.Consume(ctx, s.admit); err != nil {
			slog.Error("intake consumer stopped", "err", err)
		}
	})
	loop(func() { s.scheduleLoop(ctx) })
	loop(func() {
		every(ctx, 2*time.Second, func() {
			if down := s.sched.ExpireNodes(s.opts.NodeTimeout); len(down) > 0 {
				slog.Warn("nodes lost heartbeat; jobs requeued", "nodes", down)
				s.Kick()
			}
		})
	})
	loop(func() {
		every(ctx, 2*time.Second, func() {
			s.m.Update(s.sched.Snapshot())
			if d, err := s.queue.Depth(ctx); err == nil {
				s.m.IntakeDepth.Set(float64(d))
			}
			s.m.PersistBacklog.Set(float64(len(s.pers)))
		})
	})
	loop(func() { every(ctx, time.Minute, func() { s.sched.Forget(s.opts.ForgetAfter) }) })
	wg.Wait()
}

func every(ctx context.Context, d time.Duration, f func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}

// Kick requests a scheduling pass soon.
func (s *Server) Kick() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

func (s *Server) scheduleLoop(ctx context.Context) {
	t := time.NewTicker(s.opts.CycleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-s.kick:
			// Coalesce bursts of kicks (e.g. many submissions) into one pass.
			time.Sleep(50 * time.Millisecond)
		}
		st := s.sched.Schedule()
		s.m.CycleDuration.Observe(st.Duration.Seconds())
		if st.Bound > 0 || st.Preempted > 0 {
			slog.Info("schedule pass", "considered", st.Considered, "bound", st.Bound, "preempted", st.Preempted, "quotaHeld", st.QuotaHeld, "unplaced", st.Unplaced, "took", st.Duration)
		}
	}
}

func (s *Server) persistLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Flush what is buffered so a clean shutdown loses nothing.
			fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for {
				select {
				case e := <-s.pers:
					_ = s.store.SaveEvent(fctx, e)
				default:
					return
				}
			}
		case e := <-s.pers:
			backoff := 100 * time.Millisecond
			for {
				err := s.store.SaveEvent(ctx, e)
				if err == nil || ctx.Err() != nil {
					break
				}
				s.m.PersistErrors.Inc()
				slog.Error("persist event, retrying", "type", e.Type, "err", err)
				time.Sleep(backoff)
				if backoff < 5*time.Second {
					backoff *= 2
				}
			}
		}
	}
}

// admit is the intake handler: submit to the scheduler; duplicates are
// treated as success so redelivered stream entries are harmless.
func (s *Server) admit(ctx context.Context, j scheduler.Job) error {
	err := s.sched.Submit(j)
	switch {
	case err == nil:
		s.Kick()
		return nil
	case err == scheduler.ErrDuplicateJob:
		return nil
	default:
		// Validation passed at the API, so this is e.g. a tenant deleted in
		// between. Record it rather than retrying forever.
		slog.Warn("job rejected at admission", "job", j.ID, "err", err)
		return nil
	}
}

func (s *Server) registerNode(ctx context.Context, n scheduler.Node) error {
	if err := s.sched.RegisterNode(n.Name, n.GPUType, n.GPUs, n.Pool); err != nil {
		return err
	}
	s.mu.Lock()
	prev, ok := s.knownNode[n.Name]
	changed := !ok || prev.GPUType != n.GPUType || prev.GPUs != n.GPUs || prev.Pool != n.Pool
	if changed {
		s.knownNode[n.Name] = n
	}
	s.mu.Unlock()
	if changed {
		s.Kick()
		return s.store.UpsertNode(ctx, n)
	}
	return nil
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "job-" + hex.EncodeToString(b)
}
