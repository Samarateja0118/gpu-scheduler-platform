// Command nodeagent simulates one or more GPU nodes. Each simulated node
// heartbeats to the control plane, receives the jobs bound to it, "runs"
// each one by waiting out its runtime, and reports completion. Jobs that
// disappear from the assignment list (preempted or cancelled) are stopped.
//
// No GPU is touched. The agent exists so the control plane can be run and
// failure-tested end to end on an ordinary Kubernetes cluster.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/controlplane"
)

var (
	running   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "nodeagent_jobs_running", Help: "Jobs running on simulated nodes."}, []string{"node"})
	finished  = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "nodeagent_jobs_finished_total", Help: "Jobs finished by result."}, []string{"result"})
	stopped   = prometheus.NewCounter(prometheus.CounterOpts{Name: "nodeagent_jobs_stopped_total", Help: "Jobs stopped because the control plane withdrew them."})
	hbErrors  = prometheus.NewCounter(prometheus.CounterOpts{Name: "nodeagent_heartbeat_errors_total", Help: "Failed heartbeats."})
	client    = &http.Client{Timeout: 5 * time.Second}
	apiToken  = os.Getenv("API_TOKEN")
	serverURL = strings.TrimRight(env("SCHEDULER_URL", "http://localhost:8080"), "/")
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	prometheus.MustRegister(running, finished, stopped, hbErrors)
	n, _ := strconv.Atoi(env("NODES", "4"))
	gpus, _ := strconv.Atoi(env("GPUS", "8"))
	failRate, _ := strconv.ParseFloat(env("FAIL_RATE", "0"), 64)
	hb, err := time.ParseDuration(env("HEARTBEAT", "2s"))
	if err != nil {
		slog.Error("bad HEARTBEAT", "err", err)
		os.Exit(1)
	}
	host, _ := os.Hostname()
	prefix := strings.ToLower(env("NODE_PREFIX", host))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
		srv := &http.Server{Addr: env("LISTEN_ADDR", ":9100"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		_ = srv.ListenAndServe()
	}()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		node := &simNode{name: fmt.Sprintf("%s-%d", prefix, i), gpus: gpus, gpuType: env("GPU_TYPE", "a100"), pool: os.Getenv("POOL"),
			failRate: failRate, jobs: map[string]context.CancelFunc{}}
		wg.Add(1)
		go func() { defer wg.Done(); node.run(ctx, hb) }()
	}
	slog.Info("simulating nodes", "count", n, "gpusPerNode", gpus, "server", serverURL)
	wg.Wait()
}

type simNode struct {
	name, gpuType, pool string
	gpus                int
	failRate            float64
	mu                  sync.Mutex
	jobs                map[string]context.CancelFunc
}

func post(ctx context.Context, path string, body, out any) error {
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+apiToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", path, resp.Status)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (n *simNode) run(ctx context.Context, every time.Duration) {
	// Jitter the start so many agents do not heartbeat in lockstep.
	time.Sleep(time.Duration(rand.Int63n(int64(every))))
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		var resp controlplane.HeartbeatResponse
		err := post(ctx, "/v1/nodes/"+n.name+"/heartbeat", controlplane.HeartbeatRequest{GPUType: n.gpuType, GPUs: n.gpus, Pool: n.pool}, &resp)
		if err != nil {
			hbErrors.Inc()
			if ctx.Err() == nil {
				slog.Warn("heartbeat failed", "node", n.name, "err", err)
			}
		} else {
			n.reconcile(ctx, resp)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (n *simNode) reconcile(ctx context.Context, resp controlplane.HeartbeatResponse) {
	n.mu.Lock()
	defer n.mu.Unlock()
	want := map[string]bool{}
	for _, a := range resp.Assignments {
		want[a.JobID] = true
		if _, ok := n.jobs[a.JobID]; ok {
			continue
		}
		jctx, cancel := context.WithCancel(ctx)
		n.jobs[a.JobID] = cancel
		deadline := a.StartedAt.Add(time.Duration(a.RuntimeSec * float64(time.Second)))
		go n.runJob(jctx, a.JobID, a.GPUIndices, deadline)
	}
	for id, cancel := range n.jobs {
		if !want[id] {
			cancel()
			delete(n.jobs, id)
			stopped.Inc()
			slog.Info("job withdrawn (preempted, cancelled or requeued)", "node", n.name, "job", id)
		}
	}
	running.WithLabelValues(n.name).Set(float64(len(n.jobs)))
}

func (n *simNode) runJob(ctx context.Context, id string, gpus []int, deadline time.Time) {
	slog.Info("job started", "node", n.name, "job", id, "gpus", gpus)
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Until(deadline)):
	}
	ok := rand.Float64() >= n.failRate
	msg := ""
	if !ok {
		msg = "simulated failure (FAIL_RATE)"
	}
	// Retry the report: a lost completion would hold GPUs forever.
	for attempt := 0; attempt < 10 && ctx.Err() == nil; attempt++ {
		if err := post(ctx, "/v1/jobs/"+id+"/complete", controlplane.CompleteRequest{Success: ok, Message: msg}, nil); err == nil {
			break
		}
		time.Sleep(time.Duration(attempt+1) * time.Second)
	}
	result := "succeeded"
	if !ok {
		result = "failed"
	}
	finished.WithLabelValues(result).Inc()
	n.mu.Lock()
	delete(n.jobs, id)
	n.mu.Unlock()
}
