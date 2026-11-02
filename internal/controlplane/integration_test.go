package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/metrics"
	"github.com/Samarateja0118/gpu-scheduler-platform/internal/queue"
	"github.com/Samarateja0118/gpu-scheduler-platform/internal/scheduler"
	"github.com/Samarateja0118/gpu-scheduler-platform/internal/store"
)

type harness struct {
	t      *testing.T
	srv    *Server
	http   *httptest.Server
	cancel context.CancelFunc
	done   chan struct{}
}

var testTenants = []scheduler.Tenant{{Name: "research", Guaranteed: 8, Max: 16}, {Name: "batch", Guaranteed: 0, Max: 16}}

// start boots a control plane on the given backends. Postgres and Redis are
// used when DATABASE_URL / REDIS_ADDR are set (CI sets both).
func start(t *testing.T, st store.Store, q queue.Queue) *harness {
	t.Helper()
	reg := prometheus.NewRegistry()
	srv := New(Options{NodeTimeout: time.Second, CycleInterval: 50 * time.Millisecond, APIToken: "secret"},
		scheduler.New(scheduler.DefaultConfig(), nil), st, q, metrics.New(reg))
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Restore(ctx, testTenants); err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, srv: srv, cancel: cancel, done: make(chan struct{})}
	h.http = httptest.NewServer(srv.Handler(reg, st.Ping))
	go func() { srv.Run(ctx); close(h.done) }()
	return h
}

func (h *harness) stop() {
	h.http.Close()
	h.cancel()
	<-h.done
}

func (h *harness) call(method, path string, body any, out any) int {
	h.t.Helper()
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	req, _ := http.NewRequest(method, h.http.URL+path, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func (h *harness) eventually(what string, f func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", what)
}

func (h *harness) job(id string) scheduler.Job {
	var j scheduler.Job
	h.call(http.MethodGet, "/v1/jobs/"+id, nil, &j)
	return j
}

func backends(t *testing.T) (store.Store, queue.Queue) {
	var st store.Store = store.Nop{}
	var q queue.Queue = queue.NewMemory(100)
	if url := os.Getenv("DATABASE_URL"); url != "" {
		pg, err := store.OpenPostgres(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = pgExec(pg, "TRUNCATE jobs, nodes, tenants, events")
		st = pg
	}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		rq, err := queue.NewRedis(addr)
		if err != nil {
			t.Fatal(err)
		}
		q = rq
	}
	return st, q
}

func TestAPIAuthAndValidation(t *testing.T) {
	h := start(t, store.Nop{}, queue.NewMemory(10))
	defer h.stop()
	req, _ := http.NewRequest(http.MethodPost, h.http.URL+"/v1/jobs", bytes.NewReader([]byte(`{}`)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401 without token, got %d", resp.StatusCode)
	}
	bad := []SubmitRequest{
		{Tenant: "nope", Priority: "batch", GPUs: 1, RuntimeSeconds: 1},
		{Tenant: "research", Priority: "urgent", GPUs: 1, RuntimeSeconds: 1},
		{Tenant: "research", Priority: "batch", GPUs: 0, RuntimeSeconds: 1},
		{Tenant: "research", Priority: "batch", GPUs: 17, RuntimeSeconds: 1},
		{ID: "Bad ID!", Tenant: "research", Priority: "batch", GPUs: 1, RuntimeSeconds: 1},
	}
	for _, b := range bad {
		if code := h.call(http.MethodPost, "/v1/jobs", b, nil); code != http.StatusBadRequest {
			t.Errorf("%+v: want 400, got %d", b, code)
		}
	}
}

// End to end through HTTP: submit, a fake node heartbeats and gets the
// assignment, reports completion; then preemption is visible to the node;
// then the control plane restarts and restores running work from Postgres.
func TestLifecycleAndRestart(t *testing.T) {
	st, q := backends(t)
	h := start(t, st, q)

	hb := func(h *harness, node string) []scheduler.Assignment {
		var r HeartbeatResponse
		if code := h.call(http.MethodPost, "/v1/nodes/"+node+"/heartbeat", HeartbeatRequest{GPUType: "a100", GPUs: 8}, &r); code != 200 {
			t.Fatalf("heartbeat %d", code)
		}
		return r.Assignments
	}
	hb(h, "node-a")
	if code := h.call(http.MethodPost, "/v1/jobs", SubmitRequest{ID: "low-1", Tenant: "batch", Priority: "batch", GPUs: 8, RuntimeSeconds: 3600}, nil); code != 202 {
		t.Fatalf("submit %d", code)
	}
	// Same id again: accepted at the API, ignored at admission.
	h.call(http.MethodPost, "/v1/jobs", SubmitRequest{ID: "low-1", Tenant: "batch", Priority: "batch", GPUs: 8, RuntimeSeconds: 3600}, nil)
	h.eventually("low-1 running", func() bool { return h.job("low-1").State == scheduler.JobRunning })
	if as := hb(h, "node-a"); len(as) != 1 || as[0].JobID != "low-1" || len(as[0].GPUIndices) != 8 {
		t.Fatalf("assignments: %+v", as)
	}

	// research is under its guarantee, so it reclaims the borrowed GPUs.
	h.call(http.MethodPost, "/v1/jobs", SubmitRequest{ID: "res-1", Tenant: "research", Priority: "standard", GPUs: 4, RuntimeSeconds: 3600}, nil)
	h.eventually("res-1 running", func() bool { return h.job("res-1").State == scheduler.JobRunning })
	if j := h.job("low-1"); j.State != scheduler.JobPending || j.Preemptions != 1 {
		t.Fatalf("low-1 should be preempted: %+v", j)
	}
	as := hb(h, "node-a")
	if len(as) != 1 || as[0].JobID != "res-1" {
		t.Fatalf("node should now run only res-1: %+v", as)
	}
	if code := h.call(http.MethodPost, "/v1/jobs/res-1/complete", CompleteRequest{Success: true}, nil); code != 204 {
		t.Fatalf("complete %d", code)
	}
	h.eventually("low-1 back on node", func() bool { return h.job("low-1").State == scheduler.JobRunning })

	if _, isPG := st.(*store.Postgres); !isPG {
		h.stop()
		t.Log("DATABASE_URL not set; skipping restart half")
		return
	}
	// Restart: a new control plane on the same database restores low-1 as
	// Running on node-a without a heartbeat in between.
	h.eventually("events persisted", func() bool { return len(h.srv.pers) == 0 })
	time.Sleep(200 * time.Millisecond)
	h.stop()
	h2 := start(t, st, q)
	defer h2.stop()
	if j := h2.job("low-1"); j.State != scheduler.JobRunning || j.Node != "node-a" {
		t.Fatalf("restore lost running job: %+v", j)
	}
	if j := h2.job("res-1"); j.State != scheduler.JobSucceeded {
		t.Fatalf("finished job should be readable from Postgres: %+v", j)
	}
	// If node-a never comes back, the job is requeued after the timeout.
	h2.eventually("requeue after node timeout", func() bool {
		j := h2.job("low-1")
		return j.State == scheduler.JobPending && j.Requeues == 1
	})
}

func pgExec(p *store.Postgres, sql string) (int64, error) {
	return p.Exec(context.Background(), sql)
}
