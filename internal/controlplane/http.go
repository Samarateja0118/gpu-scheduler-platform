package controlplane

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/scheduler"
	"github.com/Samarateja0118/gpu-scheduler-platform/internal/store"
)

var idRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// SubmitRequest is the body of POST /v1/jobs.
type SubmitRequest struct {
	ID             string  `json:"id,omitempty"`
	Tenant         string  `json:"tenant"`
	Priority       string  `json:"priority"` // batch | standard | critical
	GPUs           int     `json:"gpus"`
	GPUType        string  `json:"gpuType,omitempty"`
	RuntimeSeconds float64 `json:"runtimeSeconds"`
	// Preemptible defaults to true except for critical jobs.
	Preemptible *bool `json:"preemptible,omitempty"`
}

// HeartbeatRequest is the body of POST /v1/nodes/{name}/heartbeat.
type HeartbeatRequest struct {
	GPUType string `json:"gpuType"`
	GPUs    int    `json:"gpus"`
	Pool    string `json:"pool,omitempty"`
}

// HeartbeatResponse lists the jobs that should be running on the node.
type HeartbeatResponse struct {
	Assignments []scheduler.Assignment `json:"assignments"`
}

// CompleteRequest is the body of POST /v1/jobs/{id}/complete.
type CompleteRequest struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// Handler returns the HTTP API.
func (s *Server) Handler(reg prometheus.Gatherer, ready func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	auth := s.requireToken
	mux.HandleFunc("POST /v1/jobs", auth(s.submit))
	mux.HandleFunc("GET /v1/jobs", s.listJobs)
	mux.HandleFunc("GET /v1/jobs/{id}", s.getJob)
	mux.HandleFunc("DELETE /v1/jobs/{id}", auth(s.cancelJob))
	mux.HandleFunc("POST /v1/jobs/{id}/complete", auth(s.completeJob))
	mux.HandleFunc("POST /v1/nodes/{name}/heartbeat", auth(s.heartbeat))
	mux.HandleFunc("POST /v1/nodes/{name}/cordon", auth(s.cordon))
	mux.HandleFunc("GET /v1/nodes", s.listNodes)
	mux.HandleFunc("GET /v1/tenants", s.listTenants)
	mux.HandleFunc("PUT /v1/tenants/{name}", auth(s.putTenant))
	mux.HandleFunc("GET /v1/cluster", s.cluster)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return limitBody(mux)
}

func limitBody(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		h.ServeHTTP(w, r)
	})
}

func (s *Server) requireToken(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.opts.APIToken != "" {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if subtle.ConstantTimeCompare([]byte(got), []byte(s.opts.APIToken)) != 1 {
				writeErr(w, http.StatusUnauthorized, errors.New("missing or invalid bearer token"))
				return
			}
		}
		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// ValidateSubmit checks a submission and converts it to a Job.
func (s *Server) ValidateSubmit(req SubmitRequest) (scheduler.Job, error) {
	if req.ID == "" {
		req.ID = newID()
	}
	if !idRe.MatchString(req.ID) {
		return scheduler.Job{}, errors.New("id must match " + idRe.String())
	}
	prio, ok := scheduler.PriorityClasses[req.Priority]
	if !ok {
		return scheduler.Job{}, fmt.Errorf("priority must be one of batch, standard, critical")
	}
	if req.GPUs < 1 || req.GPUs > 64 {
		return scheduler.Job{}, errors.New("gpus must be between 1 and 64")
	}
	if req.RuntimeSeconds <= 0 || req.RuntimeSeconds > 7*24*3600 {
		return scheduler.Job{}, errors.New("runtimeSeconds must be in (0, 604800]")
	}
	tenants := s.sched.Tenants()
	t, ok := tenants[req.Tenant]
	if !ok {
		return scheduler.Job{}, fmt.Errorf("unknown tenant %q", req.Tenant)
	}
	if req.GPUs > t.Max {
		return scheduler.Job{}, fmt.Errorf("job wants %d GPUs but tenant max is %d", req.GPUs, t.Max)
	}
	pre := prio != scheduler.PriorityCritical
	if req.Preemptible != nil {
		pre = *req.Preemptible
	}
	return scheduler.Job{ID: req.ID, Tenant: req.Tenant, Priority: prio, GPUs: req.GPUs, GPUType: req.GPUType,
		Runtime: time.Duration(req.RuntimeSeconds * float64(time.Second)), Preemptible: pre, SubmittedAt: time.Now()}, nil
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	var req SubmitRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	j, err := s.ValidateSubmit(req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.queue.Publish(r.Context(), j); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"id": j.ID, "status": "Queued"})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.sched.Job(id)
	if errors.Is(err, scheduler.ErrUnknownJob) {
		j, err = s.store.Job(r.Context(), id)
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, http.StatusNotFound, fmt.Errorf("job %s not found (it may still be in the intake queue)", id))
			return
		}
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	state := scheduler.JobState(r.URL.Query().Get("state"))
	tenant := r.URL.Query().Get("tenant")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	out := []*scheduler.Job{}
	for _, j := range s.sched.Jobs(state) {
		if tenant != "" && j.Tenant != tenant {
			continue
		}
		out = append(out, j)
		if len(out) == limit {
			break
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	if err := s.sched.Cancel(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	s.Kick()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) completeJob(w http.ResponseWriter, r *http.Request) {
	var req CompleteRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.sched.Complete(r.PathValue("id"), req.Success, req.Message); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	s.Kick()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	var req HeartbeatRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	name := r.PathValue("name")
	if !idRe.MatchString(name) || req.GPUs < 1 || req.GPUs > 64 {
		writeErr(w, http.StatusBadRequest, errors.New("invalid node name or gpu count"))
		return
	}
	if err := s.registerNode(r.Context(), scheduler.Node{Name: name, GPUType: req.GPUType, GPUs: req.GPUs, Pool: req.Pool}); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	as, err := s.sched.Assignments(name)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if as == nil {
		as = []scheduler.Assignment{}
	}
	writeJSON(w, http.StatusOK, HeartbeatResponse{Assignments: as})
}

func (s *Server) cordon(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Cordoned bool `json:"cordoned"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	name := r.PathValue("name")
	if err := s.sched.Cordon(name, req.Cordoned); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	s.mu.Lock()
	n := s.knownNode[name]
	n.Cordoned = req.Cordoned
	s.knownNode[name] = n
	s.mu.Unlock()
	if err := s.store.UpsertNode(r.Context(), n); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listNodes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"nodes": s.sched.Snapshot().Nodes})
}

func (s *Server) listTenants(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"tenants": s.sched.Tenants()})
}

func (s *Server) putTenant(w http.ResponseWriter, r *http.Request) {
	var t scheduler.Tenant
	if err := decode(r, &t); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	t.Name = r.PathValue("name")
	if err := s.sched.UpsertTenant(t); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.store.UpsertTenant(r.Context(), t); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	s.Kick()
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) cluster(w http.ResponseWriter, _ *http.Request) {
	sn := s.sched.Snapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"gpusTotal": sn.GPUsTotal, "gpusAllocated": sn.GPUsUsed, "runningJobs": sn.RunningJobs,
		"pendingByClass": sn.PendingByCls, "oldestPendingSeconds": sn.OldestWait.Seconds(),
		"tenants": sn.Tenants, "nodes": len(sn.Nodes), "policy": s.sched.Config(),
	})
}
