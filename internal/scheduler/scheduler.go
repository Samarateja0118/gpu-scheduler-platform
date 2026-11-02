package scheduler

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrUnknownTenant = errors.New("unknown tenant")
	ErrUnknownJob    = errors.New("unknown job")
	ErrInvalidJob    = errors.New("invalid job")
	ErrDuplicateJob  = errors.New("duplicate job id")
	ErrUnknownNode   = errors.New("unknown node")
)

// Scheduler holds cluster state. All methods are safe for concurrent use.
// Listeners run synchronously while the lock is held and must not call
// back into the Scheduler.
type Scheduler struct {
	mu        sync.Mutex
	cfg       Config
	now       func() time.Time
	nodes     map[string]*Node
	nodeNames []string // sorted, for deterministic placement
	tenants   map[string]*Tenant
	jobs      map[string]*Job
	pending   map[string]*Job
	running   map[string]*Job
	usage     map[string]int // GPUs held by running jobs, per tenant
	listeners []func(Event)
}

// New creates a scheduler. now may be nil for wall-clock time.
func New(cfg Config, now func() time.Time) *Scheduler {
	if now == nil {
		now = time.Now
	}
	return &Scheduler{
		cfg:     cfg,
		now:     now,
		nodes:   map[string]*Node{},
		tenants: map[string]*Tenant{},
		jobs:    map[string]*Job{},
		pending: map[string]*Job{},
		running: map[string]*Job{},
		usage:   map[string]int{},
	}
}

// OnEvent registers a listener.
func (s *Scheduler) OnEvent(f func(Event)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners = append(s.listeners, f)
}

func (s *Scheduler) emit(e Event) {
	if e.Job != nil {
		e.Job = e.Job.clone()
	}
	for _, l := range s.listeners {
		l(e)
	}
}

// Config returns the active configuration.
func (s *Scheduler) Config() Config { return s.cfg }

// ---------------------------------------------------------------- tenants

// UpsertTenant creates or updates a tenant.
func (s *Scheduler) UpsertTenant(t Tenant) error {
	if t.Name == "" || t.Guaranteed < 0 || t.Max < t.Guaranteed {
		return fmt.Errorf("%w: tenant needs a name and 0 <= guaranteed <= max", ErrInvalidJob)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tc := t
	s.tenants[t.Name] = &tc
	s.recomputeBorrowed(t.Name)
	return nil
}

// Tenants returns tenants with their current usage.
func (s *Scheduler) Tenants() map[string]TenantUsage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]TenantUsage{}
	for n, t := range s.tenants {
		out[n] = TenantUsage{Tenant: *t, InUse: s.usage[n]}
	}
	return out
}

// TenantUsage is a tenant plus the GPUs it holds.
type TenantUsage struct {
	Tenant
	InUse int `json:"inUse"`
}

// ------------------------------------------------------------------ nodes

// RegisterNode adds a node or refreshes an existing one's heartbeat.
// Capacity of an existing node is not changed while it holds jobs.
func (s *Scheduler) RegisterNode(name, gpuType string, gpus int, pool string) error {
	if name == "" || gpus <= 0 {
		return fmt.Errorf("%w: node needs a name and gpus > 0", ErrInvalidJob)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	n, ok := s.nodes[name]
	if !ok {
		n = &Node{Name: name, GPUType: gpuType, GPUs: gpus, Pool: pool, Alloc: make([]string, gpus)}
		s.nodes[name] = n
		s.nodeNames = append(s.nodeNames, name)
		sort.Strings(s.nodeNames)
	} else if n.FreeGPUs() == n.GPUs {
		n.GPUType, n.Pool = gpuType, pool
		if n.GPUs != gpus {
			n.GPUs, n.Alloc = gpus, make([]string, gpus)
		}
	}
	n.LastHeartbeat = now
	if !n.Ready {
		n.Ready = true
		s.emit(Event{Type: EvNodeUp, Time: now, Node: name})
	}
	return nil
}

// Heartbeat refreshes a known node.
func (s *Scheduler) Heartbeat(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[name]
	if !ok {
		return ErrUnknownNode
	}
	n.LastHeartbeat = s.now()
	if !n.Ready {
		n.Ready = true
		s.emit(Event{Type: EvNodeUp, Time: n.LastHeartbeat, Node: name})
	}
	return nil
}

// Cordon stops new placements on a node; running jobs stay.
func (s *Scheduler) Cordon(name string, cordoned bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[name]
	if !ok {
		return ErrUnknownNode
	}
	n.Cordoned = cordoned
	return nil
}

// ExpireNodes marks nodes without a heartbeat for longer than timeout as
// not ready and requeues their jobs. It returns the expired node names.
func (s *Scheduler) ExpireNodes(timeout time.Duration) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var down []string
	for _, name := range s.nodeNames {
		n := s.nodes[name]
		if !n.Ready || now.Sub(n.LastHeartbeat) <= timeout {
			continue
		}
		n.Ready = false
		down = append(down, name)
		s.emit(Event{Type: EvNodeDown, Time: now, Node: name})
		for _, id := range uniqueJobs(n.Alloc) {
			j := s.jobs[id]
			lost := now.Sub(j.StartedAt)
			s.release(j)
			j.Requeues++
			j.State, j.QueuedAt, j.Node, j.GPUIndices = JobPending, now, "", nil
			j.Message = "node " + name + " lost heartbeat; requeued"
			s.pending[id] = j
			s.recomputeBorrowed(j.Tenant)
			s.emit(Event{Type: EvRequeued, Time: now, Job: j, Node: name, Lost: lost})
		}
	}
	return down
}

func uniqueJobs(alloc []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, a := range alloc {
		if a != "" && !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out
}

// Assignment tells a node agent what should be running on it.
type Assignment struct {
	JobID      string    `json:"jobId"`
	GPUIndices []int     `json:"gpuIndices"`
	StartedAt  time.Time `json:"startedAt"`
	RuntimeSec float64   `json:"runtimeSeconds"`
}

// Assignments returns the jobs currently bound to a node.
func (s *Scheduler) Assignments(node string) ([]Assignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[node]
	if !ok {
		return nil, ErrUnknownNode
	}
	var out []Assignment
	for _, id := range uniqueJobs(n.Alloc) {
		j := s.jobs[id]
		out = append(out, Assignment{JobID: id, GPUIndices: append([]int(nil), j.GPUIndices...), StartedAt: j.StartedAt, RuntimeSec: j.Runtime.Seconds()})
	}
	return out, nil
}

// ------------------------------------------------------------------- jobs

// Submit queues a new job. The job's ID must be unique; resubmitting an
// identical ID returns ErrDuplicateJob so intake can be idempotent.
func (s *Scheduler) Submit(j Job) error {
	if j.ID == "" || j.GPUs <= 0 || j.Runtime <= 0 {
		return fmt.Errorf("%w: id, gpus > 0 and runtime > 0 are required", ErrInvalidJob)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tenants[j.Tenant]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownTenant, j.Tenant)
	}
	if s.cfg.EnforceQuota && j.GPUs > t.Max {
		return fmt.Errorf("%w: job wants %d GPUs, tenant max is %d", ErrInvalidJob, j.GPUs, t.Max)
	}
	if _, dup := s.jobs[j.ID]; dup {
		return ErrDuplicateJob
	}
	now := s.now()
	jc := j.clone()
	jc.State = JobPending
	if jc.SubmittedAt.IsZero() {
		jc.SubmittedAt = now
	}
	jc.QueuedAt = now
	s.jobs[jc.ID] = jc
	s.pending[jc.ID] = jc
	s.emit(Event{Type: EvSubmitted, Time: now, Job: jc})
	return nil
}

// Complete records that a running job finished.
func (s *Scheduler) Complete(id string, success bool, msg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return ErrUnknownJob
	}
	if j.State != JobRunning {
		return nil // late or duplicate report; idempotent
	}
	now := s.now()
	s.release(j)
	j.FinishedAt, j.Message = now, msg
	ev := EvCompleted
	j.State = JobSucceeded
	if !success {
		j.State, ev = JobFailed, EvFailed
	}
	s.recomputeBorrowed(j.Tenant)
	s.emit(Event{Type: ev, Time: now, Job: j, Node: j.Node})
	return nil
}

// Cancel stops a pending or running job.
func (s *Scheduler) Cancel(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return ErrUnknownJob
	}
	if j.State.Terminal() {
		return nil
	}
	now := s.now()
	if j.State == JobRunning {
		s.release(j)
		s.recomputeBorrowed(j.Tenant)
	} else {
		j.Wait += now.Sub(j.QueuedAt)
		delete(s.pending, id)
	}
	j.State, j.FinishedAt = JobCancelled, now
	s.emit(Event{Type: EvCancelled, Time: now, Job: j})
	return nil
}

// Job returns a copy of a job.
func (s *Scheduler) Job(id string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrUnknownJob
	}
	return j.clone(), nil
}

// Jobs returns copies of all jobs in the given state ("" for all).
func (s *Scheduler) Jobs(state JobState) []*Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Job
	for _, j := range s.jobs {
		if state == "" || j.State == state {
			out = append(out, j.clone())
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].SubmittedAt.Before(out[b].SubmittedAt) })
	return out
}

// Forget drops terminal jobs older than age from memory (they stay in the
// durable store). Keeps the in-memory map bounded on long runs.
func (s *Scheduler) Forget(age time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	cut := s.now().Add(-age)
	n := 0
	for id, j := range s.jobs {
		if j.State.Terminal() && j.FinishedAt.Before(cut) {
			delete(s.jobs, id)
			n++
		}
	}
	return n
}

// Restore loads a job from durable storage after a restart. Running jobs
// are re-bound to their recorded node and GPUs; if that is impossible the
// job is requeued.
func (s *Scheduler) Restore(j Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	jc := j.clone()
	s.jobs[jc.ID] = jc
	switch jc.State {
	case JobPending:
		s.pending[jc.ID] = jc
	case JobRunning:
		n, ok := s.nodes[jc.Node]
		okIdx := ok && len(jc.GPUIndices) == jc.GPUs
		for _, i := range jc.GPUIndices {
			if !ok || i < 0 || i >= len(n.Alloc) || n.Alloc[i] != "" {
				okIdx = false
			}
		}
		if !okIdx {
			jc.State, jc.Node, jc.GPUIndices, jc.QueuedAt = JobPending, "", nil, s.now()
			jc.Requeues++
			s.pending[jc.ID] = jc
			return
		}
		for _, i := range jc.GPUIndices {
			n.Alloc[i] = jc.ID
		}
		s.usage[jc.Tenant] += jc.GPUs
		s.running[jc.ID] = jc
		s.recomputeBorrowed(jc.Tenant)
	}
}

// RestoreNode loads a node from durable storage. It starts Ready with a
// fresh heartbeat so agents get a grace period to reconnect.
func (s *Scheduler) RestoreNode(n Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[n.Name]; ok {
		return
	}
	nc := &Node{Name: n.Name, GPUType: n.GPUType, GPUs: n.GPUs, Pool: n.Pool, Cordoned: n.Cordoned, Ready: true, LastHeartbeat: s.now(), Alloc: make([]string, n.GPUs)}
	s.nodes[n.Name] = nc
	s.nodeNames = append(s.nodeNames, n.Name)
	sort.Strings(s.nodeNames)
}

func (s *Scheduler) release(j *Job) {
	if n, ok := s.nodes[j.Node]; ok {
		for _, i := range j.GPUIndices {
			if i < len(n.Alloc) && n.Alloc[i] == j.ID {
				n.Alloc[i] = ""
			}
		}
	}
	s.usage[j.Tenant] -= j.GPUs
	delete(s.running, j.ID)
}

// recomputeBorrowed marks a tenant's running jobs as borrowed or not. The
// oldest running jobs fill the guarantee first; anything beyond it is
// borrowed and may be reclaimed by other tenants.
func (s *Scheduler) recomputeBorrowed(tenant string) {
	t, ok := s.tenants[tenant]
	if !ok {
		return
	}
	var running []*Job
	for _, j := range s.running {
		if j.Tenant == tenant {
			running = append(running, j)
		}
	}
	sort.Slice(running, func(a, b int) bool {
		if !running[a].StartedAt.Equal(running[b].StartedAt) {
			return running[a].StartedAt.Before(running[b].StartedAt)
		}
		return running[a].ID < running[b].ID
	})
	used := 0
	for _, j := range running {
		used += j.GPUs
		j.Borrowed = s.cfg.EnforceQuota && used > t.Guaranteed
	}
}
