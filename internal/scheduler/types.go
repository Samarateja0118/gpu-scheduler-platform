// Package scheduler implements the GPU job scheduler: placement of
// whole-GPU jobs onto nodes with priorities, tenant quotas (guarantee plus
// borrowing), preemption and dedicated node pools for workload isolation.
//
// GPUs are simulated: a node advertises a GPU count and type, and the
// scheduler hands out GPU indices. Nothing here talks to a real device.
package scheduler

import (
	"encoding/json"
	"time"
)

// JobState is the lifecycle state of a job.
type JobState string

const (
	JobPending   JobState = "Pending"
	JobRunning   JobState = "Running"
	JobSucceeded JobState = "Succeeded"
	JobFailed    JobState = "Failed"
	JobCancelled JobState = "Cancelled"
)

// Terminal reports whether the job will never run again.
func (s JobState) Terminal() bool {
	return s == JobSucceeded || s == JobFailed || s == JobCancelled
}

// Priority classes. Higher value wins.
const (
	PriorityBatch    = 100
	PriorityStandard = 500
	PriorityCritical = 1000
)

// PriorityClasses maps the names accepted by the API to values.
var PriorityClasses = map[string]int{
	"batch":    PriorityBatch,
	"standard": PriorityStandard,
	"critical": PriorityCritical,
}

// PriorityClassName returns the class name for a priority value.
func PriorityClassName(p int) string {
	switch {
	case p >= PriorityCritical:
		return "critical"
	case p >= PriorityStandard:
		return "standard"
	default:
		return "batch"
	}
}

// Job is a request for N whole GPUs on a single node for Runtime.
type Job struct {
	ID          string        `json:"id"`
	Tenant      string        `json:"tenant"`
	Priority    int           `json:"priority"`
	GPUs        int           `json:"gpus"`
	GPUType     string        `json:"gpuType,omitempty"`
	Runtime     time.Duration `json:"-"`
	Preemptible bool          `json:"preemptible"`

	State       JobState `json:"state"`
	Node        string   `json:"node,omitempty"`
	GPUIndices  []int    `json:"gpuIndices,omitempty"`
	Borrowed    bool     `json:"borrowed"`
	Preemptions int      `json:"preemptions"`
	Requeues    int      `json:"requeues"`
	Message     string   `json:"message,omitempty"`

	SubmittedAt time.Time     `json:"submittedAt"`
	QueuedAt    time.Time     `json:"queuedAt"`
	StartedAt   time.Time     `json:"startedAt,omitempty"`
	FinishedAt  time.Time     `json:"finishedAt,omitempty"`
	Wait        time.Duration `json:"-"` // cumulative time spent Pending
}

// MarshalJSON renders durations as seconds.
func (j Job) MarshalJSON() ([]byte, error) {
	type alias Job
	return json.Marshal(struct {
		alias
		RuntimeSeconds float64 `json:"runtimeSeconds"`
		WaitSeconds    float64 `json:"waitSeconds"`
	}{alias(j), j.Runtime.Seconds(), j.Wait.Seconds()})
}

// UnmarshalJSON is the inverse of MarshalJSON.
func (j *Job) UnmarshalJSON(b []byte) error {
	type alias Job
	aux := struct {
		*alias
		RuntimeSeconds float64 `json:"runtimeSeconds"`
		WaitSeconds    float64 `json:"waitSeconds"`
	}{alias: (*alias)(j)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	j.Runtime = time.Duration(aux.RuntimeSeconds * float64(time.Second))
	j.Wait = time.Duration(aux.WaitSeconds * float64(time.Second))
	return nil
}

func (j *Job) clone() *Job {
	c := *j
	c.GPUIndices = append([]int(nil), j.GPUIndices...)
	return &c
}

// Node is a (simulated) GPU server.
type Node struct {
	Name          string    `json:"name"`
	GPUType       string    `json:"gpuType"`
	GPUs          int       `json:"gpus"`
	Pool          string    `json:"pool,omitempty"`
	Ready         bool      `json:"ready"`
	Cordoned      bool      `json:"cordoned"`
	LastHeartbeat time.Time `json:"lastHeartbeat"`
	// Alloc[i] is the job holding GPU i, or "" if free.
	Alloc []string `json:"alloc"`
}

// FreeGPUs returns the number of unallocated GPUs.
func (n *Node) FreeGPUs() int {
	f := 0
	for _, a := range n.Alloc {
		if a == "" {
			f++
		}
	}
	return f
}

func (n *Node) schedulable() bool { return n.Ready && !n.Cordoned }

func (n *Node) clone() *Node {
	c := *n
	c.Alloc = append([]string(nil), n.Alloc...)
	return &c
}

// Tenant carries the quota and isolation settings for a team.
//
// Guaranteed GPUs are never taken away by another tenant. A tenant may
// borrow idle capacity up to Max; borrowed jobs are the first to be
// preempted when a tenant still under its guarantee needs GPUs back.
// A tenant with a Pool only runs on nodes in that pool, and pool nodes
// only run tenants of that pool (dedicated hardware isolation).
type Tenant struct {
	Name       string `json:"name"`
	Guaranteed int    `json:"guaranteed"`
	Max        int    `json:"max"`
	Pool       string `json:"pool,omitempty"`
}

// Ordering of the pending queue.
type Ordering string

const (
	OrderFIFO     Ordering = "fifo"
	OrderPriority Ordering = "priority"
)

// Placement chooses among nodes that fit a job.
type Placement string

const (
	PlaceFirstFit Placement = "firstfit"
	PlaceBestFit  Placement = "bestfit" // tightest node: fewest free GPUs left
	PlaceSpread   Placement = "spread"  // emptiest node
)

// Config selects scheduler policies.
type Config struct {
	Ordering     Ordering
	Placement    Placement
	Preemption   bool
	EnforceQuota bool
	// Aging: every AgingInterval spent pending adds AgingBoost to the
	// job's ordering priority, up to AgingCap. Aging changes queue order
	// only; preemption decisions use the base priority.
	AgingInterval time.Duration
	AgingBoost    int
	AgingCap      int
	// IslandSize groups GPUs on a node into interconnect islands (e.g. 4
	// GPUs sharing NVLink). Jobs that fit in one island are kept inside one.
	IslandSize int
}

// DefaultConfig is the full policy set.
func DefaultConfig() Config {
	return Config{
		Ordering:      OrderPriority,
		Placement:     PlaceBestFit,
		Preemption:    true,
		EnforceQuota:  true,
		AgingInterval: 10 * time.Minute,
		AgingBoost:    25,
		AgingCap:      300,
		IslandSize:    4,
	}
}

// EventType names a state change.
type EventType string

const (
	EvSubmitted EventType = "submitted"
	EvBound     EventType = "bound"
	EvPreempted EventType = "preempted"
	EvCompleted EventType = "completed"
	EvFailed    EventType = "failed"
	EvCancelled EventType = "cancelled"
	EvRequeued  EventType = "requeued" // node failure
	EvNodeDown  EventType = "node_down"
	EvNodeUp    EventType = "node_up"
)

// Event is emitted for every state change. Job is a copy.
type Event struct {
	Type EventType
	Time time.Time
	Job  *Job
	Node string
	// By is the job that caused a preemption.
	By string
	// Lost is the work thrown away by a preemption or requeue (GPU-seconds
	// are Lost * GPUs). Jobs restart from scratch: no checkpointing.
	Lost time.Duration
}
