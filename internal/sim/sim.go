// Package sim replays a synthetic GPU workload through the scheduler on a
// virtual clock, so scheduling policies can be compared on identical job
// traces. Nothing runs on real hardware: a job "runs" by its completion
// event being scheduled Runtime after it is bound.
package sim

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/scheduler"
)

// JobSpec is one job in a trace.
type JobSpec struct {
	ID          string
	Arrive      time.Duration
	Tenant      string
	Priority    int
	GPUs        int
	GPUType     string
	Runtime     time.Duration
	Preemptible bool
}

// NodeSpec describes a simulated node.
type NodeSpec struct {
	Name    string
	GPUType string
	GPUs    int
	Pool    string
}

// Scenario is a cluster, its tenants and a trace.
type Scenario struct {
	Nodes   []NodeSpec
	Tenants []scheduler.Tenant
	Jobs    []JobSpec
	// Window is the arrival window; utilization is measured over it so the
	// drain at the end does not dilute the number.
	Window time.Duration
}

// Capacity returns the total GPU count.
func (sc Scenario) Capacity() int {
	c := 0
	for _, n := range sc.Nodes {
		c += n.GPUs
	}
	return c
}

// OfferedLoad is requested GPU-time over capacity in the window.
func (sc Scenario) OfferedLoad() float64 {
	var gs float64
	for _, j := range sc.Jobs {
		gs += float64(j.GPUs) * j.Runtime.Hours()
	}
	return gs / (float64(sc.Capacity()) * sc.Window.Hours())
}

type tenantMix struct {
	name     string
	share    float64 // share of offered load
	prios    []weighted[int]
	sizes    []weighted[int]
	medianRT time.Duration
	h100     float64 // probability the job requires h100
	diurnal  bool
}

type weighted[T any] struct {
	v T
	w float64
}

func pick[T any](r *rand.Rand, ws []weighted[T]) T {
	tot := 0.0
	for _, w := range ws {
		tot += w.w
	}
	x := r.Float64() * tot
	for _, w := range ws {
		if x < w.w {
			return w.v
		}
		x -= w.w
	}
	return ws[len(ws)-1].v
}

// DefaultScenario builds a 16-node, 128-GPU cluster shared by three
// tenants (online inference, research training, batch) and a trace of
// the given length whose offered load is roughly `load` of capacity.
func DefaultScenario(seed int64, window time.Duration, load float64) Scenario {
	sc := Scenario{Window: window}
	for i := 0; i < 16; i++ {
		typ := "a100"
		if i >= 12 {
			typ = "h100"
		}
		sc.Nodes = append(sc.Nodes, NodeSpec{Name: fmt.Sprintf("gpu-node-%02d", i), GPUType: typ, GPUs: 8})
	}
	sc.Tenants = []scheduler.Tenant{
		{Name: "inference", Guaranteed: 48, Max: 64},
		{Name: "research", Guaranteed: 48, Max: 112},
		{Name: "batch", Guaranteed: 32, Max: 128},
	}
	mixes := []tenantMix{
		{name: "inference", share: 0.35, diurnal: true, medianRT: 20 * time.Minute,
			prios: []weighted[int]{{scheduler.PriorityCritical, 0.7}, {scheduler.PriorityStandard, 0.3}},
			sizes: []weighted[int]{{1, 0.5}, {2, 0.3}, {4, 0.2}}},
		{name: "research", share: 0.40, medianRT: 45 * time.Minute, h100: 0.15,
			prios: []weighted[int]{{scheduler.PriorityStandard, 0.6}, {scheduler.PriorityBatch, 0.4}},
			sizes: []weighted[int]{{1, 0.3}, {2, 0.2}, {4, 0.25}, {8, 0.25}}},
		{name: "batch", share: 0.25, medianRT: 30 * time.Minute,
			prios: []weighted[int]{{scheduler.PriorityBatch, 1}},
			sizes: []weighted[int]{{1, 0.5}, {2, 0.25}, {4, 0.15}, {8, 0.1}}},
	}
	r := rand.New(rand.NewSource(seed))
	capGPUh := float64(sc.Capacity()) * window.Hours()
	const sigma = 0.9
	n := 0
	for _, m := range mixes {
		target := capGPUh * load * m.share
		var got float64
		// Arrivals: Poisson with a mean rate set so expected GPU-hours hit
		// the target; diurnal tenants are thinned by a sine over 24h.
		meanSize := 0.0
		tw := 0.0
		for _, s := range m.sizes {
			meanSize += float64(s.v) * s.w
			tw += s.w
		}
		meanSize /= tw
		meanRT := m.medianRT.Hours() * math.Exp(sigma*sigma/2)
		rate := target / (meanSize * meanRT) / window.Hours() // jobs per hour
		peak := rate
		if m.diurnal {
			peak = rate * 1.6
		}
		t := 0.0
		for {
			t += r.ExpFloat64() / peak
			if t >= window.Hours() {
				break
			}
			if m.diurnal {
				lambda := rate * (1 + 0.6*math.Sin(2*math.Pi*t/24))
				if r.Float64() > lambda/peak {
					continue
				}
			}
			rt := time.Duration(float64(m.medianRT) * math.Exp(sigma*r.NormFloat64()))
			if rt < time.Minute {
				rt = time.Minute
			}
			if rt > 8*time.Hour {
				rt = 8 * time.Hour
			}
			prio := pick(r, m.prios)
			js := JobSpec{
				ID: fmt.Sprintf("%s-%05d", m.name, n), Arrive: time.Duration(t * float64(time.Hour)),
				Tenant: m.name, Priority: prio, GPUs: pick(r, m.sizes), Runtime: rt,
				Preemptible: prio != scheduler.PriorityCritical,
			}
			if r.Float64() < m.h100 {
				js.GPUType = "h100"
			}
			got += float64(js.GPUs) * rt.Hours()
			sc.Jobs = append(sc.Jobs, js)
			n++
		}
	}
	sort.Slice(sc.Jobs, func(a, b int) bool { return sc.Jobs[a].Arrive < sc.Jobs[b].Arrive })
	return sc
}

// ------------------------------------------------------------------ engine

type evKind int

const (
	evArrive evKind = iota
	evFinish
)

type simEvent struct {
	at    time.Duration
	kind  evKind
	idx   int    // arrival: index into Jobs
	id    string // finish: job id
	epoch int    // finish: which start of the job this completion belongs to
	seq   int
}

type evHeap []simEvent

func (h evHeap) Len() int { return len(h) }
func (h evHeap) Less(a, b int) bool {
	if h[a].at != h[b].at {
		return h[a].at < h[b].at
	}
	return h[a].seq < h[b].seq
}
func (h evHeap) Swap(a, b int) { h[a], h[b] = h[b], h[a] }
func (h *evHeap) Push(x any)   { *h = append(*h, x.(simEvent)) }
func (h *evHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

// ClassStats are wait-time percentiles for one priority class.
type ClassStats struct {
	Jobs     int     `json:"jobs"`
	P50Min   float64 `json:"p50WaitMin"`
	P95Min   float64 `json:"p95WaitMin"`
	P99Min   float64 `json:"p99WaitMin"`
	MeanMin  float64 `json:"meanWaitMin"`
	Preempts int     `json:"preemptions"`
}

// Result is the outcome of one simulation.
type Result struct {
	Policy             string                `json:"policy"`
	Seed               int64                 `json:"seed"`
	Jobs               int                   `json:"jobs"`
	OfferedLoad        float64               `json:"offeredLoad"`
	Utilization        float64               `json:"utilization"`
	UsefulUtilization  float64               `json:"usefulUtilization"`
	IdleWhileQueued    float64               `json:"idleWhileQueuedFrac"`
	Preemptions        int                   `json:"preemptions"`
	LostGPUHours       float64               `json:"lostGpuHours"`
	LostFrac           float64               `json:"lostFracOfDelivered"`
	ByClass            map[string]ClassStats `json:"byClass"`
	BySize             map[int]ClassStats    `json:"bySize"`
	Makespan           time.Duration         `json:"-"`
	Cycles             int                   `json:"cycles"`
	CycleP99Micros     float64               `json:"cycleP99Micros"`
	GuaranteeShortfall float64               `json:"guaranteeShortfallGpuH"`
}

// Run replays the scenario under cfg.
func Run(sc Scenario, cfg scheduler.Config, policy string, seed int64) (Result, error) {
	var now time.Duration
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := scheduler.New(cfg, func() time.Time { return base.Add(now) })
	for _, t := range sc.Tenants {
		if err := s.UpsertTenant(t); err != nil {
			return Result{}, err
		}
	}
	for _, n := range sc.Nodes {
		if err := s.RegisterNode(n.Name, n.GPUType, n.GPUs, n.Pool); err != nil {
			return Result{}, err
		}
	}
	h := &evHeap{}
	seq := 0
	push := func(e simEvent) { e.seq = seq; seq++; heap.Push(h, e) }
	for i, j := range sc.Jobs {
		push(simEvent{at: j.Arrive, kind: evArrive, idx: i})
	}
	epoch := map[string]int{}
	runtime := map[string]time.Duration{}
	firstWait := map[string]time.Duration{}
	var lostGPUh, lostInWindow float64
	preByClass := map[string]int{}
	preBySize := map[int]int{}
	preempts := 0
	s.OnEvent(func(e scheduler.Event) {
		switch e.Type {
		case scheduler.EvBound:
			epoch[e.Job.ID]++
			push(simEvent{at: now + runtime[e.Job.ID], kind: evFinish, id: e.Job.ID, epoch: epoch[e.Job.ID]})
			if _, ok := firstWait[e.Job.ID]; !ok {
				firstWait[e.Job.ID] = e.Job.StartedAt.Sub(e.Job.SubmittedAt)
			}
		case scheduler.EvPreempted:
			if now <= sc.Window {
				lostInWindow += e.Lost.Hours() * float64(e.Job.GPUs)
			}
			preempts++
			preByClass[scheduler.PriorityClassName(e.Job.Priority)]++
			preBySize[e.Job.GPUs]++
			lostGPUh += e.Lost.Hours() * float64(e.Job.GPUs)
		}
	})

	capacity := float64(sc.Capacity())
	var usedGPUh, idleQueuedGPUh, shortfall float64
	var cycleTimes []time.Duration
	last := time.Duration(0)
	pendingDemand := map[string]int{}
	accumulate := func(to time.Duration) {
		if to <= last {
			return
		}
		end := to
		if end > sc.Window {
			end = sc.Window
		}
		if end > last {
			dt := (end - last).Hours()
			sn := s.Snapshot()
			usedGPUh += float64(sn.GPUsUsed) * dt
			queued := sn.PendingByCls["batch"] + sn.PendingByCls["standard"] + sn.PendingByCls["critical"]
			if queued > 0 {
				idleQueuedGPUh += float64(sn.GPUsTotal-sn.GPUsUsed) * dt
			}
			// Guarantee shortfall: GPU-hours a tenant wanted, within its
			// guarantee, but did not get.
			for name, tu := range sn.Tenants {
				want := tu.InUse + pendingDemand[name]
				if want > tu.Guaranteed {
					want = tu.Guaranteed
				}
				if want > tu.InUse {
					shortfall += float64(want-tu.InUse) * dt
				}
			}
		}
		last = to
	}
	for h.Len() > 0 {
		at := (*h)[0].at
		accumulate(at)
		now = at
		for h.Len() > 0 && (*h)[0].at == at {
			e := heap.Pop(h).(simEvent)
			switch e.kind {
			case evArrive:
				js := sc.Jobs[e.idx]
				runtime[js.ID] = js.Runtime
				if err := s.Submit(scheduler.Job{ID: js.ID, Tenant: js.Tenant, Priority: js.Priority, GPUs: js.GPUs, GPUType: js.GPUType, Runtime: js.Runtime, Preemptible: js.Preemptible}); err != nil {
					return Result{}, err
				}
			case evFinish:
				if epoch[e.id] == e.epoch {
					if err := s.Complete(e.id, true, ""); err != nil {
						return Result{}, err
					}
				}
			}
		}
		st := s.Schedule()
		cycleTimes = append(cycleTimes, st.Duration)
		pendingDemand = map[string]int{}
		for _, j := range s.Jobs(scheduler.JobPending) {
			pendingDemand[j.Tenant] += j.GPUs
		}
	}

	res := Result{Policy: policy, Seed: seed, Jobs: len(sc.Jobs), OfferedLoad: sc.OfferedLoad(), Makespan: now,
		Preemptions: preempts, LostGPUHours: lostGPUh, ByClass: map[string]ClassStats{}, BySize: map[int]ClassStats{},
		Cycles: len(cycleTimes), GuaranteeShortfall: shortfall}
	res.Utilization = usedGPUh / (capacity * sc.Window.Hours())
	res.IdleWhileQueued = idleQueuedGPUh / (capacity * sc.Window.Hours())
	// Allocated time includes work later thrown away by preemption; useful
	// utilization subtracts it (approximation: all lost work is charged to
	// the window).
	res.UsefulUtilization = (usedGPUh - math.Min(usedGPUh, lostInWindow)) / (capacity * sc.Window.Hours())
	var delivered float64
	byClass := map[string][]float64{}
	bySize := map[int][]float64{}
	for _, js := range sc.Jobs {
		w, ok := firstWait[js.ID]
		if !ok {
			return Result{}, fmt.Errorf("job %s never started", js.ID)
		}
		delivered += float64(js.GPUs) * js.Runtime.Hours()
		c := scheduler.PriorityClassName(js.Priority)
		byClass[c] = append(byClass[c], w.Minutes())
		bySize[js.GPUs] = append(bySize[js.GPUs], w.Minutes())
	}
	res.LostFrac = lostGPUh / delivered
	for c, ws := range byClass {
		cs := summarize(ws)
		cs.Preempts = preByClass[c]
		res.ByClass[c] = cs
	}
	for g, ws := range bySize {
		cs := summarize(ws)
		cs.Preempts = preBySize[g]
		res.BySize[g] = cs
	}
	sort.Slice(cycleTimes, func(a, b int) bool { return cycleTimes[a] < cycleTimes[b] })
	if len(cycleTimes) > 0 {
		res.CycleP99Micros = float64(cycleTimes[len(cycleTimes)*99/100].Microseconds())
	}
	return res, nil
}

func summarize(xs []float64) ClassStats {
	sort.Float64s(xs)
	q := func(p float64) float64 {
		if len(xs) == 0 {
			return 0
		}
		return xs[int(math.Min(float64(len(xs)-1), p*float64(len(xs))))]
	}
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return ClassStats{Jobs: len(xs), P50Min: q(0.50), P95Min: q(0.95), P99Min: q(0.99), MeanMin: sum / math.Max(1, float64(len(xs)))}
}

// Policies are the configurations compared by the benchmark.
func Policies() []struct {
	Name string
	Cfg  scheduler.Config
} {
	full := scheduler.DefaultConfig()
	fifo := scheduler.Config{Ordering: scheduler.OrderFIFO, Placement: scheduler.PlaceFirstFit}
	prioNoPre := full
	prioNoPre.Preemption = false
	spread := full
	spread.Placement = scheduler.PlaceSpread
	return []struct {
		Name string
		Cfg  scheduler.Config
	}{
		{"fifo-firstfit", fifo},
		{"priority-bestfit-nopreempt", prioNoPre},
		{"priority-bestfit-preempt", full},
		{"priority-spread-preempt", spread},
	}
}
