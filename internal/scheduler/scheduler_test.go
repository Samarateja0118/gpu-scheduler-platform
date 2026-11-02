package scheduler

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTest(t *testing.T, cfg Config) (*Scheduler, *fakeClock, *[]Event) {
	t.Helper()
	c := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s := New(cfg, c.now)
	var evs []Event
	s.OnEvent(func(e Event) { evs = append(evs, e) })
	return s, c, &evs
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func job(id, tenant string, prio, gpus int, preemptible bool) Job {
	return Job{ID: id, Tenant: tenant, Priority: prio, GPUs: gpus, Runtime: time.Hour, Preemptible: preemptible}
}

func state(t *testing.T, s *Scheduler, id string) *Job {
	t.Helper()
	j, err := s.Job(id)
	must(t, err)
	return j
}

func TestBestFitPacksTightestNode(t *testing.T) {
	s, _, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 16, Max: 16}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	must(t, s.RegisterNode("n2", "a100", 8, ""))
	must(t, s.Submit(job("big", "a", 500, 6, true)))
	s.Schedule()
	must(t, s.Submit(job("small", "a", 500, 2, true)))
	s.Schedule()
	big, small := state(t, s, "big"), state(t, s, "small")
	if small.Node != big.Node {
		t.Fatalf("best fit should pack small next to big: big=%s small=%s", big.Node, small.Node)
	}
}

func TestSpreadUsesEmptiestNode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Placement = PlaceSpread
	s, _, _ := newTest(t, cfg)
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 16, Max: 16}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	must(t, s.RegisterNode("n2", "a100", 8, ""))
	must(t, s.Submit(job("j1", "a", 500, 2, true)))
	s.Schedule()
	must(t, s.Submit(job("j2", "a", 500, 2, true)))
	s.Schedule()
	if state(t, s, "j1").Node == state(t, s, "j2").Node {
		t.Fatal("spread should use different nodes")
	}
}

func TestIslandAwareGPUSelection(t *testing.T) {
	s, _, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 8, Max: 8}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	for i, g := range []int{1, 2, 4} {
		must(t, s.Submit(job(fmt.Sprintf("j%d", i), "a", 500, g, true)))
		s.Schedule()
	}
	// j0 (1) and j1 (2) share island 0..3; j2 (4) gets island 4..7 whole.
	j2 := state(t, s, "j2")
	for _, i := range j2.GPUIndices {
		if i < 4 {
			t.Fatalf("4-GPU job split across islands: %v", j2.GPUIndices)
		}
	}
}

func TestPriorityOrdering(t *testing.T) {
	s, _, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 8, Max: 8}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	must(t, s.Submit(job("low", "a", PriorityBatch, 8, false)))
	must(t, s.Submit(job("high", "a", PriorityCritical, 8, false)))
	s.Schedule()
	if state(t, s, "high").State != JobRunning || state(t, s, "low").State != JobPending {
		t.Fatal("critical job should run first")
	}
}

func TestFIFOOrdering(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Ordering = OrderFIFO
	s, c, _ := newTest(t, cfg)
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 8, Max: 8}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	must(t, s.Submit(job("low", "a", PriorityBatch, 8, false)))
	c.advance(time.Second)
	must(t, s.Submit(job("high", "a", PriorityCritical, 8, false)))
	s.Schedule()
	if state(t, s, "low").State != JobRunning {
		t.Fatal("fifo should run the older job")
	}
}

func TestPreemptsLowerPriorityWithinTenant(t *testing.T) {
	s, _, evs := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 8, Max: 8}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	must(t, s.Submit(job("low1", "a", PriorityBatch, 4, true)))
	must(t, s.Submit(job("low2", "a", PriorityBatch, 4, true)))
	s.Schedule()
	must(t, s.Submit(job("high", "a", PriorityCritical, 4, false)))
	st := s.Schedule()
	if st.Preempted != 1 || state(t, s, "high").State != JobRunning {
		t.Fatalf("expected one preemption and high running, got %+v", st)
	}
	var pre []Event
	for _, e := range *evs {
		if e.Type == EvPreempted {
			pre = append(pre, e)
		}
	}
	if len(pre) != 1 || pre[0].By != "high" || pre[0].Job.Preemptions != 1 {
		t.Fatalf("bad preemption events: %+v", pre)
	}
}

func TestNeverPreemptsNonPreemptibleOrEqualPriority(t *testing.T) {
	s, _, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 8, Max: 8}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	must(t, s.Submit(job("pinned", "a", PriorityBatch, 4, false)))
	must(t, s.Submit(job("peer", "a", PriorityCritical, 4, true)))
	s.Schedule()
	must(t, s.Submit(job("high", "a", PriorityCritical, 4, false)))
	if st := s.Schedule(); st.Preempted != 0 {
		t.Fatal("must not preempt non-preemptible or equal priority work")
	}
}

func TestGuaranteeReclaimsBorrowedCapacity(t *testing.T) {
	s, c, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 8, Max: 16}))
	must(t, s.UpsertTenant(Tenant{Name: "b", Guaranteed: 8, Max: 16}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	must(t, s.RegisterNode("n2", "a100", 8, ""))
	// b fills the cluster: 8 within guarantee, 8 borrowed.
	must(t, s.Submit(job("b1", "b", PriorityCritical, 8, true)))
	c.advance(time.Second)
	must(t, s.Submit(job("b2", "b", PriorityCritical, 8, true)))
	s.Schedule()
	if !state(t, s, "b2").Borrowed || state(t, s, "b1").Borrowed {
		t.Fatal("second b job should be the borrowed one")
	}
	// a, under its guarantee, reclaims even with lower priority.
	c.advance(time.Second)
	must(t, s.Submit(job("a1", "a", PriorityBatch, 8, false)))
	s.Schedule()
	if state(t, s, "a1").State != JobRunning || state(t, s, "b2").State != JobPending {
		t.Fatal("tenant under guarantee should reclaim borrowed GPUs")
	}
	if state(t, s, "b1").State != JobRunning {
		t.Fatal("guaranteed job must not be touched")
	}
	// a borrowing more must not touch b's guaranteed job.
	must(t, s.Submit(job("a2", "a", PriorityCritical+1, 8, true)))
	if st := s.Schedule(); st.Preempted != 0 {
		t.Fatal("borrowing tenant preempted a guaranteed job")
	}
}

func TestQuotaMaxHoldsJobs(t *testing.T) {
	s, _, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 2, Max: 4}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	for i := 0; i < 3; i++ {
		must(t, s.Submit(job(fmt.Sprintf("j%d", i), "a", 500, 2, true)))
	}
	st := s.Schedule()
	if st.Bound != 2 || st.QuotaHeld != 1 {
		t.Fatalf("max=4 should admit two 2-GPU jobs: %+v", st)
	}
	if err := s.Submit(job("huge", "a", 500, 5, true)); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("job bigger than tenant max should be rejected, got %v", err)
	}
}

func TestDedicatedPoolIsolation(t *testing.T) {
	s, _, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "secure", Guaranteed: 8, Max: 8, Pool: "secure"}))
	must(t, s.UpsertTenant(Tenant{Name: "shared", Guaranteed: 8, Max: 16}))
	must(t, s.RegisterNode("s1", "h100", 8, "secure"))
	must(t, s.RegisterNode("g1", "a100", 8, ""))
	must(t, s.Submit(job("x", "shared", 500, 8, true)))
	must(t, s.Submit(job("y", "shared", 500, 8, true)))
	must(t, s.Submit(job("z", "secure", 500, 8, true)))
	s.Schedule()
	if n := state(t, s, "z").Node; n != "s1" {
		t.Fatalf("secure job on %q", n)
	}
	if state(t, s, "y").State != JobPending && state(t, s, "x").State != JobPending {
		t.Fatal("shared tenant leaked onto dedicated pool")
	}
}

func TestGPUTypeConstraint(t *testing.T) {
	s, _, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 16, Max: 16}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	must(t, s.RegisterNode("n2", "h100", 8, ""))
	j := job("h", "a", 500, 1, true)
	j.GPUType = "h100"
	must(t, s.Submit(j))
	s.Schedule()
	if state(t, s, "h").Node != "n2" {
		t.Fatal("gpu type constraint ignored")
	}
}

func TestNodeExpiryRequeues(t *testing.T) {
	s, c, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 16, Max: 16}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	must(t, s.Submit(job("j", "a", 500, 8, true)))
	s.Schedule()
	c.advance(20 * time.Second)
	must(t, s.RegisterNode("n2", "a100", 8, "")) // fresh heartbeat
	down := s.ExpireNodes(15 * time.Second)
	if len(down) != 1 || down[0] != "n1" {
		t.Fatalf("expected n1 down, got %v", down)
	}
	j := state(t, s, "j")
	if j.State != JobPending || j.Requeues != 1 {
		t.Fatalf("job should be requeued: %+v", j)
	}
	s.Schedule()
	if state(t, s, "j").Node != "n2" {
		t.Fatal("requeued job should land on the healthy node")
	}
	must(t, s.Heartbeat("n1"))
	if sn := s.Snapshot(); sn.GPUsTotal != 16 {
		t.Fatalf("n1 should be back: %d", sn.GPUsTotal)
	}
}

func TestAgingLetsOldBatchJobOvertake(t *testing.T) {
	s, c, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 8, Max: 8}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	must(t, s.Submit(job("old", "a", PriorityStandard, 8, false)))
	c.advance(3 * time.Hour) // +300 cap → 800
	must(t, s.Submit(job("new", "a", PriorityStandard+200, 8, false)))
	s.Schedule()
	if state(t, s, "old").State != JobRunning {
		t.Fatal("aging should lift the old job above a slightly higher new one")
	}
}

func TestCompleteCancelAndIdempotency(t *testing.T) {
	s, _, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 8, Max: 8}))
	must(t, s.RegisterNode("n1", "a100", 8, ""))
	must(t, s.Submit(job("j", "a", 500, 8, true)))
	if err := s.Submit(job("j", "a", 500, 8, true)); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("want duplicate error, got %v", err)
	}
	s.Schedule()
	must(t, s.Complete("j", true, ""))
	must(t, s.Complete("j", true, "")) // duplicate report is fine
	if state(t, s, "j").State != JobSucceeded || s.Snapshot().GPUsUsed != 0 {
		t.Fatal("completion should free GPUs")
	}
	must(t, s.Submit(job("k", "a", 500, 8, true)))
	must(t, s.Cancel("k"))
	if s.Schedule().Bound != 0 {
		t.Fatal("cancelled job must not be bound")
	}
}

func TestRestoreRebindsRunningJobs(t *testing.T) {
	s, _, _ := newTest(t, DefaultConfig())
	must(t, s.UpsertTenant(Tenant{Name: "a", Guaranteed: 8, Max: 8}))
	s.RestoreNode(Node{Name: "n1", GPUType: "a100", GPUs: 8})
	s.Restore(Job{ID: "r", Tenant: "a", Priority: 500, GPUs: 4, Runtime: time.Hour, State: JobRunning, Node: "n1", GPUIndices: []int{0, 1, 2, 3}})
	s.Restore(Job{ID: "bad", Tenant: "a", Priority: 500, GPUs: 4, Runtime: time.Hour, State: JobRunning, Node: "gone", GPUIndices: []int{0, 1, 2, 3}})
	if sn := s.Snapshot(); sn.GPUsUsed != 4 || sn.Tenants["a"].InUse != 4 {
		t.Fatalf("restore should rebuild allocation: %+v", sn)
	}
	if state(t, s, "bad").State != JobPending {
		t.Fatal("job on unknown node should be requeued")
	}
}

// Invariant check over a random workload: no GPU is double-allocated,
// usage matches allocations, and guaranteed jobs are never preempted by
// other tenants.
func TestInvariantsUnderChurn(t *testing.T) {
	s, c, evs := newTest(t, DefaultConfig())
	for _, tn := range []Tenant{{Name: "a", Guaranteed: 16, Max: 40}, {Name: "b", Guaranteed: 16, Max: 40}, {Name: "c", Guaranteed: 8, Max: 40}} {
		must(t, s.UpsertTenant(tn))
	}
	for i := 0; i < 5; i++ {
		must(t, s.RegisterNode(fmt.Sprintf("n%d", i), "a100", 8, ""))
	}
	rng := uint64(42)
	next := func(n int) int {
		rng = rng*6364136223846793005 + 1442695040888963407
		return int((rng >> 33) % uint64(n))
	}
	tenants := []string{"a", "b", "c"}
	prios := []int{PriorityBatch, PriorityStandard, PriorityCritical}
	sizes := []int{1, 2, 4, 8}
	for step := 0; step < 2000; step++ {
		c.advance(time.Minute)
		must(t, s.Submit(job(fmt.Sprintf("j%d", step), tenants[next(3)], prios[next(3)], sizes[next(4)], next(4) != 0)))
		for _, j := range s.Jobs(JobRunning) {
			if next(5) == 0 {
				must(t, s.Complete(j.ID, true, ""))
			}
		}
		s.Schedule()
		sn := s.Snapshot()
		used := map[string]int{}
		for _, n := range sn.Nodes {
			for _, a := range n.Alloc {
				if a != "" {
					j := state(t, s, a)
					if j.State != JobRunning || j.Node != n.Name {
						t.Fatalf("GPU held by non-running job %s", a)
					}
					used[j.Tenant]++
				}
			}
		}
		for name, tu := range sn.Tenants {
			if used[name] != tu.InUse || tu.InUse > tu.Max {
				t.Fatalf("tenant %s usage %d alloc %d max %d", name, tu.InUse, used[name], tu.Max)
			}
		}
	}
	for _, e := range *evs {
		if e.Type != EvPreempted {
			continue
		}
		by := state(t, s, e.By)
		if by.Tenant != e.Job.Tenant && !e.Job.Borrowed {
			t.Fatalf("cross-tenant preemption of guaranteed job %s by %s", e.Job.ID, e.By)
		}
		if !e.Job.Preemptible {
			t.Fatalf("non-preemptible job %s preempted", e.Job.ID)
		}
	}
}
