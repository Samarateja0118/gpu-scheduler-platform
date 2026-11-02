package scheduler

import (
	"fmt"
	"testing"
	"time"
)

// benchCluster builds nodes x 8 GPUs and `pending` queued jobs over three
// tenants with mixed sizes and priorities.
func benchCluster(b *testing.B, nodes, pending int) *Scheduler {
	b.Helper()
	s := New(DefaultConfig(), func() time.Time { return time.Unix(0, 0) })
	gpus := nodes * 8
	for _, t := range []Tenant{{Name: "a", Guaranteed: gpus / 3, Max: gpus}, {Name: "b", Guaranteed: gpus / 3, Max: gpus}, {Name: "c", Guaranteed: gpus / 3, Max: gpus}} {
		if err := s.UpsertTenant(t); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < nodes; i++ {
		if err := s.RegisterNode(fmt.Sprintf("n%04d", i), "a100", 8, ""); err != nil {
			b.Fatal(err)
		}
	}
	sizes := []int{1, 2, 4, 8}
	prios := []int{PriorityBatch, PriorityStandard, PriorityCritical}
	for i := 0; i < pending; i++ {
		j := Job{ID: fmt.Sprintf("j%06d", i), Tenant: string(rune('a' + i%3)), Priority: prios[i%3], GPUs: sizes[i%4], Runtime: time.Hour, Preemptible: i%3 != 2}
		if err := s.Submit(j); err != nil {
			b.Fatal(err)
		}
	}
	return s
}

// Full pass that places as much of a large backlog as fits, then tries
// preemption for everything left. This is the worst case per pass.
func BenchmarkScheduleBacklog(b *testing.B) {
	for _, c := range []struct{ nodes, pending int }{{100, 1000}, {500, 5000}} {
		b.Run(fmt.Sprintf("nodes=%d/pending=%d", c.nodes, c.pending), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				s := benchCluster(b, c.nodes, c.pending)
				b.StartTimer()
				s.Schedule()
			}
		})
	}
}

// Steady state: the cluster is full and a pass finds nothing to do. This
// is what most passes look like in production.
func BenchmarkScheduleSteadyFull(b *testing.B) {
	s := benchCluster(b, 500, 5000)
	s.Schedule()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Schedule()
	}
}
