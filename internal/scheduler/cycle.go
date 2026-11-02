package scheduler

import (
	"sort"
	"time"
)

// CycleStats summarises one scheduling pass.
type CycleStats struct {
	Considered int
	Bound      int
	Preempted  int
	QuotaHeld  int // jobs skipped because their tenant is at Max
	Unplaced   int // jobs that fit quota but found no node
	Duration   time.Duration
}

// Schedule runs one pass over the pending queue: jobs are taken in
// priority order (with aging) and placed on the best fitting node; when
// nothing fits and preemption is enabled, lower priority or borrowed work
// is evicted to make room. A pass never blocks behind a job that does not
// fit (no reservations), so small jobs can overtake large ones.
func (s *Scheduler) Schedule() CycleStats {
	start := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var st CycleStats
	// Within one pass, a failed attempt depends only on the job's shape and
	// on cluster state. Placement failures stay valid until GPUs are freed
	// (an eviction); preemption failures until anything changes (a bind).
	// Caching both keeps a pass over a long backlog cheap when full.
	type shape struct {
		tenant, gpuType string
		prio, gpus      int
	}
	noFit := map[shape]bool{}
	noPreempt := map[shape]bool{}
	for _, j := range s.queueOrder(now) {
		st.Considered++
		t := s.tenants[j.Tenant]
		if t == nil {
			continue
		}
		quotaNeed := 0
		if s.cfg.EnforceQuota {
			quotaNeed = s.usage[j.Tenant] + j.GPUs - t.Max
		}
		key := shape{j.Tenant, j.GPUType, j.Priority, j.GPUs}
		preemptAndBind := func(need int) bool {
			if !s.cfg.Preemption || noPreempt[key] {
				return false
			}
			n, victims := s.findPreemption(j, t, need)
			if n == nil {
				noPreempt[key] = true
				return false
			}
			for _, v := range victims {
				s.evict(v, j, now)
			}
			st.Preempted += len(victims)
			gpus, _ := s.pickGPUs(n, j.GPUs)
			s.bind(j, n, gpus, now)
			st.Bound++
			clear(noFit)
			clear(noPreempt)
			return true
		}
		if quotaNeed > 0 {
			// Over Max: the only way in is to displace the tenant's own
			// lower priority work on the node it lands on.
			if !preemptAndBind(quotaNeed) {
				st.QuotaHeld++
			}
			continue
		}
		if !noFit[key] {
			if n, gpus := s.findPlacement(j, t); n != nil {
				s.bind(j, n, gpus, now)
				st.Bound++
				clear(noPreempt)
				continue
			}
			noFit[key] = true
		}
		if !preemptAndBind(0) {
			st.Unplaced++
		}
	}
	st.Duration = time.Since(start)
	return st
}

func (s *Scheduler) effectivePriority(j *Job, now time.Time) int {
	p := j.Priority
	if s.cfg.AgingInterval > 0 && s.cfg.AgingBoost > 0 {
		boost := int(now.Sub(j.QueuedAt)/s.cfg.AgingInterval) * s.cfg.AgingBoost
		if s.cfg.AgingCap > 0 && boost > s.cfg.AgingCap {
			boost = s.cfg.AgingCap
		}
		p += boost
	}
	return p
}

func (s *Scheduler) queueOrder(now time.Time) []*Job {
	q := make([]*Job, 0, len(s.pending))
	eff := make(map[string]int, len(s.pending))
	for _, j := range s.pending {
		q = append(q, j)
		eff[j.ID] = s.effectivePriority(j, now)
	}
	sort.Slice(q, func(a, b int) bool {
		ja, jb := q[a], q[b]
		if s.cfg.Ordering == OrderPriority && eff[ja.ID] != eff[jb.ID] {
			return eff[ja.ID] > eff[jb.ID]
		}
		if !ja.SubmittedAt.Equal(jb.SubmittedAt) {
			return ja.SubmittedAt.Before(jb.SubmittedAt)
		}
		return ja.ID < jb.ID
	})
	return q
}

// eligible reports whether node n may ever run job j of tenant t,
// ignoring current allocations.
func eligible(n *Node, j *Job, t *Tenant) bool {
	if !n.schedulable() || n.GPUs < j.GPUs {
		return false
	}
	if j.GPUType != "" && j.GPUType != n.GPUType {
		return false
	}
	return n.Pool == t.Pool // dedicated pools isolate tenants both ways
}

func (s *Scheduler) findPlacement(j *Job, t *Tenant) (*Node, []int) {
	var best *Node
	var bestGPUs []int
	bestScore := 0
	for _, name := range s.nodeNames {
		n := s.nodes[name]
		if !eligible(n, j, t) {
			continue
		}
		free := n.FreeGPUs()
		if free < j.GPUs {
			continue
		}
		gpus, inIsland := s.pickGPUs(n, j.GPUs)
		var score int
		switch s.cfg.Placement {
		case PlaceFirstFit:
			return n, gpus
		case PlaceSpread:
			score = -free * 2
		default: // best fit
			score = (free - j.GPUs) * 2
		}
		if !inIsland {
			score++ // tie-break toward placements that stay in one island
		}
		if best == nil || score < bestScore {
			best, bestGPUs, bestScore = n, gpus, score
		}
	}
	return best, bestGPUs
}

// pickGPUs chooses k free GPU indices on n. When the job fits in one
// interconnect island it is kept inside the fullest island that fits.
func (s *Scheduler) pickGPUs(n *Node, k int) ([]int, bool) {
	is := s.cfg.IslandSize
	if is > 0 && k <= is && n.GPUs%is == 0 {
		bestIsland, bestFree := -1, 0
		for start := 0; start < n.GPUs; start += is {
			free := 0
			for i := start; i < start+is; i++ {
				if n.Alloc[i] == "" {
					free++
				}
			}
			if free >= k && (bestIsland < 0 || free < bestFree) {
				bestIsland, bestFree = start, free
			}
		}
		if bestIsland >= 0 {
			var out []int
			for i := bestIsland; i < bestIsland+is && len(out) < k; i++ {
				if n.Alloc[i] == "" {
					out = append(out, i)
				}
			}
			return out, true
		}
	}
	var out []int
	for i := 0; i < n.GPUs && len(out) < k; i++ {
		if n.Alloc[i] == "" {
			out = append(out, i)
		}
	}
	return out, is == 0 || k > is
}

func (s *Scheduler) bind(j *Job, n *Node, gpus []int, now time.Time) {
	for _, i := range gpus {
		n.Alloc[i] = j.ID
	}
	delete(s.pending, j.ID)
	j.Wait += now.Sub(j.QueuedAt)
	j.State, j.Node, j.GPUIndices, j.StartedAt, j.Message = JobRunning, n.Name, gpus, now, ""
	s.usage[j.Tenant] += j.GPUs
	s.running[j.ID] = j
	s.recomputeBorrowed(j.Tenant)
	s.emit(Event{Type: EvBound, Time: now, Job: j, Node: n.Name})
}

// canPreempt applies the preemption rules for preemptor j (tenant t):
//   - a job never preempts non-preemptible work;
//   - within a tenant, only strictly lower priority jobs are victims;
//   - across tenants with quotas on, only borrowed jobs are victims, and
//     only if j is reclaiming its guarantee or outranks the victim;
//   - with quotas off, any strictly lower priority job is a victim.
func (s *Scheduler) canPreempt(j *Job, withinGuarantee bool, v *Job) bool {
	if !v.Preemptible {
		return false
	}
	if v.Tenant == j.Tenant {
		return v.Priority < j.Priority
	}
	if !s.cfg.EnforceQuota {
		return v.Priority < j.Priority
	}
	return v.Borrowed && (withinGuarantee || v.Priority < j.Priority)
}

// findPreemption picks the node where evicting the cheapest victim set
// frees enough GPUs. Cost order: highest victim priority, then number of
// victims, then GPU-seconds of work thrown away. If quotaNeed > 0 the job
// is over its tenant's Max, so only the tenant's own jobs are candidates
// and they must free at least quotaNeed GPUs of quota as well as space.
func (s *Scheduler) findPreemption(j *Job, t *Tenant, quotaNeed int) (*Node, []*Job) {
	now := s.now()
	within := s.cfg.EnforceQuota && s.usage[j.Tenant]+j.GPUs <= t.Guaranteed
	type cost struct {
		maxPrio, count int
		lost           float64
	}
	var bestNode *Node
	var bestVictims []*Job
	var best cost
	for _, name := range s.nodeNames {
		n := s.nodes[name]
		if !eligible(n, j, t) {
			continue
		}
		var cands []*Job
		for _, id := range uniqueJobs(n.Alloc) {
			v := s.jobs[id]
			if quotaNeed > 0 && v.Tenant != j.Tenant {
				continue
			}
			if s.canPreempt(j, within, v) {
				cands = append(cands, v)
			}
		}
		sort.Slice(cands, func(a, b int) bool {
			va, vb := cands[a], cands[b]
			if va.Priority != vb.Priority {
				return va.Priority < vb.Priority
			}
			if va.Borrowed != vb.Borrowed {
				return va.Borrowed
			}
			return va.StartedAt.After(vb.StartedAt) // least work lost first
		})
		free := n.FreeGPUs()
		var victims []*Job
		c := cost{}
		freedQuota := 0
		for _, v := range cands {
			if free >= j.GPUs && freedQuota >= quotaNeed {
				break
			}
			victims = append(victims, v)
			free += v.GPUs
			freedQuota += v.GPUs
			if v.Priority > c.maxPrio {
				c.maxPrio = v.Priority
			}
			c.count++
			c.lost += now.Sub(v.StartedAt).Seconds() * float64(v.GPUs)
		}
		if free < j.GPUs || freedQuota < quotaNeed || len(victims) == 0 {
			continue
		}
		better := bestNode == nil || c.maxPrio < best.maxPrio ||
			(c.maxPrio == best.maxPrio && (c.count < best.count ||
				(c.count == best.count && c.lost < best.lost)))
		if better {
			bestNode, bestVictims, best = n, victims, c
		}
	}
	return bestNode, bestVictims
}

func (s *Scheduler) evict(v, by *Job, now time.Time) {
	lost := now.Sub(v.StartedAt)
	node := v.Node
	s.release(v)
	v.Preemptions++
	v.State, v.Node, v.GPUIndices, v.QueuedAt = JobPending, "", nil, now
	v.Message = "preempted by " + by.ID
	s.pending[v.ID] = v
	s.recomputeBorrowed(v.Tenant)
	s.emit(Event{Type: EvPreempted, Time: now, Job: v, Node: node, By: by.ID, Lost: lost})
}

// Snapshot is a point-in-time view used by the API and metrics.
type Snapshot struct {
	Nodes        []*Node                `json:"nodes"`
	Tenants      map[string]TenantUsage `json:"tenants"`
	PendingByCls map[string]int         `json:"pendingByClass"`
	RunningJobs  int                    `json:"runningJobs"`
	GPUsTotal    int                    `json:"gpusTotal"`
	GPUsUsed     int                    `json:"gpusUsed"`
	OldestWait   time.Duration          `json:"-"`
}

// Snapshot returns copies of nodes and summary counts.
func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	sn := Snapshot{Tenants: map[string]TenantUsage{}, PendingByCls: map[string]int{"batch": 0, "standard": 0, "critical": 0}}
	for _, name := range s.nodeNames {
		n := s.nodes[name]
		sn.Nodes = append(sn.Nodes, n.clone())
		if n.Ready {
			sn.GPUsTotal += n.GPUs
			sn.GPUsUsed += n.GPUs - n.FreeGPUs()
		}
	}
	for name, t := range s.tenants {
		sn.Tenants[name] = TenantUsage{Tenant: *t, InUse: s.usage[name]}
	}
	for _, j := range s.pending {
		sn.PendingByCls[PriorityClassName(j.Priority)]++
		if w := now.Sub(j.QueuedAt); w > sn.OldestWait {
			sn.OldestWait = w
		}
	}
	sn.RunningJobs = len(s.running)
	return sn
}
