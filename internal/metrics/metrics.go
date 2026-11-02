// Package metrics exposes scheduler state to Prometheus.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/scheduler"
)

// Metrics holds the control plane's collectors.
type Metrics struct {
	Submitted      *prometheus.CounterVec
	Finished       *prometheus.CounterVec
	Preemptions    *prometheus.CounterVec
	PreemptedGPUs  prometheus.Counter
	Requeues       prometheus.Counter
	QueueWait      *prometheus.HistogramVec
	CycleDuration  prometheus.Histogram
	CycleBound     prometheus.Counter
	IntakeDepth    prometheus.Gauge
	PersistBacklog prometheus.Gauge
	PersistErrors  prometheus.Counter

	gpusTotal       *prometheus.GaugeVec
	gpusUsed        *prometheus.GaugeVec
	nodes           *prometheus.GaugeVec
	pending         *prometheus.GaugeVec
	running         prometheus.Gauge
	oldestPending   prometheus.Gauge
	tenantInUse     *prometheus.GaugeVec
	tenantGuarantee *prometheus.GaugeVec
	tenantMax       *prometheus.GaugeVec
}

// New registers collectors on reg.
func New(reg prometheus.Registerer) *Metrics {
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	m := &Metrics{
		Submitted:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpusched_jobs_submitted_total", Help: "Jobs admitted by the scheduler."}, []string{"tenant", "class"}),
		Finished:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpusched_jobs_finished_total", Help: "Jobs that reached a terminal state."}, []string{"tenant", "result"}),
		Preemptions:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gpusched_preemptions_total", Help: "Jobs evicted to make room for other jobs."}, []string{"tenant", "class"}),
		PreemptedGPUs:  prometheus.NewCounter(prometheus.CounterOpts{Name: "gpusched_preempted_gpu_seconds_total", Help: "GPU-seconds of work discarded by preemption (no checkpointing)."}),
		Requeues:       prometheus.NewCounter(prometheus.CounterOpts{Name: "gpusched_node_failure_requeues_total", Help: "Running jobs requeued because their node stopped heartbeating."}),
		QueueWait:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gpusched_queue_wait_seconds", Help: "Time a job waited in the queue before each start.", Buckets: []float64{0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}}, []string{"class"}),
		CycleDuration:  prometheus.NewHistogram(prometheus.HistogramOpts{Name: "gpusched_schedule_cycle_seconds", Help: "Wall time of one scheduling pass.", Buckets: prometheus.ExponentialBuckets(0.00001, 4, 10)}),
		CycleBound:     prometheus.NewCounter(prometheus.CounterOpts{Name: "gpusched_bindings_total", Help: "Jobs bound to nodes."}),
		IntakeDepth:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "gpusched_intake_queue_depth", Help: "Submissions not yet admitted (Redis stream pending + lag)."}),
		PersistBacklog: prometheus.NewGauge(prometheus.GaugeOpts{Name: "gpusched_persist_backlog", Help: "Events waiting to be written to Postgres."}),
		PersistErrors:  prometheus.NewCounter(prometheus.CounterOpts{Name: "gpusched_persist_errors_total", Help: "Failed Postgres writes (retried)."}),

		gpusTotal:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpusched_gpus_total", Help: "GPUs on ready nodes."}, []string{"gpu_type"}),
		gpusUsed:        prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpusched_gpus_allocated", Help: "GPUs allocated to running jobs on ready nodes."}, []string{"gpu_type"}),
		nodes:           prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpusched_nodes", Help: "Nodes by state."}, []string{"state"}),
		pending:         prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpusched_pending_jobs", Help: "Jobs waiting for GPUs."}, []string{"class"}),
		running:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "gpusched_running_jobs", Help: "Jobs holding GPUs."}),
		oldestPending:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "gpusched_oldest_pending_seconds", Help: "Age of the longest waiting job."}),
		tenantInUse:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpusched_tenant_gpus_in_use", Help: "GPUs held by a tenant."}, []string{"tenant"}),
		tenantGuarantee: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpusched_tenant_gpus_guaranteed", Help: "Tenant guaranteed GPUs."}, []string{"tenant"}),
		tenantMax:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gpusched_tenant_gpus_max", Help: "Tenant GPU limit including borrowing."}, []string{"tenant"}),
	}
	for _, c := range []prometheus.Collector{m.Submitted, m.Finished, m.Preemptions, m.PreemptedGPUs, m.Requeues, m.QueueWait, m.CycleDuration, m.CycleBound,
		m.IntakeDepth, m.PersistBacklog, m.PersistErrors, m.gpusTotal, m.gpusUsed, m.nodes, m.pending, m.running, m.oldestPending, m.tenantInUse, m.tenantGuarantee, m.tenantMax} {
		f(c)
	}
	return m
}

// Observe updates counters from a scheduler event.
func (m *Metrics) Observe(e scheduler.Event) {
	if e.Job == nil {
		return
	}
	cls := scheduler.PriorityClassName(e.Job.Priority)
	switch e.Type {
	case scheduler.EvSubmitted:
		m.Submitted.WithLabelValues(e.Job.Tenant, cls).Inc()
	case scheduler.EvBound:
		m.CycleBound.Inc()
		m.QueueWait.WithLabelValues(cls).Observe(e.Job.StartedAt.Sub(e.Job.QueuedAt).Seconds())
	case scheduler.EvPreempted:
		m.Preemptions.WithLabelValues(e.Job.Tenant, cls).Inc()
		m.PreemptedGPUs.Add(e.Lost.Seconds() * float64(e.Job.GPUs))
	case scheduler.EvRequeued:
		m.Requeues.Inc()
	case scheduler.EvCompleted:
		m.Finished.WithLabelValues(e.Job.Tenant, "succeeded").Inc()
	case scheduler.EvFailed:
		m.Finished.WithLabelValues(e.Job.Tenant, "failed").Inc()
	case scheduler.EvCancelled:
		m.Finished.WithLabelValues(e.Job.Tenant, "cancelled").Inc()
	}
}

// Update refreshes gauges from a snapshot.
func (m *Metrics) Update(sn scheduler.Snapshot) {
	m.gpusTotal.Reset()
	m.gpusUsed.Reset()
	ready, notReady, cordoned := 0, 0, 0
	for _, n := range sn.Nodes {
		switch {
		case !n.Ready:
			notReady++
			continue
		case n.Cordoned:
			cordoned++
		default:
			ready++
		}
		m.gpusTotal.WithLabelValues(n.GPUType).Add(float64(n.GPUs))
		m.gpusUsed.WithLabelValues(n.GPUType).Add(float64(n.GPUs - n.FreeGPUs()))
	}
	m.nodes.WithLabelValues("ready").Set(float64(ready))
	m.nodes.WithLabelValues("not_ready").Set(float64(notReady))
	m.nodes.WithLabelValues("cordoned").Set(float64(cordoned))
	for c, v := range sn.PendingByCls {
		m.pending.WithLabelValues(c).Set(float64(v))
	}
	m.running.Set(float64(sn.RunningJobs))
	m.oldestPending.Set(sn.OldestWait.Seconds())
	for name, t := range sn.Tenants {
		m.tenantInUse.WithLabelValues(name).Set(float64(t.InUse))
		m.tenantGuarantee.WithLabelValues(name).Set(float64(t.Guaranteed))
		m.tenantMax.WithLabelValues(name).Set(float64(t.Max))
	}
}
