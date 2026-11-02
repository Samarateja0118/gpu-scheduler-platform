package rollout

import (
	"context"
	"strings"
	"testing"
	"time"
)

type fakeCluster struct {
	stable     int32
	canary     int32
	promoted   string
	status     string
	history    []int32
	failCanary bool
}

func (f *fakeCluster) StableReplicas(context.Context) (int32, error) { return f.stable, nil }
func (f *fakeCluster) SetCanary(_ context.Context, _ string, _ map[string]string, n int32) error {
	f.history = append(f.history, n)
	if f.failCanary && n > 0 {
		return context.DeadlineExceeded
	}
	f.canary = n
	return nil
}
func (f *fakeCluster) Promote(_ context.Context, img string) error { f.promoted = img; return nil }
func (f *fakeCluster) Annotate(_ context.Context, s, _ string) error {
	f.status = s
	return nil
}

// fakeMetrics answers by query shape; errRate/p95 may change per call.
type fakeMetrics struct {
	reqs    float64
	errRate func(call int) float64
	p95     float64
	calls   int
}

func (m *fakeMetrics) Query(_ context.Context, q string) (float64, bool, error) {
	switch {
	case strings.Contains(q, "histogram_quantile"):
		return m.p95, true, nil
	case strings.Contains(q, `code=~"5.."`):
		m.calls++
		return m.reqs * m.errRate(m.calls), true, nil
	default:
		if m.reqs == 0 {
			return 0, false, nil
		}
		return m.reqs, true, nil
	}
}

func newCtl(c *fakeCluster, m *fakeMetrics) *Controller {
	now := time.Unix(0, 0)
	return &Controller{Cluster: c, Metrics: m, SLO: SLO{MaxErrorRate: 0.02, MaxP95: 0.25, MinRequests: 50},
		Now:   func() time.Time { return now },
		Sleep: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }}
}

var plan = Plan{Image: "modelserver:v2", Steps: []int{10, 25, 50}, StepDuration: time.Minute, AnalysisEvery: 15 * time.Second, FailuresToAbort: 2, Window: "1m"}

func TestCanaryReplicas(t *testing.T) {
	cases := []struct {
		stable int32
		pct    int
		want   int32
	}{{4, 10, 1}, {4, 25, 1}, {4, 50, 4}, {9, 10, 1}, {10, 50, 10}, {6, 25, 2}}
	for _, c := range cases {
		if got := CanaryReplicas(c.stable, c.pct); got != c.want {
			t.Errorf("CanaryReplicas(%d,%d)=%d want %d", c.stable, c.pct, got, c.want)
		}
	}
}

func TestHealthyCanaryIsPromoted(t *testing.T) {
	c := &fakeCluster{stable: 4}
	m := &fakeMetrics{reqs: 500, p95: 0.05, errRate: func(int) float64 { return 0.001 }}
	res, err := newCtl(c, m).Run(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Promoted || c.promoted != "modelserver:v2" || c.canary != 0 || c.status != "promoted" {
		t.Fatalf("expected promotion: %+v %+v", res, c)
	}
	if len(res.Analyses) != 12 { // 3 steps x 4 analyses
		t.Fatalf("analyses: %d", len(res.Analyses))
	}
}

func TestErrorBreachRollsBack(t *testing.T) {
	c := &fakeCluster{stable: 4}
	m := &fakeMetrics{reqs: 500, p95: 0.05, errRate: func(int) float64 { return 0.2 }}
	res, err := newCtl(c, m).Run(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if !res.RolledBack || c.promoted != "" || c.canary != 0 || c.status != "rolled-back" {
		t.Fatalf("expected rollback: %+v %+v", res, c)
	}
	if len(res.Analyses) != 2 || !strings.Contains(res.Reason, "error rate") {
		t.Fatalf("should abort after 2 consecutive breaches at first step: %+v", res)
	}
}

func TestSingleBlipDoesNotAbort(t *testing.T) {
	c := &fakeCluster{stable: 4}
	m := &fakeMetrics{reqs: 500, p95: 0.05, errRate: func(call int) float64 {
		if call == 2 {
			return 0.5
		}
		return 0
	}}
	res, _ := newCtl(c, m).Run(context.Background(), plan)
	if !res.Promoted {
		t.Fatalf("one breach followed by passes should not roll back: %+v", res)
	}
}

func TestLatencyBreachRollsBack(t *testing.T) {
	c := &fakeCluster{stable: 4}
	m := &fakeMetrics{reqs: 500, p95: 0.6, errRate: func(int) float64 { return 0 }}
	res, _ := newCtl(c, m).Run(context.Background(), plan)
	if !res.RolledBack || !strings.Contains(res.Reason, "p95") {
		t.Fatalf("latency breach should roll back: %+v", res)
	}
}

func TestNoTrafficRollsBack(t *testing.T) {
	c := &fakeCluster{stable: 4}
	m := &fakeMetrics{reqs: 0, errRate: func(int) float64 { return 0 }}
	res, _ := newCtl(c, m).Run(context.Background(), plan)
	if !res.RolledBack || !strings.Contains(res.Reason, "no canary traffic") {
		t.Fatalf("no data must not count as a pass: %+v", res)
	}
}

func TestCanaryNotReadyRollsBack(t *testing.T) {
	c := &fakeCluster{stable: 4, failCanary: true}
	m := &fakeMetrics{reqs: 500, errRate: func(int) float64 { return 0 }}
	res, _ := newCtl(c, m).Run(context.Background(), plan)
	if !res.RolledBack || c.canary != 0 {
		t.Fatalf("unready canary should roll back: %+v", res)
	}
}
