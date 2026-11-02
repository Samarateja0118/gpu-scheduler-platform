// Package rollout runs a canary release of the model server and rolls it
// back automatically when the canary breaks its SLO.
//
// Traffic is split by replica ratio: stable and canary Deployments share a
// Service selector, so the canary's share of requests is roughly
// canary/(canary+stable) pods. This needs no service mesh, and it leaves
// the stable Deployment's replica count to its HorizontalPodAutoscaler.
package rollout

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"
)

// Cluster is what the controller needs from Kubernetes.
type Cluster interface {
	StableReplicas(ctx context.Context) (int32, error)
	// SetCanary points the canary Deployment at image (plus env overrides,
	// nil for none) with n replicas and waits until they are ready.
	SetCanary(ctx context.Context, image string, env map[string]string, n int32) error
	// Promote moves the stable Deployment to image and waits for the
	// rolling update to finish.
	Promote(ctx context.Context, image string) error
	// Annotate records the rollout outcome on the canary Deployment.
	Annotate(ctx context.Context, status, reason string) error
}

// Metrics runs instant PromQL queries. ok is false when the query returned
// no series (e.g. no traffic yet).
type Metrics interface {
	Query(ctx context.Context, q string) (v float64, ok bool, err error)
}

// SLO is the bar a canary must clear at every analysis.
type SLO struct {
	MaxErrorRate float64 // e.g. 0.01 = 1% of requests may fail
	MaxP95       float64 // seconds
	MinRequests  float64 // canary requests per analysis window to judge at all
}

// Plan describes the rollout.
type Plan struct {
	Image           string
	Env             map[string]string // env overrides for the canary
	Steps           []int             // canary traffic percentages, e.g. 10, 25, 50
	StepDuration    time.Duration
	AnalysisEvery   time.Duration
	FailuresToAbort int    // consecutive SLO breaches that trigger rollback
	Window          string // PromQL range, e.g. "1m"
	Selector        string // extra label matchers for the canary series
}

// Analysis is one evaluation of the canary.
type Analysis struct {
	Step      int     `json:"stepPercent"`
	Requests  float64 `json:"requests"`
	ErrorRate float64 `json:"errorRate"`
	P95       float64 `json:"p95Seconds"`
	Verdict   string  `json:"verdict"` // pass | breach | no-data
	Reason    string  `json:"reason,omitempty"`
}

// Result summarises a rollout.
type Result struct {
	Promoted   bool       `json:"promoted"`
	RolledBack bool       `json:"rolledBack"`
	Reason     string     `json:"reason"`
	Analyses   []Analysis `json:"analyses"`
	Duration   string     `json:"duration"`
}

// CanaryReplicas is the canary pod count that gives about pct percent of
// traffic next to `stable` stable pods (at least 1).
func CanaryReplicas(stable int32, pct int) int32 {
	if pct >= 100 {
		return stable
	}
	n := math.Round(float64(stable) * float64(pct) / float64(100-pct))
	if n < 1 {
		n = 1
	}
	return int32(n)
}

// Controller drives one rollout.
type Controller struct {
	Cluster Cluster
	Metrics Metrics
	SLO     SLO
	Sleep   func(context.Context, time.Duration) error // injectable for tests
	Now     func() time.Time
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func (c *Controller) analyse(ctx context.Context, p Plan, step int) (Analysis, error) {
	sel := `track="canary"`
	if p.Selector != "" {
		sel += "," + p.Selector
	}
	a := Analysis{Step: step}
	reqs, ok, err := c.Metrics.Query(ctx, fmt.Sprintf(`sum(increase(modelserver_requests_total{%s}[%s]))`, sel, p.Window))
	if err != nil {
		return a, err
	}
	if !ok || reqs < c.SLO.MinRequests {
		a.Requests, a.Verdict, a.Reason = reqs, "no-data", fmt.Sprintf("only %.0f canary requests in %s", reqs, p.Window)
		return a, nil
	}
	a.Requests = reqs
	errs, _, err := c.Metrics.Query(ctx, fmt.Sprintf(`sum(increase(modelserver_requests_total{%s,code=~"5.."}[%s]))`, sel, p.Window))
	if err != nil {
		return a, err
	}
	a.ErrorRate = errs / reqs
	p95, ok, err := c.Metrics.Query(ctx, fmt.Sprintf(`histogram_quantile(0.95, sum by (le) (rate(modelserver_request_duration_seconds_bucket{%s}[%s])))`, sel, p.Window))
	if err != nil {
		return a, err
	}
	if ok && !math.IsNaN(p95) {
		a.P95 = p95
	}
	switch {
	case a.ErrorRate > c.SLO.MaxErrorRate:
		a.Verdict, a.Reason = "breach", fmt.Sprintf("error rate %.2f%% > %.2f%%", 100*a.ErrorRate, 100*c.SLO.MaxErrorRate)
	case a.P95 > c.SLO.MaxP95:
		a.Verdict, a.Reason = "breach", fmt.Sprintf("p95 %.0fms > %.0fms", 1000*a.P95, 1000*c.SLO.MaxP95)
	default:
		a.Verdict = "pass"
	}
	return a, nil
}

// Run executes the plan. On SLO breach (or a step with no usable data) it
// scales the canary to zero and returns RolledBack. On success it promotes
// the image to the stable Deployment and removes the canary.
func (c *Controller) Run(ctx context.Context, p Plan) (Result, error) {
	if c.Sleep == nil {
		c.Sleep = sleepCtx
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	start := c.Now()
	res := Result{}
	rollback := func(reason string) (Result, error) {
		slog.Warn("rolling back canary", "reason", reason)
		res.RolledBack, res.Reason = true, reason
		res.Duration = c.Now().Sub(start).Round(time.Second).String()
		// Use a fresh context: rollback must happen even if ctx expired.
		rctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := c.Cluster.SetCanary(rctx, p.Image, nil, 0); err != nil {
			return res, fmt.Errorf("rollback failed: %w", err)
		}
		return res, c.Cluster.Annotate(rctx, "rolled-back", reason)
	}
	for _, pct := range p.Steps {
		stable, err := c.Cluster.StableReplicas(ctx)
		if err != nil {
			return rollback("cannot read stable replicas: " + err.Error())
		}
		n := CanaryReplicas(stable, pct)
		slog.Info("canary step", "percent", pct, "stableReplicas", stable, "canaryReplicas", n)
		if err := c.Cluster.SetCanary(ctx, p.Image, p.Env, n); err != nil {
			return rollback("canary did not become ready: " + err.Error())
		}
		breaches, judged := 0, 0
		deadline := c.Now().Add(p.StepDuration)
		for c.Now().Before(deadline) || judged == 0 {
			if err := c.Sleep(ctx, p.AnalysisEvery); err != nil {
				return rollback("interrupted: " + err.Error())
			}
			a, err := c.analyse(ctx, p, pct)
			if err != nil {
				return rollback("metrics query failed: " + err.Error())
			}
			res.Analyses = append(res.Analyses, a)
			slog.Info("analysis", "step", pct, "verdict", a.Verdict, "requests", a.Requests, "errorRate", a.ErrorRate, "p95", a.P95, "reason", a.Reason)
			switch a.Verdict {
			case "breach":
				breaches++
				judged++
				if breaches >= p.FailuresToAbort {
					return rollback(fmt.Sprintf("SLO breach at %d%%: %s", pct, a.Reason))
				}
			case "pass":
				breaches = 0
				judged++
			case "no-data":
				// A canary that never gets traffic is not a pass.
				if c.Now().After(deadline.Add(p.StepDuration)) {
					return rollback(fmt.Sprintf("no canary traffic at %d%%", pct))
				}
			}
		}
	}
	slog.Info("canary healthy at every step; promoting", "image", p.Image)
	if err := c.Cluster.Promote(ctx, p.Image); err != nil {
		return rollback("promotion failed: " + err.Error())
	}
	if err := c.Cluster.SetCanary(ctx, p.Image, nil, 0); err != nil {
		return res, err
	}
	res.Promoted, res.Reason = true, "all steps within SLO"
	res.Duration = c.Now().Sub(start).Round(time.Second).String()
	return res, c.Cluster.Annotate(ctx, "promoted", res.Reason)
}
