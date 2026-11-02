// Command rollout performs a canary release of the model server with
// automatic rollback on SLO breach. It runs from CI or a laptop with a
// kubeconfig, or in-cluster as a Job.
//
//	rollout -image ghcr.io/x/modelserver:sha -steps 10,25,50 -prometheus http://localhost:9090
//
// Exit codes: 0 promoted, 3 rolled back, 1 error.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/rollout"
)

type envFlags map[string]string

func (e envFlags) String() string { return fmt.Sprint(map[string]string(e)) }
func (e envFlags) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok {
		return fmt.Errorf("want KEY=VALUE, got %q", v)
	}
	e[k] = val
	return nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	var (
		ns         = flag.String("namespace", "gpusched", "namespace")
		stable     = flag.String("stable", "modelserver", "stable Deployment")
		canary     = flag.String("canary", "modelserver-canary", "canary Deployment")
		image      = flag.String("image", "", "image to roll out (required)")
		steps      = flag.String("steps", "10,25,50", "canary traffic percentages")
		stepDur    = flag.Duration("step-duration", 2*time.Minute, "time at each step")
		every      = flag.Duration("analysis-interval", 20*time.Second, "time between SLO checks")
		failures   = flag.Int("failures", 2, "consecutive breaches that trigger rollback")
		window     = flag.String("window", "1m", "PromQL range for SLO queries")
		promURL    = flag.String("prometheus", "http://localhost:9090", "Prometheus base URL")
		maxErr     = flag.Float64("max-error-rate", 0.01, "SLO: max fraction of 5xx")
		maxP95     = flag.Float64("max-p95", 0.25, "SLO: max p95 latency (seconds)")
		minReq     = flag.Float64("min-requests", 50, "canary requests per window needed to judge")
		readyIn    = flag.Duration("ready-timeout", 3*time.Minute, "wait for pods to become ready")
		kubeconfig = flag.String("kubeconfig", "", "kubeconfig path (default: in-cluster, then $KUBECONFIG or ~/.kube/config)")
		env        = envFlags{}
	)
	flag.Var(env, "set-env", "KEY=VALUE env override for the canary (repeatable)")
	flag.Parse()
	if *image == "" {
		fmt.Fprintln(os.Stderr, "-image is required")
		os.Exit(1)
	}
	var pcts []int
	for _, s := range strings.Split(*steps, ",") {
		p, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || p <= 0 || p >= 100 {
			fmt.Fprintln(os.Stderr, "steps must be integers in (0,100)")
			os.Exit(1)
		}
		pcts = append(pcts, p)
	}
	var cfg *rest.Config
	var err error
	// In a pod: in-cluster config. Otherwise -kubeconfig, $KUBECONFIG or
	// ~/.kube/config, like kubectl.
	if cfg, err = rest.InClusterConfig(); err != nil {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		rules.ExplicitPath = *kubeconfig
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	}
	if err != nil {
		slog.Error("kube config", "err", err)
		os.Exit(1)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Error("kube client", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctl := &rollout.Controller{
		Cluster: &rollout.Kube{Client: cs, Namespace: *ns, Stable: *stable, Canary: *canary, ReadyWithin: *readyIn},
		Metrics: &rollout.Prometheus{URL: strings.TrimRight(*promURL, "/")},
		SLO:     rollout.SLO{MaxErrorRate: *maxErr, MaxP95: *maxP95, MinRequests: *minReq},
	}
	// Judge only the release being rolled out: without this, errors from a
	// previous (rolled back) canary still inside the query window would be
	// charged to the new one.
	selector := ""
	if v, ok := env["MODEL_VERSION"]; ok {
		selector = fmt.Sprintf("version=%q", v)
	}
	res, err := ctl.Run(ctx, rollout.Plan{Image: *image, Env: env, Steps: pcts, StepDuration: *stepDur, AnalysisEvery: *every, FailuresToAbort: *failures, Window: *window, Selector: selector})
	out, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(out))
	switch {
	case err != nil:
		slog.Error("rollout", "err", err)
		os.Exit(1)
	case res.RolledBack:
		os.Exit(3)
	}
}
