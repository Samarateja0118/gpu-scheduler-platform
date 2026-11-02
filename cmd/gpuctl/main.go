// Command gpuctl is a small client for the scheduler API.
//
//	gpuctl submit -tenant research -priority standard -gpus 4 -runtime 60
//	gpuctl workload -n 200 -rate 5 -runtime-scale 0.05
//	gpuctl wait -timeout 10m        # until no job is pending or running
//	gpuctl cluster
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/controlplane"
)

var (
	server = strings.TrimRight(envOr("SCHEDULER_URL", "http://localhost:8080"), "/")
	token  = os.Getenv("API_TOKEN")
	client = &http.Client{Timeout: 10 * time.Second}
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: gpuctl submit|workload|wait|cluster|jobs [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "submit":
		err = submit(os.Args[2:])
	case "workload":
		err = workload(os.Args[2:])
	case "wait":
		err = wait(os.Args[2:])
	case "cluster":
		err = get("/v1/cluster")
	case "jobs":
		q := ""
		if len(os.Args) > 2 {
			q = "?state=" + os.Args[2]
		}
		err = get("/v1/jobs" + q)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func do(method, path string, body any) ([]byte, int, error) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, server+path, r)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return b, resp.StatusCode, err
}

func get(path string) error {
	b, code, err := do(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if code >= 300 {
		return fmt.Errorf("%d: %s", code, b)
	}
	var v any
	_ = json.Unmarshal(b, &v)
	out, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(out))
	return nil
}

func submitOne(req controlplane.SubmitRequest) error {
	b, code, err := do(http.MethodPost, "/v1/jobs", req)
	if err != nil {
		return err
	}
	if code != http.StatusAccepted {
		return fmt.Errorf("%d: %s", code, b)
	}
	return nil
}

func submit(args []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	var r controlplane.SubmitRequest
	fs.StringVar(&r.ID, "id", "", "job id (optional, makes retries idempotent)")
	fs.StringVar(&r.Tenant, "tenant", "research", "tenant")
	fs.StringVar(&r.Priority, "priority", "standard", "batch|standard|critical")
	fs.IntVar(&r.GPUs, "gpus", 1, "GPUs")
	fs.StringVar(&r.GPUType, "gpu-type", "", "required GPU type")
	fs.Float64Var(&r.RuntimeSeconds, "runtime", 60, "runtime seconds")
	_ = fs.Parse(args)
	return submitOne(r)
}

// workload submits a scaled-down version of the simulator's job mix.
func workload(args []string) error {
	fs := flag.NewFlagSet("workload", flag.ExitOnError)
	n := fs.Int("n", 100, "jobs")
	rate := fs.Float64("rate", 2, "jobs per second")
	scale := fs.Float64("runtime-scale", 0.05, "multiply simulated runtimes (minutes) by this to get seconds")
	seed := fs.Int64("seed", 1, "random seed")
	prefix := fs.String("prefix", "wl", "job id prefix")
	_ = fs.Parse(args)
	r := rand.New(rand.NewSource(*seed))
	type mix struct {
		tenant string
		w      float64
		prios  []string
		sizes  []int
		median float64 // minutes
	}
	mixes := []mix{
		{"inference", 0.35, []string{"critical", "critical", "standard"}, []int{1, 1, 2, 4}, 20},
		{"research", 0.40, []string{"standard", "standard", "batch"}, []int{1, 2, 4, 8}, 45},
		{"batch", 0.25, []string{"batch"}, []int{1, 1, 2, 4, 8}, 30},
	}
	start := time.Now()
	failed := 0
	for i := 0; i < *n; i++ {
		x := r.Float64()
		m := mixes[len(mixes)-1]
		for _, c := range mixes {
			if x < c.w {
				m = c
				break
			}
			x -= c.w
		}
		minutes := math.Min(480, math.Max(1, m.median*math.Exp(0.9*r.NormFloat64())))
		req := controlplane.SubmitRequest{ID: fmt.Sprintf("%s-%05d", *prefix, i), Tenant: m.tenant, Priority: m.prios[r.Intn(len(m.prios))],
			GPUs: m.sizes[r.Intn(len(m.sizes))], RuntimeSeconds: math.Max(1, minutes**scale)}
		if err := submitOne(req); err != nil {
			failed++
			fmt.Fprintln(os.Stderr, "submit:", err)
		}
		time.Sleep(time.Until(start.Add(time.Duration(float64(i+1) / *rate * float64(time.Second)))))
	}
	fmt.Printf("submitted %d jobs (%d failed) in %s\n", *n-failed, failed, time.Since(start).Round(time.Millisecond))
	if failed > 0 {
		return fmt.Errorf("%d submissions failed", failed)
	}
	return nil
}

func wait(args []string) error {
	fs := flag.NewFlagSet("wait", flag.ExitOnError)
	timeout := fs.Duration("timeout", 10*time.Minute, "give up after")
	_ = fs.Parse(args)
	deadline := time.Now().Add(*timeout)
	for time.Now().Before(deadline) {
		b, code, err := do(http.MethodGet, "/v1/cluster", nil)
		if err == nil && code == 200 {
			var c struct {
				Running int            `json:"runningJobs"`
				Pending map[string]int `json:"pendingByClass"`
				Used    int            `json:"gpusAllocated"`
				Total   int            `json:"gpusTotal"`
			}
			_ = json.Unmarshal(b, &c)
			pend := 0
			for _, v := range c.Pending {
				pend += v
			}
			fmt.Printf("%s running=%d pending=%d gpus=%d/%d\n", time.Now().Format(time.TimeOnly), c.Running, pend, c.Used, c.Total)
			if c.Running == 0 && pend == 0 {
				return nil
			}
		}
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("timed out")
}
