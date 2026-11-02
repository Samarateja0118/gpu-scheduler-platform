// Command loadgen sends open-loop load to the model server in steps and
// prints one JSON line per step (latency percentiles, errors, and which
// track/version answered). Run it inside the cluster so requests go
// through the Service and spread over pods.
//
//	loadgen -url http://modelserver:8080/v1/infer -steps 30:60s,90:60s,180:120s
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type step struct {
	qps float64
	dur time.Duration
}

type result struct {
	lat   time.Duration
	code  int
	track string
}

func parseSteps(s string) ([]step, error) {
	var out []step
	for _, part := range strings.Split(s, ",") {
		q, d, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf("step %q: want qps:duration", part)
		}
		qps, err := strconv.ParseFloat(q, 64)
		if err != nil {
			return nil, err
		}
		dur, err := time.ParseDuration(d)
		if err != nil {
			return nil, err
		}
		out = append(out, step{qps, dur})
	}
	return out, nil
}

func main() {
	url := flag.String("url", "http://modelserver:8080/v1/infer", "endpoint")
	steps := flag.String("steps", "30:60s,90:60s,180:120s,30:60s", "qps:duration list")
	workers := flag.Int("workers", 64, "max concurrent requests")
	flag.Parse()
	plan, err := parseSteps(*steps)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: *workers, DisableKeepAlives: true}}
	body := []byte(`{"inputs":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]}`)
	sem := make(chan struct{}, *workers)
	start := time.Now()
	for i, st := range plan {
		var mu sync.Mutex
		var res []result
		dropped := 0
		var wg sync.WaitGroup
		interval := time.Duration(float64(time.Second) / st.qps)
		stepStart := time.Now()
		for n := 0; time.Since(stepStart) < st.dur; n++ {
			time.Sleep(time.Until(stepStart.Add(time.Duration(n) * interval)))
			select {
			case sem <- struct{}{}:
			default:
				dropped++ // all workers busy: count, do not queue (open loop)
				continue
			}
			wg.Add(1)
			go func() {
				defer func() { <-sem; wg.Done() }()
				t0 := time.Now()
				r := result{code: 0}
				resp, err := client.Post(*url, "application/json", bytes.NewReader(body))
				if err == nil {
					b, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					r.code = resp.StatusCode
					var v struct {
						Track   string `json:"track"`
						Version string `json:"version"`
					}
					if json.Unmarshal(b, &v) == nil && v.Track != "" {
						r.track = v.Track + "/" + v.Version
					}
				}
				r.lat = time.Since(t0)
				mu.Lock()
				res = append(res, r)
				mu.Unlock()
			}()
		}
		wg.Wait()
		lats := make([]float64, 0, len(res))
		errs := 0
		tracks := map[string]int{}
		for _, r := range res {
			lats = append(lats, float64(r.lat.Microseconds())/1000)
			if r.code != http.StatusOK {
				errs++
			}
			if r.track != "" {
				tracks[r.track]++
			}
		}
		sort.Float64s(lats)
		q := func(p float64) float64 {
			if len(lats) == 0 {
				return 0
			}
			return lats[min(len(lats)-1, int(p*float64(len(lats))))]
		}
		line, _ := json.Marshal(map[string]any{
			"step": i, "targetQps": st.qps, "seconds": st.dur.Seconds(), "tStart": stepStart.Sub(start).Seconds(),
			"sent": len(res), "achievedQps": float64(len(res)) / st.dur.Seconds(), "errors": errs, "droppedClientSide": dropped,
			"p50ms": q(0.50), "p95ms": q(0.95), "p99ms": q(0.99), "byTrack": tracks,
		})
		fmt.Println(string(line))
	}
}
