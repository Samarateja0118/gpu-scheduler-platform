// Command simbench replays identical synthetic GPU job traces through each
// scheduling policy on a virtual clock and prints utilization, queue wait
// and preemption numbers. GPUs are simulated; the numbers describe the
// scheduling algorithm, not real hardware.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/sim"
)

func main() {
	seeds := flag.Int("seeds", 5, "number of random traces")
	hours := flag.Float64("hours", 72, "arrival window in hours")
	loads := flag.String("loads", "0.85,1.0", "comma separated offered loads")
	out := flag.String("out", "", "directory for results.json and results.md")
	flag.Parse()

	var all []sim.Result
	var md strings.Builder
	fmt.Fprintf(&md, "Cluster: 16 nodes x 8 simulated GPUs (12 a100, 4 h100), 3 tenants, %.0fh arrival window, %d seeds per cell. Values are means over seeds (min-max in brackets for GPU allocated). Waits are submission to first start, in minutes. GPUs are simulated; this measures the scheduling algorithm on a virtual clock.\n\n", *hours, *seeds)
	for _, ls := range strings.Split(*loads, ",") {
		load, err := strconv.ParseFloat(strings.TrimSpace(ls), 64)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(&md, "### Offered load %.2f\n\n", load)
		md.WriteString("| policy | jobs | GPU allocated | useful GPU util (minus preempted work) | idle GPUs while jobs queued | critical p95 wait (min) | standard p95 | batch p95 | 8-GPU p95 | preemptions | GPU-h lost to preemption | guarantee shortfall GPU-h | cycle p99 (us) |\n")
		md.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
		for _, p := range sim.Policies() {
			var rs []sim.Result
			for seed := int64(1); seed <= int64(*seeds); seed++ {
				sc := sim.DefaultScenario(seed, time.Duration(*hours*float64(time.Hour)), load)
				r, err := sim.Run(sc, p.Cfg, p.Name, seed)
				if err != nil {
					log.Fatalf("%s seed %d: %v", p.Name, seed, err)
				}
				rs = append(rs, r)
				all = append(all, r)
			}
			mean := func(f func(sim.Result) float64) float64 {
				s := 0.0
				for _, r := range rs {
					s += f(r)
				}
				return s / float64(len(rs))
			}
			utils := make([]float64, len(rs))
			for i, r := range rs {
				utils[i] = r.Utilization
			}
			sort.Float64s(utils)
			fmt.Fprintf(&md, "| %s | %.0f | %.1f%% [%.1f-%.1f] | %.1f%% | %.1f%% | %.1f | %.1f | %.1f | %.1f | %.0f | %.0f (%.1f%%) | %.0f | %.0f |\n",
				p.Name, mean(func(r sim.Result) float64 { return float64(r.Jobs) }),
				100*mean(func(r sim.Result) float64 { return r.Utilization }), 100*utils[0], 100*utils[len(utils)-1],
				100*mean(func(r sim.Result) float64 { return r.UsefulUtilization }),
				100*mean(func(r sim.Result) float64 { return r.IdleWhileQueued }),
				mean(func(r sim.Result) float64 { return r.ByClass["critical"].P95Min }),
				mean(func(r sim.Result) float64 { return r.ByClass["standard"].P95Min }),
				mean(func(r sim.Result) float64 { return r.ByClass["batch"].P95Min }),
				mean(func(r sim.Result) float64 { return r.BySize[8].P95Min }),
				mean(func(r sim.Result) float64 { return float64(r.Preemptions) }),
				mean(func(r sim.Result) float64 { return r.LostGPUHours }),
				100*mean(func(r sim.Result) float64 { return r.LostFrac }),
				mean(func(r sim.Result) float64 { return r.GuaranteeShortfall }),
				mean(func(r sim.Result) float64 { return r.CycleP99Micros }))
		}
		md.WriteString("\n")
	}
	fmt.Print(md.String())
	if *out != "" {
		if err := os.MkdirAll(*out, 0o755); err != nil {
			log.Fatal(err)
		}
		b, _ := json.MarshalIndent(all, "", "  ")
		must(os.WriteFile(filepath.Join(*out, "results.json"), b, 0o644))
		must(os.WriteFile(filepath.Join(*out, "results.md"), []byte(md.String()), 0o644))
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
