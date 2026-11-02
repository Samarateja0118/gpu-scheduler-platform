// Command modelserver is a stand-in inference server. It runs a small
// fixed-weight MLP on the CPU for each request (so autoscaling reacts to
// real CPU load) and can inject faults to exercise canary rollback.
//
//	MODEL_VERSION      reported in responses and metrics (default v1)
//	TRACK              stable | canary (set from the pod label)
//	FAULT_ERROR_RATE   fraction of requests answered with HTTP 500
//	FAULT_LATENCY_MS   extra latency added to every request
//	MLP_LAYERS         hidden layers to evaluate per request (default 8)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const dim = 128

var (
	version   = env("MODEL_VERSION", "v1")
	track     = env("TRACK", "stable")
	errRate   = envFloat("FAULT_ERROR_RATE", 0)
	extraLat  = time.Duration(envFloat("FAULT_LATENCY_MS", 0) * float64(time.Millisecond))
	layers    = int(envFloat("MLP_LAYERS", 8))
	weights   [][]float32
	requests  = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "modelserver_requests_total", Help: "Inference requests."}, []string{"version", "track", "code"})
	latency   = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "modelserver_request_duration_seconds", Help: "Inference latency.", Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}}, []string{"version", "track"})
	inflight  = prometheus.NewGauge(prometheus.GaugeOpts{Name: "modelserver_inflight_requests", Help: "Requests being served."})
	buildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "modelserver_build_info", Help: "Version of the running model."}, []string{"version", "track"})
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envFloat(k string, d float64) float64 {
	v, err := strconv.ParseFloat(os.Getenv(k), 64)
	if err != nil {
		return d
	}
	return v
}

func init() {
	r := rand.New(rand.NewSource(7))
	weights = make([][]float32, 4) // 4 distinct matrices reused across layers
	for l := range weights {
		weights[l] = make([]float32, dim*dim)
		for i := range weights[l] {
			weights[l][i] = float32(r.NormFloat64() / math.Sqrt(dim))
		}
	}
}

// forward evaluates a tanh MLP; ~layers*dim*dim multiply-adds.
func forward(in []float32) float32 {
	x := make([]float32, dim)
	copy(x, in)
	y := make([]float32, dim)
	for l := 0; l < layers; l++ {
		w := weights[l%len(weights)]
		for i := 0; i < dim; i++ {
			var s float32
			row := w[i*dim : (i+1)*dim]
			for k, v := range x {
				s += row[k] * v
			}
			y[i] = float32(math.Tanh(float64(s)))
		}
		x, y = y, x
	}
	var out float32
	for _, v := range x {
		out += v
	}
	return out / dim
}

type inferRequest struct {
	Inputs []float32 `json:"inputs"`
}

func infer(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	inflight.Inc()
	defer inflight.Dec()
	code := http.StatusOK
	defer func() {
		requests.WithLabelValues(version, track, strconv.Itoa(code)).Inc()
		latency.WithLabelValues(version, track).Observe(time.Since(start).Seconds())
	}()
	var req inferRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		code = http.StatusBadRequest
		http.Error(w, err.Error(), code)
		return
	}
	if extraLat > 0 {
		time.Sleep(extraLat)
	}
	out := forward(req.Inputs)
	if errRate > 0 && rand.Float64() < errRate {
		code = http.StatusInternalServerError
		http.Error(w, "injected fault", code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"version": version, "track": track, "score": out})
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	prometheus.MustRegister(requests, latency, inflight, buildInfo)
	buildInfo.WithLabelValues(version, track).Set(1)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/infer", infer)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.Handle("GET /metrics", promhttp.Handler())
	srv := &http.Server{Addr: env("LISTEN_ADDR", ":8080"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		slog.Info("serving", "version", version, "track", track, "errRate", errRate, "extraLatency", extraLat)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("listen", "err", err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
}
