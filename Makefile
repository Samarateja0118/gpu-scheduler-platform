.PHONY: test bench sim images lint
test:
	go test -race ./...
bench:
	go test -run '^$$' -bench . -benchmem ./internal/scheduler/
sim:
	go run ./cmd/simbench -seeds 5 -hours 72 -loads 0.7,0.85,1.0 -out results/simbench
images:
	for c in scheduler nodeagent modelserver rollout gpuctl loadgen; do docker build --build-arg CMD=$$c -t gpusched/$$c:dev . ; done
lint:
	gofmt -l . && go vet ./...
	helm lint deploy/helm/gpu-scheduler
