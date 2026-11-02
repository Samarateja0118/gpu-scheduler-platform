#!/usr/bin/env bash
# End-to-end scheduler test on a live cluster:
#   1. submit a scaled-down mixed workload through the API
#   2. kill a node agent pod mid-run (its simulated nodes stop heartbeating)
#   3. kill the scheduler pod mid-run (state must come back from Postgres)
#   4. wait for the queue to drain and check every job finished
# Requires kubectl access, jq, and the gpuctl binary on PATH.
set -euo pipefail
NS=${NS:-gpusched}
JOBS=${JOBS:-150}
RATE=${RATE:-5}
SCALE=${SCALE:-0.5}       # job runtime seconds = simulated minutes * SCALE
OUT=${OUT:-results/e2e}
mkdir -p "$OUT"
log() { echo "[$(date +%T)] $*"; }
retry() { for _ in $(seq 60); do "$@" >/dev/null 2>&1 && return 0; sleep 2; done; echo "timed out: $*"; exit 1; }

kubectl -n "$NS" port-forward svc/scheduler 18080:8080 >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
export SCHEDULER_URL=http://localhost:18080
export API_TOKEN=$(kubectl -n "$NS" get secret gpu-scheduler-secrets -o jsonpath='{.data.apiToken}' | base64 -d)
retry curl -sf $SCHEDULER_URL/readyz
retry bash -c "[ \"\$(curl -s $SCHEDULER_URL/v1/cluster | jq .gpusTotal)\" -gt 0 ]"
log "cluster: $(curl -s $SCHEDULER_URL/v1/cluster | jq -c '{gpusTotal,nodes}')"

PREFIX=e2e-$(date +%s)
gpuctl workload -n "$JOBS" -rate "$RATE" -runtime-scale "$SCALE" -prefix "$PREFIX" &
WL=$!
sleep 12
VICTIM=$(kubectl -n "$NS" get pods -l app=nodeagent,pool=a100 -o jsonpath='{.items[0].metadata.name}')
log "failure 1: deleting node agent pod $VICTIM (its nodes stop heartbeating)"
kubectl -n "$NS" delete pod "$VICTIM" --wait=false
sleep 25
log "failure 2: deleting the scheduler pod (restore from Postgres + Redis redelivery)"
kubectl -n "$NS" delete pod -l app=scheduler --wait=false
kill $PF 2>/dev/null || true
sleep 3
kubectl -n "$NS" rollout status deploy/scheduler --timeout=120s
kubectl -n "$NS" port-forward svc/scheduler 18080:8080 >/dev/null 2>&1 &
PF=$!
retry curl -sf $SCHEDULER_URL/readyz
log "scheduler back: $(curl -s $SCHEDULER_URL/v1/cluster | jq -c '{runningJobs,pendingByClass}')"
wait $WL
gpuctl wait -timeout 15m | tail -3

# Every submitted job must have reached Succeeded (FAIL_RATE=0).
ok=0; bad=0; pre=0; req=0
for i in $(seq -f "%05g" 0 $((JOBS-1))); do
  j=$(curl -s $SCHEDULER_URL/v1/jobs/$PREFIX-$i)
  if [ "$(echo "$j" | jq -r .state)" = "Succeeded" ]; then ok=$((ok+1)); else bad=$((bad+1)); echo "not done: $j"; fi
  pre=$((pre + $(echo "$j" | jq '.preemptions // 0')))
  req=$((req + $(echo "$j" | jq '.requeues // 0')))
done
log "jobs succeeded=$ok not_succeeded=$bad preemptions=$pre node_failure_requeues=$req"
echo "{\"jobs\":$JOBS,\"succeeded\":$ok,\"notSucceeded\":$bad,\"preemptions\":$pre,\"requeues\":$req}" > "$OUT/summary.json"
[ "$bad" -eq 0 ]
