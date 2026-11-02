#!/usr/bin/env bash
# Canary rollout end to end against a live cluster:
#   1. steady in-cluster load on the model server Service
#   2. a healthy release (v2) must be promoted
#   3. a release returning 15% errors (v3-bad) must be rolled back
#   4. a release adding 400ms latency (v4-slow) must be rolled back
# Requires kubectl, the rollout binary on PATH, and the chart installed.
set -euo pipefail
NS=${NS:-gpusched}
IMAGE=${IMAGE:-gpusched/modelserver:dev}
LOADGEN_IMAGE=${LOADGEN_IMAGE:-gpusched/loadgen:dev}
OUT=${OUT:-results/canary}
STEP=${STEP:-60s}
mkdir -p "$OUT"
log() { echo "[$(date +%T)] $*"; }

kubectl -n "$NS" delete pod canary-load --ignore-not-found --wait=true >/dev/null
kubectl -n "$NS" run canary-load --image="$LOADGEN_IMAGE" --image-pull-policy=IfNotPresent --restart=Never \
  -- -url http://modelserver:8080/v1/infer -steps 40:30m -workers 32 >/dev/null
kubectl -n "$NS" wait --for=condition=Ready pod/canary-load --timeout=120s >/dev/null
kubectl -n "$NS" port-forward svc/prometheus 19090:9090 >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true; kubectl -n "$NS" delete pod canary-load --ignore-not-found --wait=false >/dev/null' EXIT
for _ in $(seq 30); do curl -sf localhost:19090/-/ready >/dev/null && break; sleep 2; done
sleep 30  # let stable traffic build a baseline

run() {
  local name=$1 want=$2; shift 2
  log "rollout $name (expect exit $want)"
  set +e
  rollout -namespace "$NS" -image "$IMAGE" -prometheus http://localhost:19090 -steps 25,50 \
    -step-duration "$STEP" -analysis-interval 15s -window 1m -max-error-rate 0.02 -max-p95 0.25 "$@" \
    > "$OUT/$name.json" 2> "$OUT/$name.log"
  local code=$?
  set -e
  log "$name exit=$code $(jq -c '{promoted,rolledBack,reason,duration}' "$OUT/$name.json")"
  [ "$code" -eq "$want" ] || { echo "unexpected exit for $name"; exit 1; }
}

run v2-healthy 0 -set-env MODEL_VERSION=v2 -set-env FAULT_ERROR_RATE=0 -set-env FAULT_LATENCY_MS=0
stable_version=$(kubectl -n "$NS" get deploy modelserver -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="MODEL_VERSION")].value}')
[ "$stable_version" = "v2" ] || { echo "stable should be v2, is $stable_version"; exit 1; }
run v3-errors 3 -set-env MODEL_VERSION=v3-bad -set-env FAULT_ERROR_RATE=0.15 -set-env FAULT_LATENCY_MS=0
run v4-slow 3 -set-env MODEL_VERSION=v4-slow -set-env FAULT_ERROR_RATE=0 -set-env FAULT_LATENCY_MS=400
stable_version=$(kubectl -n "$NS" get deploy modelserver -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="MODEL_VERSION")].value}')
canary=$(kubectl -n "$NS" get deploy modelserver-canary -o jsonpath='{.spec.replicas}')
log "after all rollouts: stable=$stable_version canaryReplicas=$canary"
[ "$stable_version" = "v2" ] && [ "$canary" = "0" ]
# Requests and 5xx served by each track/version over the run.
q() { curl -s localhost:19090/api/v1/query --data-urlencode "query=$1" | jq -c '[.data.result[] | {track: .metric.track, version: .metric.version, value: (.value[1] | tonumber | floor)}]'; }
echo "{\"requests\": $(q 'sum by (track, version) (increase(modelserver_requests_total[30m]))'), \"errors\": $(q 'sum by (track, version) (increase(modelserver_requests_total{code=~"5.."}[30m]))')}" | tee "$OUT/requests-by-version.json"
