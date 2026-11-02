#!/usr/bin/env bash
# Step load against the model server and record what the HPA does.
set -euo pipefail
NS=${NS:-gpusched}
LOADGEN_IMAGE=${LOADGEN_IMAGE:-gpusched/loadgen:dev}
STEPS=${STEPS:-30:90s,90:90s,180:150s,30:150s}
OUT=${OUT:-results/hpa}
mkdir -p "$OUT"
kubectl -n "$NS" delete pod hpa-load --ignore-not-found --wait=true >/dev/null
kubectl -n "$NS" run hpa-load --image="$LOADGEN_IMAGE" --image-pull-policy=IfNotPresent --restart=Never \
  -- -url http://modelserver:8080/v1/infer -steps "$STEPS" -workers 64 >/dev/null
echo "t_seconds,replicas_current,replicas_desired,cpu_percent_of_request" > "$OUT/hpa.csv"
t0=$(date +%s)
while :; do
  phase=$(kubectl -n "$NS" get pod hpa-load -o jsonpath='{.status.phase}')
  h=$(kubectl -n "$NS" get hpa modelserver -o json)
  echo "$(( $(date +%s) - t0 )),$(echo "$h" | jq '.status.currentReplicas'),$(echo "$h" | jq '.status.desiredReplicas'),$(echo "$h" | jq '.status.currentMetrics[0].resource.current.averageUtilization // "null"')" >> "$OUT/hpa.csv"
  [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ] && break
  sleep 10
done
kubectl -n "$NS" logs hpa-load > "$OUT/loadgen.jsonl"
kubectl -n "$NS" delete pod hpa-load --wait=false >/dev/null
cat "$OUT/loadgen.jsonl"
cat "$OUT/hpa.csv"
