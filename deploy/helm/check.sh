#!/usr/bin/env bash
# Check the Helm chart on a kind cluster (QV-173): lint, install with the trial Postgres, one span with the token
# through the Service, the span in Postgres, then an upgrade that keeps the generated token and the data.
# CI runs it (.github/workflows/ci.yml, job "kubernetes"). Locally, from the repository root:
#
#   docker build --build-arg TRAILBEAM_EXTRAS=server,postgres -t trailbeam-k8s:check .
#   kind create cluster --name tb-check
#   deploy/helm/check.sh tb-check trailbeam-k8s:check
#   kind delete cluster --name tb-check && docker image rm trailbeam-k8s:check
#
# Needs kind, kubectl (pointed at that cluster) and helm 3 or 4 on the PATH.
set -euo pipefail

cluster=${1:?usage: check.sh <kind cluster> <image:tag>}
image=${2:?usage: check.sh <kind cluster> <image:tag>}
here=$(cd "$(dirname "$0")" && pwd)
chart=$here/trailbeam
root=$(cd "$here/../.." && pwd)
img=(--set "image.repository=${image%:*}" --set "image.tag=${image##*:}" --set image.pullPolicy=Never)

step() { printf '\n== %s\n' "$*"; }
same() {  # same <got> <want> <what>
  if [ "$1" != "$2" ]; then echo "FAIL: $3: got '$1', want '$2'"; exit 1; fi
  echo "ok: $3"
}

step "lint: the three ways to give it Postgres"
helm lint --strict "$chart" --set postgresql.enabled=true
helm lint --strict "$chart" --set store.url=postgresql://u:p@db:5432/trailbeam --set ingress.enabled=true
helm lint --strict "$chart" --set store.existingSecret=pg --set auth.existingSecret=tb

step "the chart refuses no Postgres, two at once, and a short token"
for bad in "" "--set store.url=postgresql://u:p@db/t --set postgresql.enabled=true" \
           "--set postgresql.enabled=true --set auth.token=short"; do
  # shellcheck disable=SC2086
  if helm template t "$chart" $bad >/dev/null 2>&1; then echo "FAIL: rendered with: ${bad:-no settings}"; exit 1; fi
  echo "ok: refused: ${bad:-no settings}"
done

step "install, with the trial Postgres"
kind load docker-image "$image" --name "$cluster"
helm install tb "$chart" --set postgresql.enabled=true "${img[@]}" --wait --timeout 5m
kubectl create configmap tb-check --from-file="$root/python/smoke/team_check.py"

# One pod in the cluster runs team_check.py against the Service, with the token from the chart's Secret.
check() {  # check <send|check> <message id>
  local job=tb-check-$1-$RANDOM
  kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata: { name: $job }
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: check
          image: $image
          imagePullPolicy: Never
          command: [python, /check/team_check.py, "http://tb-trailbeam:4318", "$1", "$2"]
          env:
            - name: TRAILBEAM_TOKEN
              valueFrom: { secretKeyRef: { name: tb-trailbeam, key: token } }
          volumeMounts: [{ name: check, mountPath: /check }]
      volumes: [{ name: check, configMap: { name: tb-check } }]
EOF
  if ! kubectl wait --for=condition=complete "job/$job" --timeout=180s; then
    kubectl logs "job/$job" || true
    echo "FAIL: $job"; exit 1
  fi
  kubectl logs "job/$job"
}
psql() { kubectl exec tb-trailbeam-postgres-0 -- psql -U trailbeam -tAc "$1"; }

step "one span with the token, through the Service"
check send k8s-check-1
same "$(psql "SELECT count(*) FROM spans WHERE mid = 'k8s-check-1'")" 1 "the span is in Postgres"
same "$(psql 'SHOW max_locks_per_transaction')" 256 "max_locks_per_transaction"

step "helm upgrade keeps the generated token and the data"
token=$(kubectl get secret tb-trailbeam -o jsonpath='{.data.token}')
helm upgrade tb "$chart" --set postgresql.enabled=true "${img[@]}" --set retention=14d --wait --timeout 5m
same "$(kubectl get secret tb-trailbeam -o jsonpath='{.data.token}')" "$token" "the token is the same after the upgrade"
kubectl rollout status deployment/tb-trailbeam --timeout=180s
check check k8s-check-1

step "all checks passed"
