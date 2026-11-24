#!/usr/bin/env bash
# Check the Helm chart on a kind cluster (QV-173): lint, install with the trial Postgres, one span with the token
# through the Service, the span in Postgres, then an upgrade that keeps the generated token and the data.
# The release is named trailbeam, as the docs and values.yaml suggest: its Service is then named trailbeam too,
# whose service-link variables (TRAILBEAM_PORT=tcp://...) crash-looped the server in 0.4.0 (QV-291).
# The Service, the PDB and the Deployment must reach the server pods only, not the trial Postgres pod, and the
# documented `kubectl port-forward service/...` must work (QV-342); a release installed by the 0.4.0 chart must
# upgrade to this one (the chart from the tag v0.4.0, fetched if missing).
# CI runs it (.github/workflows/ci.yml, job "kubernetes"). Locally, from the repository root:
#
#   docker build --build-arg TRAILBEAM_EXTRAS=server,postgres -t trailbeam-k8s:check .
#   kind create cluster --name tb-check
#   deploy/helm/check.sh tb-check trailbeam-k8s:check
#   kind delete cluster --name tb-check && docker image rm trailbeam-k8s:check
#
# Needs kind, kubectl (pointed at that cluster), helm 3 or 4, git and curl on the PATH.
set -euo pipefail

cluster=${1:?usage: check.sh <kind cluster> <image:tag>}
image=${2:?usage: check.sh <kind cluster> <image:tag>}
here=$(cd "$(dirname "$0")" && pwd)
chart=$here/trailbeam
root=$(cd "$here/../.." && pwd)
rel=trailbeam   # the release, and with it the Deployment, the Service and the Secret (trailbeam.fullname)
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
helm install "$rel" "$chart" --set postgresql.enabled=true "${img[@]}" --wait --timeout 5m
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
          command: [python, /check/team_check.py, "http://$rel:4318", "$1", "$2"]
          env:
            - name: TRAILBEAM_TOKEN
              valueFrom: { secretKeyRef: { name: $rel, key: token } }
          volumeMounts: [{ name: check, mountPath: /check }]
      volumes: [{ name: check, configMap: { name: tb-check } }]
EOF
  if ! kubectl wait --for=condition=complete "job/$job" --timeout=180s; then
    kubectl logs "job/$job" || true
    echo "FAIL: $job"; exit 1
  fi
  kubectl logs "job/$job"
}
psql() { kubectl exec "$rel-postgres-0" -- psql -U trailbeam -tAc "$1"; }

# The pods behind the Service, the PDB and the Deployment are the server's, never the Postgres pod (QV-342).
servers_only() {  # servers_only <namespace> <release> [deployment too: yes|no]
  local ns=$1 r=$2 n
  n=$(kubectl -n "$ns" get deployment "$r" -o jsonpath='{.spec.replicas}')
  same "$(kubectl -n "$ns" get endpointslices -l "kubernetes.io/service-name=$r" \
          -o jsonpath='{range .items[*].endpoints[*]}{.targetRef.name}{"\n"}{end}' | grep -vc -- "-postgres-" || true)" \
       "$n" "$ns/$r: the Service's endpoints are the $n server pods"
  same "$(kubectl -n "$ns" get endpointslices -l "kubernetes.io/service-name=$r" \
          -o jsonpath='{range .items[*].endpoints[*]}{.targetRef.name}{"\n"}{end}' | grep -c -- "-postgres-" || true)" \
       0 "$ns/$r: the Postgres pod is not behind the Service"
  same "$(kubectl -n "$ns" get pdb "$r" -o jsonpath='{.status.expectedPods}')" "$n" "$ns/$r: the PDB counts the $n server pods only"
  if [ "${3:-yes}" = yes ]; then
    local sel
    sel=$(kubectl -n "$ns" get deployment "$r" -o jsonpath='{.spec.selector.matchLabels}' \
          | python3 -c 'import json,sys; print(",".join(f"{k}={v}" for k,v in json.load(sys.stdin).items()))')
    same "$(kubectl -n "$ns" get pods -l "$sel" --no-headers | grep -c -- "-postgres-" || true)" 0 \
         "$ns/$r: the Deployment's selector leaves out the Postgres pod"
    # kubectl logs deployment/... picks one of the pods that selector matches
    local out
    out=$(kubectl -n "$ns" logs "deployment/$r" --tail 1 2>&1 >/dev/null || true)
    case $out in *-postgres-*) echo "FAIL: $ns/$r: kubectl logs deployment/$r used the Postgres pod: $out"; exit 1;; esac
    echo "ok: $ns/$r: kubectl logs deployment/$r reads a server pod"
  fi
}

# The documented check: kubectl port-forward service/<release> 4318:4318, then /api/health. A free local port
# instead of 4318, as runners share one machine.
port_forward() {  # port_forward <namespace> <release>
  local ns=$1 r=$2 log pid port="" i
  log=$(mktemp)
  kubectl -n "$ns" port-forward "service/$r" :4318 >"$log" 2>&1 & pid=$!
  for i in $(seq 1 30); do
    port=$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) .*/\1/p' "$log" | head -1)
    [ -n "$port" ] && break
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if [ -z "$port" ] || ! curl -fsS --retry 5 --retry-connrefused --retry-delay 1 "http://127.0.0.1:$port/api/health" >/dev/null; then
    kill "$pid" 2>/dev/null || true; cat "$log"; rm -f "$log"
    echo "FAIL: $ns/$r: kubectl port-forward service/$r"; exit 1
  fi
  kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; rm -f "$log"
  echo "ok: $ns/$r: kubectl port-forward service/$r reaches /api/health"
}

step "no service-link variables in the server's environment (QV-291)"
pod=$(kubectl get pod -l "app.kubernetes.io/instance=$rel,app.kubernetes.io/component=server" -o jsonpath='{.items[0].metadata.name}')
same "$(kubectl exec "$pod" -c trailbeam -- printenv TRAILBEAM_PORT)" 4318 "TRAILBEAM_PORT in the pod"
same "$(kubectl exec "$pod" -c trailbeam -- printenv TRAILBEAM_SERVICE_HOST || true)" "" "no TRAILBEAM_SERVICE_HOST"

step "the Service, the PDB and the Deployment reach the server pods only; the documented port-forward works (QV-342)"
servers_only default "$rel"
port_forward default "$rel"

step "one span with the token, through the Service"
check send k8s-check-1
same "$(psql "SELECT count(*) FROM spans WHERE mid = 'k8s-check-1'")" 1 "the span is in Postgres"
same "$(psql 'SHOW max_locks_per_transaction')" 256 "max_locks_per_transaction"

step "helm upgrade keeps the generated token and the data"
token=$(kubectl get secret "$rel" -o jsonpath='{.data.token}')
helm upgrade "$rel" "$chart" --set postgresql.enabled=true "${img[@]}" --set retention=14d --wait --timeout 5m
same "$(kubectl get secret "$rel" -o jsonpath='{.data.token}')" "$token" "the token is the same after the upgrade"
kubectl rollout status "deployment/$rel" --timeout=180s
check check k8s-check-1

step "a release installed by the 0.4.0 chart upgrades to this one (QV-342)"
# Release tb (Deployment and Service tb-trailbeam) in a namespace of its own: the 0.4.0 chart crash-loops under the name trailbeam (QV-291).
old=$(mktemp -d)
if ! git -C "$root" rev-parse -q --verify "refs/tags/v0.4.0" >/dev/null; then
  git -C "$root" fetch -q --depth 1 origin tag v0.4.0
fi
git -C "$root" archive v0.4.0 deploy/helm/trailbeam | tar -x -C "$old"
kubectl create namespace tb-old
helm install tb "$old/deploy/helm/trailbeam" -n tb-old --set postgresql.enabled=true "${img[@]}" --wait --timeout 5m
helm upgrade tb "$chart" -n tb-old --set postgresql.enabled=true "${img[@]}" --wait --timeout 5m
kubectl -n tb-old rollout status deployment/tb-trailbeam --timeout=180s
# The Deployment keeps its 0.4.0 selector (it can't change in place), so kubectl logs deployment/tb-trailbeam may still
# pick Postgres there; the Service and the PDB are fixed.
servers_only tb-old tb-trailbeam no
port_forward tb-old tb-trailbeam

step "the optional one-time step for such a release: a Deployment with the new selector (CHANGELOG, 0.4.1)"
kubectl -n tb-old delete deployment tb-trailbeam --cascade=orphan
helm upgrade tb "$chart" -n tb-old --set postgresql.enabled=true "${img[@]}" --wait --timeout 5m
kubectl -n tb-old rollout status deployment/tb-trailbeam --timeout=180s
kubectl -n tb-old delete replicaset -l 'app.kubernetes.io/instance=tb,app.kubernetes.io/name=trailbeam,!app.kubernetes.io/component'
same "$(kubectl -n tb-old get rs -l app.kubernetes.io/instance=tb,app.kubernetes.io/component=server \
        -o jsonpath='{range .items[?(@.spec.replicas>0)]}{.metadata.ownerReferences[0].name}{"\n"}{end}' | sort -u)" \
     tb-trailbeam "the server's ReplicaSet belongs to the new Deployment"
servers_only tb-old tb-trailbeam
port_forward tb-old tb-trailbeam
helm uninstall tb -n tb-old --wait --timeout 3m
kubectl delete namespace tb-old --wait=false
rm -rf "$old"

step "all checks passed"
