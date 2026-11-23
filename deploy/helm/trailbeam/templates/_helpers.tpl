{{- define "trailbeam.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "trailbeam.postgresName" -}}
{{- printf "%s-postgres" (include "trailbeam.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "trailbeam.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "trailbeam.labels" -}}
{{ include "trailbeam.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "trailbeam.image" -}}
{{- printf "%s:%s" .Values.image.repository (default (printf "%s-postgres" .Chart.AppVersion) .Values.image.tag) -}}
{{- end -}}

{{/* The Secret with token and receipt-hash-key: the user's, or the chart's. */}}
{{- define "trailbeam.authSecret" -}}
{{- default (include "trailbeam.fullname" .) .Values.auth.existingSecret -}}
{{- end -}}

{{/* A value kept across upgrades: the one given, else the one already in the cluster's Secret, else a new one. */}}
{{- define "trailbeam.keep" -}}
{{- $given := index . 0 -}}{{- $secret := index . 1 -}}{{- $key := index . 2 -}}
{{- if $given -}}
{{- $given | b64enc -}}
{{- else if and $secret $secret.data (hasKey $secret.data $key) -}}
{{- index $secret.data $key -}}
{{- else -}}
{{- /* 32 random bytes as 64 hex characters, as openssl rand -hex 32 makes */ -}}
{{- randAlphaNum 64 | sha256sum | b64enc -}}
{{- end -}}
{{- end -}}

{{- define "trailbeam.validate" -}}
{{- $sources := 0 -}}
{{- if .Values.store.url }}{{ $sources = add1 $sources }}{{ end -}}
{{- if .Values.store.existingSecret }}{{ $sources = add1 $sources }}{{ end -}}
{{- if .Values.postgresql.enabled }}{{ $sources = add1 $sources }}{{ end -}}
{{- if eq $sources 0 -}}
{{- fail "\n\nTrailbeam on Kubernetes needs Postgres. Set one of:\n  --set store.url='postgresql://<user>:<password>@<host>:5432/<database>'   (a Postgres you run)\n  --set store.existingSecret=<secret>   (a Secret with the key store-url)\n  --set postgresql.enabled=true   (a trial Postgres in the cluster)" -}}
{{- end -}}
{{- if gt $sources 1 -}}
{{- fail "\n\nSet only one of store.url, store.existingSecret and postgresql.enabled." -}}
{{- end -}}
{{- if and .Values.auth.existingSecret (or .Values.auth.token .Values.auth.receiptHashKey) -}}
{{- fail "\n\nauth.existingSecret is set: put the token and the receipt-hash key in that Secret, not in auth.token / auth.receiptHashKey." -}}
{{- end -}}
{{- if and .Values.auth.token (or (lt (len .Values.auth.token) 16) (contains " " .Values.auth.token)) -}}
{{- fail "\n\nauth.token: at least 16 printable ASCII characters, no spaces (make one with: openssl rand -hex 32)." -}}
{{- end -}}
{{- end -}}

{{- define "trailbeam.containerSecurity" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities: { drop: [ALL] }
{{- end -}}
