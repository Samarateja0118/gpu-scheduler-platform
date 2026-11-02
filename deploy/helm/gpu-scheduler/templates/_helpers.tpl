{{- define "gs.labels" -}}
app.kubernetes.io/part-of: gpu-scheduler
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "gs.image" -}}
{{ .root.Values.image.registry }}/{{ .component }}:{{ .root.Values.image.tag }}
{{- end -}}

{{- define "gs.podSecurity" -}}
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  seccompProfile: {type: RuntimeDefault}
{{- end -}}

{{- define "gs.containerSecurity" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities: {drop: [ALL]}
{{- end -}}

{{/* Reuse an existing secret value across upgrades, else generate one. */}}
{{- define "gs.secretValue" -}}
{{- $existing := lookup "v1" "Secret" .root.Release.Namespace "gpu-scheduler-secrets" -}}
{{- if .value -}}{{ .value }}
{{- else if and $existing (index $existing.data .key) -}}{{ index $existing.data .key | b64dec }}
{{- else -}}{{ randAlphaNum 32 }}{{- end -}}
{{- end -}}
