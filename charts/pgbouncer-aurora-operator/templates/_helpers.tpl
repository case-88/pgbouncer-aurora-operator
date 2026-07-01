{{/*
Expand the chart name.
*/}}
{{- define "pgbouncer-aurora-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "pgbouncer-aurora-operator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart label.
*/}}
{{- define "pgbouncer-aurora-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "pgbouncer-aurora-operator.labels" -}}
helm.sh/chart: {{ include "pgbouncer-aurora-operator.chart" . }}
{{ include "pgbouncer-aurora-operator.selectorLabels" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "pgbouncer-aurora-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pgbouncer-aurora-operator.name" . }}
{{- end -}}

{{/*
ServiceAccount name.
*/}}
{{- define "pgbouncer-aurora-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "pgbouncer-aurora-operator.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
