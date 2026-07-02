{{- define "scheduler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "scheduler.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "scheduler.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "scheduler.labels" -}}
app.kubernetes.io/name: {{ include "scheduler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "scheduler.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "scheduler.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
