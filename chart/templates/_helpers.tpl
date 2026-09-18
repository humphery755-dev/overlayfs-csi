{{- /* chart 内共享命名规则的唯一真源（csi.yaml 与 webhook.yaml 共用） */ -}}
{{- define "overlayfs-csi.webhookSecretName" -}}
{{- printf "%s-webhook-cert" .Values.name -}}
{{- end -}}
{{- define "overlayfs-csi.webhookSvcName" -}}
{{- printf "%s-webhook" (.Values.name | replace "." "-") -}}
{{- end -}}
