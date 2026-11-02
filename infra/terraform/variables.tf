variable "create_kind_cluster" {
  description = "Create a local kind cluster. Set false to deploy into an existing cluster from kubeconfig_path."
  type        = bool
  default     = true
}

variable "cluster_name" {
  type    = string
  default = "gpusched"
}

variable "kind_node_image" {
  type    = string
  default = "kindest/node:v1.31.0"
}

variable "kind_workers" {
  description = "Worker nodes in the kind cluster."
  type        = number
  default     = 2
  validation {
    condition     = var.kind_workers >= 0 && var.kind_workers <= 5
    error_message = "Keep the local cluster between 0 and 5 workers."
  }
}

variable "kubeconfig_path" {
  description = "Used when create_kind_cluster is false."
  type        = string
  default     = "~/.kube/config"
}

variable "namespace" {
  type    = string
  default = "gpusched"
}

variable "image_registry" {
  type    = string
  default = "ghcr.io/samarateja0118/gpu-scheduler-platform"
}

variable "image_tag" {
  type    = string
  default = "latest"
}

variable "extra_values_files" {
  description = "Additional Helm values files, e.g. ../../deploy/helm/values-local.yaml."
  type        = list(string)
  default     = []
}
