# Provisions a local Kubernetes cluster (kind) and deploys the platform
# with Helm. With create_kind_cluster = false it deploys into an existing
# cluster instead, which is how it was exercised locally on k3s.

resource "kind_cluster" "this" {
  count          = var.create_kind_cluster ? 1 : 0
  name           = var.cluster_name
  node_image     = var.kind_node_image
  wait_for_ready = true

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"
    }

    dynamic "node" {
      for_each = range(var.kind_workers)
      content {
        role = "worker"
        labels = {
          "gpusched.dev/role" = "worker"
        }
      }
    }
  }
}

locals {
  kind      = var.create_kind_cluster ? kind_cluster.this[0] : null
  host      = var.create_kind_cluster ? local.kind.endpoint : null
  ca        = var.create_kind_cluster ? local.kind.cluster_ca_certificate : null
  cert      = var.create_kind_cluster ? local.kind.client_certificate : null
  key       = var.create_kind_cluster ? local.kind.client_key : null
  cfg_path  = var.create_kind_cluster ? null : pathexpand(var.kubeconfig_path)
  chart_dir = "${path.module}/../../deploy/helm/gpu-scheduler"
}

provider "kubernetes" {
  host                   = local.host
  cluster_ca_certificate = local.ca
  client_certificate     = local.cert
  client_key             = local.key
  config_path            = local.cfg_path
}

provider "helm" {
  kubernetes {
    host                   = local.host
    cluster_ca_certificate = local.ca
    client_certificate     = local.cert
    client_key             = local.key
    config_path            = local.cfg_path
  }
}

resource "kubernetes_namespace" "gpusched" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/part-of" = "gpu-scheduler"
      # Enforce the restricted Pod Security Standard where the chart allows it.
      "pod-security.kubernetes.io/warn" = "restricted"
    }
  }
}

resource "helm_release" "platform" {
  name      = "gs"
  namespace = kubernetes_namespace.gpusched.metadata[0].name
  chart     = local.chart_dir
  wait      = true
  timeout   = 600

  values = [for f in var.extra_values_files : file(f)]

  set {
    name  = "image.registry"
    value = var.image_registry
  }
  set {
    name  = "image.tag"
    value = var.image_tag
  }
}
