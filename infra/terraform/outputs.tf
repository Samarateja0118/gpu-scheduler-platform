output "kubeconfig_path" {
  value = var.create_kind_cluster ? kind_cluster.this[0].kubeconfig_path : pathexpand(var.kubeconfig_path)
}

output "namespace" {
  value = kubernetes_namespace.gpusched.metadata[0].name
}

output "release" {
  value = "${helm_release.platform.name} (revision ${helm_release.platform.metadata[0].revision})"
}
