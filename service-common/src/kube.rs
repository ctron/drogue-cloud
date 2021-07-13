pub fn namespace() -> Option<String> {
    namespace_from_env().or_else(namespace_from_cluster)
}

/// Try getting the namespace from the environment variables
fn namespace_from_env() -> Option<String> {
    std::env::var_os("NAMESPACE").and_then(|s| s.to_str().map(|s| s.to_string()))
}

/// Try getting the namespace from the cluster configuration
fn namespace_from_cluster() -> Option<String> {
    kube::Config::from_cluster_env()
        .ok()
        .map(|cfg| cfg.default_namespace)
}
