use std::path::PathBuf;
use std::time::Duration;

use kube::Client;
use kube::config::{KubeConfigOptions, Kubeconfig};
use secrecy::SecretBox;

use super::exec_resolver::resolve_exec_token;

#[derive(Debug, Clone, Default)]
pub struct ContextSelector {
    pub kubeconfig_path: Option<PathBuf>,
    pub context_name: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    #[error("kubeconfig parse error: {0}")]
    Parse(String),
    #[error("context '{0}' not found in kubeconfig")]
    ContextNotFound(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to build kubernetes client: {0}")]
    Client(String),
}

/// Resolve kubeconfig path from explicit selector, env, or default location.
///
/// Precedence: `kubeconfig_path` > first entry in `KUBECONFIG` > `~/.kube/config`.
pub fn resolve_kubeconfig(selector: &ContextSelector) -> Result<Kubeconfig, ContextError> {
    let path = if let Some(p) = &selector.kubeconfig_path {
        p.clone()
    } else if let Ok(env) = std::env::var("KUBECONFIG") {
        env.split(':')
            .next()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| ContextError::Parse("empty KUBECONFIG".into()))?
    } else {
        let home = std::env::var("HOME").unwrap_or_default();
        PathBuf::from(home).join(".kube/config")
    };
    Kubeconfig::read_from(&path).map_err(|e| ContextError::Parse(e.to_string()))
}

pub fn pick_context_name<'a>(
    cfg: &'a Kubeconfig,
    selector: &'a ContextSelector,
) -> Result<&'a str, ContextError> {
    let want = selector
        .context_name
        .as_deref()
        .or(cfg.current_context.as_deref())
        .ok_or_else(|| ContextError::ContextNotFound("(none)".into()))?;
    if cfg.contexts.iter().any(|c| c.name == want) {
        Ok(want)
    } else {
        Err(ContextError::ContextNotFound(want.to_string()))
    }
}

/// Namespace for the active context: context `namespace` when set, otherwise `"default"`.
pub fn default_namespace(
    cfg: &Kubeconfig,
    selector: &ContextSelector,
) -> Result<String, ContextError> {
    let ctx_name = pick_context_name(cfg, selector)?;
    let named = cfg
        .contexts
        .iter()
        .find(|c| c.name == ctx_name)
        .ok_or_else(|| ContextError::ContextNotFound(ctx_name.to_string()))?;
    Ok(named
        .context
        .as_ref()
        .and_then(|ctx| ctx.namespace.as_deref())
        .filter(|ns| !ns.trim().is_empty())
        .unwrap_or("default")
        .to_string())
}

fn cluster_for_context<'a>(
    kubeconfig: &'a Kubeconfig,
    ctx_name: &str,
) -> Option<&'a kube::config::Cluster> {
    let named_ctx = kubeconfig.contexts.iter().find(|c| c.name == ctx_name)?;
    let cluster_name = named_ctx.context.as_ref()?.cluster.as_str();
    kubeconfig
        .clusters
        .iter()
        .find(|c| c.name == cluster_name)
        .and_then(|c| c.cluster.as_ref())
}

fn apply_exec_token_to_kubeconfig(kubeconfig: &mut Kubeconfig, ctx_name: &str) {
    let cluster = cluster_for_context(kubeconfig, ctx_name).cloned();
    let Some(server) = cluster
        .as_ref()
        .and_then(|c| c.server.as_deref())
        .map(str::to_string)
    else {
        return;
    };
    let Some(user_name) = kubeconfig
        .contexts
        .iter()
        .find(|c| c.name == ctx_name)
        .and_then(|c| c.context.as_ref())
        .and_then(|c| c.user.as_deref())
        .map(str::to_string)
    else {
        return;
    };
    let exec = kubeconfig
        .auth_infos
        .iter()
        .find(|u| u.name == user_name)
        .and_then(|u| u.auth_info.as_ref())
        .and_then(|a| a.exec.clone());
    let Some(exec) = exec else {
        return;
    };
    let Some(token) = resolve_exec_token(&exec, cluster.as_ref()) else {
        tracing::debug!(
            user = %user_name,
            server = %server,
            "exec token resolution failed; using kubeconfig exec fallback"
        );
        return;
    };
    let Some(auth_info) = kubeconfig
        .auth_infos
        .iter_mut()
        .find(|u| u.name == user_name)
        .and_then(|u| u.auth_info.as_mut())
    else {
        return;
    };
    auth_info.token = Some(SecretBox::new(token.into_boxed_str()));
    auth_info.exec = None;
}

pub async fn build_client(selector: &ContextSelector) -> Result<Client, ContextError> {
    let mut kubeconfig = resolve_kubeconfig(selector)?;
    let ctx_name = pick_context_name(&kubeconfig, selector)?.to_string();
    let ctx_for_exec = ctx_name.clone();
    kubeconfig = tokio::task::spawn_blocking(move || {
        apply_exec_token_to_kubeconfig(&mut kubeconfig, &ctx_for_exec);
        kubeconfig
    })
    .await
    .map_err(|e| ContextError::Client(format!("exec resolver task failed: {e}")))?;
    let opts = KubeConfigOptions {
        context: Some(ctx_name),
        cluster: None,
        user: None,
    };
    let mut config = kube::Config::from_custom_kubeconfig(kubeconfig, &opts)
        .await
        .map_err(|e| ContextError::Parse(e.to_string()))?;
    config.connect_timeout = Some(Duration::from_secs(30));
    Client::try_from(config).map_err(|e| ContextError::Client(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const SAMPLE: &str = r#"
apiVersion: v1
kind: Config
current-context: default
contexts:
  - name: default
    context:
      cluster: local
      user: dev
  - name: prod
    context:
      cluster: prod
      user: prod
clusters:
  - name: local
    cluster:
      server: https://localhost
  - name: prod
    cluster:
      server: https://prod.example.com
users:
  - name: dev
    user: {}
  - name: prod
    user: {}
"#;

    fn write_temp(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn resolves_explicit_path() {
        let f = write_temp(SAMPLE);
        let sel = ContextSelector {
            kubeconfig_path: Some(f.path().to_path_buf()),
            ..Default::default()
        };
        let cfg = resolve_kubeconfig(&sel).unwrap();
        assert_eq!(cfg.current_context.as_deref(), Some("default"));
    }

    #[test]
    fn picks_explicit_context() {
        let f = write_temp(SAMPLE);
        let cfg = Kubeconfig::read_from(f.path()).unwrap();
        let sel = ContextSelector {
            context_name: Some("prod".into()),
            ..Default::default()
        };
        assert_eq!(pick_context_name(&cfg, &sel).unwrap(), "prod");
    }

    #[test]
    fn errors_on_missing_context() {
        let f = write_temp(SAMPLE);
        let cfg = Kubeconfig::read_from(f.path()).unwrap();
        let sel = ContextSelector {
            context_name: Some("ghost".into()),
            ..Default::default()
        };
        let err = pick_context_name(&cfg, &sel).unwrap_err();
        assert!(matches!(err, ContextError::ContextNotFound(_)));
    }

    #[test]
    fn default_namespace_uses_context_namespace_when_set() {
        let content = r#"
apiVersion: v1
kind: Config
current-context: staging
contexts:
  - name: staging
    context:
      cluster: local
      user: dev
      namespace: team-a
clusters:
  - name: local
    cluster:
      server: https://localhost
users:
  - name: dev
    user: {}
"#;
        let f = write_temp(content);
        let cfg = Kubeconfig::read_from(f.path()).unwrap();
        let sel = ContextSelector {
            kubeconfig_path: Some(f.path().to_path_buf()),
            ..Default::default()
        };
        assert_eq!(default_namespace(&cfg, &sel).unwrap(), "team-a");
    }

    #[test]
    fn default_namespace_falls_back_to_default_when_unset() {
        let f = write_temp(SAMPLE);
        let cfg = Kubeconfig::read_from(f.path()).unwrap();
        let sel = ContextSelector {
            kubeconfig_path: Some(f.path().to_path_buf()),
            ..Default::default()
        };
        assert_eq!(default_namespace(&cfg, &sel).unwrap(), "default");
    }

    #[test]
    fn default_namespace_honors_explicit_context_flag() {
        let content = r#"
apiVersion: v1
kind: Config
current-context: default
contexts:
  - name: default
    context:
      cluster: local
      user: dev
      namespace: wrong
  - name: prod
    context:
      cluster: prod
      user: prod
      namespace: production
clusters:
  - name: local
    cluster:
      server: https://localhost
  - name: prod
    cluster:
      server: https://prod.example.com
users:
  - name: dev
    user: {}
  - name: prod
    user: {}
"#;
        let f = write_temp(content);
        let cfg = Kubeconfig::read_from(f.path()).unwrap();
        let sel = ContextSelector {
            kubeconfig_path: Some(f.path().to_path_buf()),
            context_name: Some("prod".into()),
        };
        assert_eq!(default_namespace(&cfg, &sel).unwrap(), "production");
    }
}
