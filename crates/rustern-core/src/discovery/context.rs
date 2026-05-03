use std::path::PathBuf;
use std::time::Duration;

use kube::Client;
use kube::config::{KubeConfigOptions, Kubeconfig};

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

/// CLI / 環境変数から kubeconfig を解決。
/// 優先順位: 引数 > KUBECONFIG(先頭パスのみ) > ~/.kube/config
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

pub async fn build_client(selector: &ContextSelector) -> Result<Client, ContextError> {
    let kubeconfig = resolve_kubeconfig(selector)?;
    let ctx_name = pick_context_name(&kubeconfig, selector)?;
    let opts = KubeConfigOptions {
        context: Some(ctx_name.to_string()),
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
}
