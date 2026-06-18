//! In-process exec credential plugin resolution (no disk cache).

use std::collections::HashMap;
use std::process::Command;

use kube::config::{Cluster, ExecConfig, ExecInteractiveMode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecCredential {
    pub api_version: Option<String>,
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<ExecCredentialSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ExecCredentialStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecCredentialSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<ExecCredentialCluster>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interactive: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecCredentialCluster {
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate_authority_data: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub insecure_skip_tls_verify: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecCredentialStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiration_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_certificate_data: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_key_data: Option<String>,
}

fn exec_cluster_from_config(cluster: &Cluster) -> ExecCredentialCluster {
    ExecCredentialCluster {
        server: cluster.server.clone(),
        certificate_authority_data: cluster.certificate_authority_data.clone(),
        insecure_skip_tls_verify: cluster.insecure_skip_tls_verify,
    }
}

fn apply_exec_env(cmd: &mut Command, env: &Option<Vec<HashMap<String, String>>>) {
    let Some(entries) = env else {
        return;
    };
    for entry in entries {
        if let (Some(name), Some(value)) = (entry.get("name"), entry.get("value")) {
            cmd.env(name, value);
        }
    }
}

fn apply_drop_env(cmd: &mut Command, drop_env: &Option<Vec<String>>) {
    let Some(envs) = drop_env else {
        return;
    };
    for name in envs {
        cmd.env_remove(name);
    }
}

fn exec_info_json(exec: &ExecConfig, cluster: Option<&Cluster>) -> Option<String> {
    if exec.provide_cluster_info && cluster.is_none() {
        tracing::debug!("exec provideClusterInfo set but cluster info unavailable");
        return None;
    }
    let interactive = exec.interactive_mode != Some(ExecInteractiveMode::Never);
    let spec_cluster = cluster.map(exec_cluster_from_config);
    let info = ExecCredential {
        api_version: exec
            .api_version
            .clone()
            .or_else(|| Some("client.authentication.k8s.io/v1".into())),
        kind: Some("ExecCredential".into()),
        spec: Some(ExecCredentialSpec {
            cluster: spec_cluster,
            interactive: Some(interactive),
        }),
        status: None,
    };
    match serde_json::to_string(&info) {
        Ok(json) => Some(json),
        Err(e) => {
            tracing::debug!(?e, "exec credential info JSON serialize failed");
            None
        }
    }
}

/// Runs the exec credential plugin and returns token status when present.
///
/// Blocking: performs `Command::output()`; call from `spawn_blocking` or other blocking context.
pub(crate) fn run_exec_plugin(
    exec: &ExecConfig,
    cluster: Option<&Cluster>,
) -> Option<ExecCredentialStatus> {
    let command = exec.command.as_deref()?;
    let mut cmd = Command::new(command);
    if let Some(args) = &exec.args {
        cmd.args(args);
    }
    apply_exec_env(&mut cmd, &exec.env);

    let interactive = exec.interactive_mode != Some(ExecInteractiveMode::Never);
    if interactive {
        cmd.stdin(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
    } else {
        cmd.stdin(std::process::Stdio::piped());
    }

    let json = exec_info_json(exec, cluster)?;
    cmd.env("KUBERNETES_EXEC_INFO", json);
    apply_drop_env(&mut cmd, &exec.drop_env);

    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    let output = match cmd.output() {
        Ok(o) => o,
        Err(e) => {
            tracing::debug!(?e, command, "exec plugin spawn failed");
            return None;
        }
    };
    if !output.status.success() {
        tracing::debug!(
            command,
            status = ?output.status,
            stderr = %String::from_utf8_lossy(&output.stderr),
            "exec plugin exited with failure"
        );
        return None;
    }
    let cred: ExecCredential = match serde_json::from_slice(&output.stdout) {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(?e, command, "exec plugin stdout JSON parse failed");
            return None;
        }
    };
    let status = cred.status?;
    if status.token.as_deref().is_none_or(str::is_empty) {
        return None;
    }
    Some(status)
}

pub(crate) fn resolve_exec_token(exec: &ExecConfig, cluster: Option<&Cluster>) -> Option<String> {
    run_exec_plugin(exec, cluster)?.token
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    fn mark_executable(path: &Path) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn write_token_exec_script(dir: &Path, token: &str) -> PathBuf {
        let script = dir.join("token-exec.sh");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
cat <<EOF
{{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{{"token":"{token}","expirationTimestamp":"2099-01-01T00:00:00Z"}}}}
EOF
"#
            ),
        )
        .expect("write script");
        mark_executable(&script);
        script
    }

    fn write_failing_exec_script(dir: &Path) -> PathBuf {
        let script = dir.join("fail-exec.sh");
        std::fs::write(&script, "#!/bin/sh\nexit 1\n").expect("write script");
        mark_executable(&script);
        script
    }

    #[test]
    fn resolve_exec_token_returns_token_on_success() {
        let tmp = tempfile::tempdir().unwrap();
        let script = write_token_exec_script(tmp.path(), "tok-ok");
        let exec = ExecConfig {
            command: Some(script.to_string_lossy().into_owned()),
            api_version: Some("client.authentication.k8s.io/v1".into()),
            args: None,
            env: None,
            drop_env: None,
            interactive_mode: None,
            provide_cluster_info: false,
            cluster: None,
        };
        assert_eq!(resolve_exec_token(&exec, None).as_deref(), Some("tok-ok"));
    }

    #[test]
    fn resolve_exec_token_returns_none_on_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let script = write_failing_exec_script(tmp.path());
        let exec = ExecConfig {
            command: Some(script.to_string_lossy().into_owned()),
            api_version: None,
            args: None,
            env: None,
            drop_env: None,
            interactive_mode: None,
            provide_cluster_info: false,
            cluster: None,
        };
        assert!(resolve_exec_token(&exec, None).is_none());
    }

    #[test]
    fn cert_only_response_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("cert-only.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
cat <<EOF
{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{"clientCertificateData":"Y2VydA=="}}
EOF
"#,
        )
        .unwrap();
        mark_executable(&script);
        let exec = ExecConfig {
            command: Some(script.to_string_lossy().into_owned()),
            api_version: None,
            args: None,
            env: None,
            drop_env: None,
            interactive_mode: None,
            provide_cluster_info: false,
            cluster: None,
        };
        assert!(resolve_exec_token(&exec, None).is_none());
    }

    fn sample_cluster() -> Cluster {
        Cluster {
            server: Some("https://cluster.example".into()),
            insecure_skip_tls_verify: None,
            certificate_authority: None,
            certificate_authority_data: Some("Y2E=".into()),
            proxy_url: None,
            tls_server_name: None,
            disable_compression: None,
            extensions: None,
        }
    }

    #[test]
    fn provide_cluster_info_without_cluster_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let script = write_token_exec_script(tmp.path(), "tok-ok");
        let exec = ExecConfig {
            command: Some(script.to_string_lossy().into_owned()),
            api_version: Some("client.authentication.k8s.io/v1".into()),
            args: None,
            env: None,
            drop_env: None,
            interactive_mode: None,
            provide_cluster_info: true,
            cluster: None,
        };
        assert!(resolve_exec_token(&exec, None).is_none());
    }

    #[test]
    fn exec_info_env_is_always_set() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("exec-info.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
if [ -z "$KUBERNETES_EXEC_INFO" ]; then
  exit 2
fi
cat <<EOF
{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{"token":"tok-env","expirationTimestamp":"2099-01-01T00:00:00Z"}}
EOF
"#,
        )
        .unwrap();
        mark_executable(&script);
        let exec = ExecConfig {
            command: Some(script.to_string_lossy().into_owned()),
            api_version: Some("client.authentication.k8s.io/v1".into()),
            args: None,
            env: None,
            drop_env: None,
            interactive_mode: Some(ExecInteractiveMode::Never),
            provide_cluster_info: false,
            cluster: None,
        };
        assert_eq!(resolve_exec_token(&exec, None).as_deref(), Some("tok-env"));
    }

    #[test]
    fn provide_cluster_info_includes_cluster_in_exec_info() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("cluster-info.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
case "$KUBERNETES_EXEC_INFO" in
  *'"server":"https://cluster.example"'*) ;;
  *) exit 2 ;;
esac
cat <<EOF
{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{"token":"tok-cluster","expirationTimestamp":"2099-01-01T00:00:00Z"}}
EOF
"#,
        )
        .unwrap();
        mark_executable(&script);
        let exec = ExecConfig {
            command: Some(script.to_string_lossy().into_owned()),
            api_version: Some("client.authentication.k8s.io/v1".into()),
            args: None,
            env: None,
            drop_env: None,
            interactive_mode: None,
            provide_cluster_info: true,
            cluster: None,
        };
        let cluster = sample_cluster();
        assert_eq!(
            resolve_exec_token(&exec, Some(&cluster)).as_deref(),
            Some("tok-cluster")
        );
    }
}
