//! Disk cache for kubeconfig exec credential plugins (token responses only).

use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use tempfile::NamedTempFile;

use chrono::{DateTime, Utc};
use kube::config::{Cluster, ExecConfig};
use seahash::SeaHasher;
use serde::{Deserialize, Serialize};

const CACHE_SUBDIR: &str = "rustern/exec-credentials";
const EXPIRY_SKEW_SECS: i64 = 60;

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

pub(crate) fn cache_dir() -> Option<PathBuf> {
    #[cfg(test)]
    if let Some(root) = test_cache_root() {
        return Some(root.join(CACHE_SUBDIR));
    }
    dirs_next::cache_dir().map(|root| root.join(CACHE_SUBDIR))
}

#[cfg(test)]
static TEST_CACHE_ROOT: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);

#[cfg(test)]
fn test_cache_root() -> Option<PathBuf> {
    TEST_CACHE_ROOT
        .lock()
        .expect("test cache lock poisoned")
        .clone()
}

#[cfg(test)]
pub(crate) fn set_test_cache_root(root: Option<PathBuf>) {
    *TEST_CACHE_ROOT.lock().expect("test cache lock poisoned") = root;
}

pub(crate) fn cache_key(cluster_server: &str, exec: &ExecConfig) -> u64 {
    let mut h = SeaHasher::new();
    cluster_server.hash(&mut h);
    exec.api_version.hash(&mut h);
    exec.command.hash(&mut h);
    if let Some(args) = &exec.args {
        for arg in args {
            arg.hash(&mut h);
        }
    }
    if let Some(env) = &exec.env {
        for entry in env {
            let mut keys: Vec<_> = entry.keys().collect();
            keys.sort();
            for key in keys {
                key.hash(&mut h);
                entry[key].hash(&mut h);
            }
        }
    }
    h.finish()
}

fn cache_path(key: u64) -> Option<PathBuf> {
    cache_dir().map(|dir| dir.join(format!("{key:016x}")))
}

fn status_still_valid(status: &ExecCredentialStatus) -> bool {
    let Some(ts) = status.expiration_timestamp.as_deref() else {
        return false;
    };
    let Ok(expiry) = DateTime::parse_from_rfc3339(ts) else {
        return false;
    };
    let threshold = Utc::now() + chrono::Duration::seconds(EXPIRY_SKEW_SECS);
    expiry.with_timezone(&Utc) > threshold
}

pub(crate) fn read_cached_status(
    cluster_server: &str,
    exec: &ExecConfig,
) -> Option<ExecCredentialStatus> {
    let path = cache_path(cache_key(cluster_server, exec))?;
    let data = fs::read_to_string(&path).ok()?;
    let cred: ExecCredential = serde_json::from_str(&data).ok()?;
    let status = cred.status?;
    if status.token.as_deref().is_none_or(str::is_empty) {
        return None;
    }
    if !status_still_valid(&status) {
        return None;
    }
    Some(status)
}

pub(crate) fn write_cached_status(
    cluster_server: &str,
    exec: &ExecConfig,
    status: &ExecCredentialStatus,
) -> std::io::Result<()> {
    let Some(dir) = cache_dir() else {
        return Ok(());
    };
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{:016x}", cache_key(cluster_server, exec)));
    let cred = ExecCredential {
        api_version: exec
            .api_version
            .clone()
            .or_else(|| Some("client.authentication.k8s.io/v1".into())),
        kind: Some("ExecCredential".into()),
        spec: None,
        status: Some(status.clone()),
    };
    let body = serde_json::to_vec(&cred).map_err(|e| std::io::Error::other(e.to_string()))?;
    let mut tmp = NamedTempFile::new_in(&dir)?;
    tmp.write_all(&body)?;
    tmp.as_file().sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o600))?;
    }
    tmp.persist(path)?;
    Ok(())
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
    if exec.provide_cluster_info
        && let Some(cluster) = cluster
    {
        let info = ExecCredential {
            api_version: exec
                .api_version
                .clone()
                .or_else(|| Some("client.authentication.k8s.io/v1".into())),
            kind: Some("ExecCredential".into()),
            spec: Some(ExecCredentialSpec {
                cluster: Some(exec_cluster_from_config(cluster)),
                interactive: None,
            }),
            status: None,
        };
        if let Ok(json) = serde_json::to_string(&info) {
            cmd.env("KUBERNETES_EXEC_INFO", json);
        }
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

pub(crate) fn resolve_exec_token(
    cluster_server: &str,
    exec: &ExecConfig,
    cluster: Option<&Cluster>,
) -> Option<String> {
    if let Some(status) = read_cached_status(cluster_server, exec) {
        return status.token;
    }
    let status = run_exec_plugin(exec, cluster)?;
    let token = status.token.clone()?;
    if let Err(e) = write_cached_status(cluster_server, exec, &status) {
        tracing::warn!(?e, "failed to write exec credential cache");
    }
    Some(token)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    fn with_test_cache<F: FnOnce()>(f: F) {
        let tmp = TempDir::new().expect("temp cache dir");
        set_test_cache_root(Some(tmp.path().to_path_buf()));
        f();
        set_test_cache_root(None);
        let _ = tmp;
    }

    fn sample_exec(command: &Path) -> ExecConfig {
        ExecConfig {
            api_version: Some("client.authentication.k8s.io/v1".into()),
            command: Some(command.to_string_lossy().into_owned()),
            args: Some(vec!["--count".into()]),
            env: Some(vec![HashMap::from([
                ("name".into(), "PLUGIN_ENV".into()),
                ("value".into(), "plugin-value".into()),
            ])]),
            drop_env: None,
            interactive_mode: None,
            provide_cluster_info: false,
            cluster: None,
        }
    }

    fn write_fake_exec_script(dir: &Path, counter: &AtomicUsize) -> PathBuf {
        let script = dir.join("fake-exec.sh");
        let counter_path = dir.join("exec-count.txt");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
count_file="{counter_path}"
count=$(cat "$count_file" 2>/dev/null || echo 0)
count=$((count + 1))
echo "$count" > "$count_file"
cat <<EOF
{{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{{"token":"tok-$count","expirationTimestamp":"2099-01-01T00:00:00Z"}}}}
EOF
"#,
                counter_path = counter_path.display()
            ),
        )
        .expect("write script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        counter.store(0, Ordering::Relaxed);
        script
    }

    fn exec_count(dir: &Path) -> usize {
        let path = dir.join("exec-count.txt");
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }

    #[test]
    fn cache_hit_avoids_second_exec_call() {
        with_test_cache(|| {
            let tmp = tempfile::tempdir().unwrap();
            let counter = AtomicUsize::new(0);
            let script = write_fake_exec_script(tmp.path(), &counter);
            let exec = sample_exec(&script);
            let server = "https://cluster.example";

            let t1 = resolve_exec_token(server, &exec, None).expect("first token");
            let t2 = resolve_exec_token(server, &exec, None).expect("cached token");
            assert_eq!(t1, "tok-1");
            assert_eq!(t2, "tok-1");
            assert_eq!(exec_count(tmp.path()), 1);
        });
    }

    #[test]
    fn expired_cache_triggers_reexec() {
        with_test_cache(|| {
            let tmp = tempfile::tempdir().unwrap();
            let counter = AtomicUsize::new(0);
            let script = write_fake_exec_script(tmp.path(), &counter);
            let exec = sample_exec(&script);
            let server = "https://cluster.example";

            let status = ExecCredentialStatus {
                token: Some("stale".into()),
                expiration_timestamp: Some("2000-01-01T00:00:00Z".into()),
                client_certificate_data: None,
                client_key_data: None,
            };
            write_cached_status(server, &exec, &status).unwrap();

            let token = resolve_exec_token(server, &exec, None).expect("fresh token");
            assert_eq!(token, "tok-1");
            assert_eq!(exec_count(tmp.path()), 1);
        });
    }

    #[test]
    fn different_args_use_separate_cache_entries() {
        with_test_cache(|| {
            let tmp = tempfile::tempdir().unwrap();
            let counter = AtomicUsize::new(0);
            let script = write_fake_exec_script(tmp.path(), &counter);
            let exec_a = sample_exec(&script);
            let mut exec_b = sample_exec(&script);
            exec_b.args = Some(vec!["--other".into()]);
            let server = "https://cluster.example";

            resolve_exec_token(server, &exec_a, None).unwrap();
            resolve_exec_token(server, &exec_b, None).unwrap();
            assert_eq!(exec_count(tmp.path()), 2);
        });
    }

    #[test]
    fn cert_only_response_is_not_cached() {
        with_test_cache(|| {
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
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
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
            let server = "https://cert-only.example";
            assert!(resolve_exec_token(server, &exec, None).is_none());
            let key = cache_key(server, &exec);
            let path = cache_path(key).expect("cache path");
            assert!(!path.exists());
        });
    }
}
