use regex::Regex;
use rustern_core::{ContextSelector, TimestampZone};

use super::Cli;

impl Cli {
    /// Build [`ContextSelector`] from global kube config flags.
    #[must_use]
    pub fn context_selector(&self) -> ContextSelector {
        ContextSelector {
            kubeconfig_path: self.kubeconfig.clone(),
            context_name: self.context.clone(),
        }
    }

    /// Resolve follow vs one-shot mode from `-f` / `--no-follow`.
    #[must_use]
    pub fn follow(&self) -> bool {
        self.follow_short || !self.no_follow
    }

    /// Cheap validation for numeric and regex flags before hitting the cluster.
    pub fn validate(&self) -> Result<(), String> {
        if self.tail.is_some_and(|v| v < 0) {
            return Err("--tail must be >= 0".into());
        }
        if let Some(s) = &self.since {
            parse_since(s)?;
        }
        if let Some(s) = &self.since_time {
            rustern_core::source::pod_log::parse_since_time(s)?;
        }
        if self.buffer_size == 0 {
            return Err("--buffer-size must be > 0".into());
        }
        if let Some(n) = self.max_log_requests
            && n == 0
        {
            return Err("--max-log-requests must be > 0 when set".into());
        }
        if let Some(ref z) = self.timezone {
            TimestampZone::parse_arg(z)?;
        }
        if self.no_init_containers && self.init_containers == Some(true) {
            return Err(
                "`--no-init-containers` conflicts with an explicit `--init-containers=true`".into(),
            );
        }
        if self.no_ephemeral_containers && self.ephemeral_containers == Some(true) {
            return Err(
                "`--no-ephemeral-containers` conflicts with `--ephemeral-containers=true`".into(),
            );
        }
        if self.no_pod_colors && self.pod_colors == Some(true) {
            return Err("`--no-pod-colors` conflicts with an explicit `--pod-colors=true`".into());
        }
        if self.no_container_colors && self.container_colors == Some(true) {
            return Err(
                "`--no-container-colors` conflicts with an explicit `--container-colors=true`"
                    .into(),
            );
        }
        for p in &self.exclude_pod {
            Regex::new(p).map_err(|e| format!("invalid --exclude-pod regex: {e}"))?;
        }
        for p in &self.exclude_container {
            Regex::new(p).map_err(|e| format!("invalid --exclude-container regex: {e}"))?;
        }
        for p in &self.include {
            Regex::new(p).map_err(|e| format!("invalid --include regex: {e}"))?;
        }
        for p in &self.exclude {
            Regex::new(p).map_err(|e| format!("invalid --exclude regex: {e}"))?;
        }
        Regex::new(&self.container).map_err(|e| format!("invalid container regex: {e}"))?;
        for p in &self.highlight {
            Regex::new(p).map_err(|e| format!("invalid --highlight regex: {e}"))?;
        }
        for p in &self.exit_on {
            Regex::new(p).map_err(|e| format!("invalid --exit-on regex: {e}"))?;
        }
        if let Some(ref expr) = self.json_query {
            rustern_core::validate_filter(expr).map_err(|e| e.to_string())?;
        }
        if let Some(ref lv) = self.exit_on_level {
            rustern_core::pipeline::ExitOnLevel::parse(lv)?;
        }
        if self.condition.is_some() && self.follow() && self.tail != Some(0) {
            return Err("--condition only works with --no-follow or --tail=0".into());
        }
        if let Some(ref c) = self.condition {
            rustern_core::discovery::pod_condition::parse_pod_condition(c)
                .map_err(|e| e.to_string())?;
        }
        crate::run_defaults::resolved_pod_query(self)?;
        crate::run_defaults::resolved_namespaces(self)?;
        Ok(())
    }
}

/// Parse `--since` as a humantime duration or a non‑negative integer (seconds).
pub(crate) fn parse_since(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty --since".into());
    }
    if let Ok(d) = humantime::parse_duration(s) {
        let secs = d.as_secs();
        return i64::try_from(secs).map_err(|_| "--since duration too large".to_string());
    }
    let n: i64 = s
        .parse()
        .map_err(|_| format!("invalid --since (expected duration or seconds): {s}"))?;
    if n < 0 {
        return Err("--since must be >= 0".into());
    }
    Ok(n)
}
