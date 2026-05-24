//! Manual soak harness: compare rstn vs stern CPU/RSS in follow mode (cluster required).

use clap::{Parser, Subcommand};
use std::io;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(
    name = "stern-compare",
    about = "Compare rstn vs stern on the same namespace/selector (manual soak; not for CI)"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<CommandKind>,

    /// Kubernetes namespace.
    #[arg(short = 'n', long = "namespace")]
    namespace: String,

    /// Label selector (e.g. app=demo).
    #[arg(short = 'l', long = "selector")]
    selector: String,

    /// Pod name regex query (default: match all).
    #[arg(default_value = ".*")]
    query: String,

    /// Soak duration in seconds.
    #[arg(long = "seconds", default_value_t = 30)]
    seconds: u64,

    /// rstn binary name or path.
    #[arg(long = "rstn", default_value = "rstn")]
    rstn: String,

    /// stern binary name or path.
    #[arg(long = "stern", default_value = "stern")]
    stern: String,
}

#[derive(Subcommand, Clone, Copy)]
enum CommandKind {
    /// Print hyperfine and time+ps shell recipes (default when no subcommand).
    Print,
    /// Run each tool locally, sample RSS with ps, print summary.
    Run,
}

#[derive(Debug, Clone)]
struct ToolSpec {
    label: &'static str,
    bin: String,
}

#[derive(Debug)]
struct SampleResult {
    label: String,
    exit_status: i32,
    peak_rss_kib: u64,
    wall: Duration,
}

fn main() {
    let cli = Cli::parse();
    let tools = [
        ToolSpec {
            label: "rstn",
            bin: cli.rstn.clone(),
        },
        ToolSpec {
            label: "stern",
            bin: cli.stern.clone(),
        },
    ];

    match cli.command.unwrap_or(CommandKind::Print) {
        CommandKind::Print => print_recipes(&cli, &tools),
        CommandKind::Run => run_comparison(&cli, &tools),
    }
}

fn follow_args(cli: &Cli) -> Vec<String> {
    vec![
        "-n".into(),
        cli.namespace.clone(),
        "-l".into(),
        cli.selector.clone(),
        cli.query.clone(),
    ]
}

fn shell_quote(s: &str) -> String {
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/' | '='))
    {
        s.to_owned()
    } else {
        format!("'{s}'")
    }
}

fn follow_shell_cmd(bin: &str, cli: &Cli) -> String {
    let args = follow_args(cli)
        .iter()
        .map(|a| shell_quote(a))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "timeout {secs}s {bin} {args} >/dev/null 2>&1",
        secs = cli.seconds
    )
}

fn print_recipes(cli: &Cli, tools: &[ToolSpec]) {
    let rstn_cmd = follow_shell_cmd(&tools[0].bin, cli);
    let stern_cmd = follow_shell_cmd(&tools[1].bin, cli);

    println!("=== stern-compare (manual soak; cluster required) ===");
    println!("namespace: {}", cli.namespace);
    println!("selector:  {}", cli.selector);
    println!("query:     {}", cli.query);
    println!("duration:  {}s", cli.seconds);
    println!();

    if which("hyperfine") {
        println!("--- hyperfine (wall time; install: https://github.com/sharkdp/hyperfine) ---");
        println!("hyperfine --warmup 1 --shell=bash \\\n  {rstn_cmd:?} \\\n  {stern_cmd:?}");
        println!();
    } else {
        eprintln!("note: hyperfine not found on PATH; skipping hyperfine recipe");
        println!();
    }

    println!("--- /usr/bin/time + ps RSS (peak resident set, KiB) ---");
    for tool in tools {
        print_time_ps_recipe(tool, cli);
        println!();
    }

    println!("--- built-in sampler ---");
    println!(
        "cargo run --release -p stern-compare -- run -n {} -l {} --seconds {} {}",
        shell_quote(&cli.namespace),
        shell_quote(&cli.selector),
        cli.seconds,
        shell_quote(&cli.query),
    );
}

fn print_time_ps_recipe(tool: &ToolSpec, cli: &Cli) {
    let cmd = follow_shell_cmd(&tool.bin, cli);
    println!("# {}", tool.label);
    println!(
        r#"(
  /usr/bin/time -l {cmd} &
  pid=$!
  peak=0
  end=$((SECONDS+{secs}))
  while [ $SECONDS -lt $end ] && kill -0 "$pid" 2>/dev/null; do
    rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')
    [ -n "$rss" ] && [ "$rss" -gt "$peak" ] && peak=$rss
    sleep 0.5
  done
  wait "$pid" 2>/dev/null || true
  echo "{label} peak RSS KiB: $peak"
)"#,
        cmd = cmd,
        secs = cli.seconds,
        label = tool.label,
    );
}

fn run_comparison(cli: &Cli, tools: &[ToolSpec]) {
    println!("=== stern-compare run ===");
    for tool in tools {
        if !which(&tool.bin) {
            eprintln!("error: {} not found on PATH", tool.bin);
            std::process::exit(1);
        }
    }

    let mut results = Vec::with_capacity(tools.len());
    for tool in tools {
        match sample_tool(tool, cli) {
            Ok(r) => results.push(r),
            Err(e) => {
                eprintln!("error sampling {}: {e}", tool.label);
                std::process::exit(1);
            }
        }
    }

    print_summary(&results);
}

fn sample_tool(tool: &ToolSpec, cli: &Cli) -> io::Result<SampleResult> {
    let mut child = Command::new(&tool.bin)
        .args(follow_args(cli))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    let pid = child.id();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(cli.seconds);
    let mut peak_rss_kib = 0_u64;

    while Instant::now() < deadline {
        if let Some(rss) = read_rss_kib(pid)? {
            peak_rss_kib = peak_rss_kib.max(rss);
        }
        if child.try_wait()?.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(500));
    }

    let _ = child.kill();
    let status = child.wait()?;
    let exit_status = status.code().unwrap_or(-1);

    Ok(SampleResult {
        label: tool.label.to_string(),
        exit_status,
        peak_rss_kib,
        wall: start.elapsed(),
    })
}

fn read_rss_kib(pid: u32) -> io::Result<Option<u64>> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()?;
    if !output.status.success() {
        return Ok(None);
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed
        .parse::<u64>()
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn print_summary(results: &[SampleResult]) {
    println!();
    println!(
        "{:<8} {:>12} {:>14} {:>8}",
        "tool", "peak RSS KiB", "wall time", "exit"
    );
    for r in results {
        println!(
            "{:<8} {:>12} {:>11.1}s {:>8}",
            r.label,
            r.peak_rss_kib,
            r.wall.as_secs_f64(),
            r.exit_status,
        );
    }
    println!();
    println!("note: exit status may be non-zero after timeout kill; compare RSS and wall time.");
}

fn which(name: &str) -> bool {
    if Path::new(name).is_file() {
        return true;
    }
    let path_var = std::env::var_os("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follow_args_include_namespace_selector_query() {
        let cli = Cli {
            command: None,
            namespace: "kube-system".into(),
            selector: "app=demo".into(),
            query: "pod-.*".into(),
            seconds: 10,
            rstn: "rstn".into(),
            stern: "stern".into(),
        };
        assert_eq!(
            follow_args(&cli),
            vec![
                "-n".to_string(),
                "kube-system".to_string(),
                "-l".to_string(),
                "app=demo".to_string(),
                "pod-.*".to_string(),
            ]
        );
    }

    #[test]
    fn shell_quote_wraps_special_chars() {
        assert_eq!(shell_quote("app=demo"), "app=demo");
        assert_eq!(shell_quote("pod-.*"), "'pod-.*'");
    }

    #[test]
    fn follow_shell_cmd_redirects_output() {
        let cli = Cli {
            command: None,
            namespace: "default".into(),
            selector: "app=x".into(),
            query: ".*".into(),
            seconds: 15,
            rstn: "rstn".into(),
            stern: "stern".into(),
        };
        let cmd = follow_shell_cmd("rstn", &cli);
        assert!(cmd.contains("timeout 15s rstn"));
        assert!(cmd.ends_with(">/dev/null 2>&1"));
    }
}
