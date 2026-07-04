# rustern

Kubernetes multi-pod and multi-container log tailer in Rust, inspired by [stern](https://github.com/stern/stern).

**rustern** (`rstn`) tails logs from many pods and containers at once. It targets **stern/kubectl parity** for day-to-day tailing, plus a small set of **rustern-plus** features (exit-on, stats, cursor reconnect). You need a working kubeconfig and permission to list/watch pods and read logs in the target cluster.

- **Users**: install, quick start, and feature notes below.
- **Contributors**: architecture, pipeline order, tests, and perf harness → [`crates/rustern-core/README.md`](crates/rustern-core/README.md).

## Install

Requires Rust **1.88+** (`rust-version` in `Cargo.toml`).

```bash
make release        # → target/release/rstn
make install        # → ~/.cargo/bin/rstn
make install-local  # → ~/.local/bin/rstn
```

Verify: `rstn --version`

Ensure `~/.cargo/bin` (or your install dir) is on `PATH`:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

Uninstall: `make uninstall`

## Quick start

Examples match `rstn --help`. Replace namespaces and workload names with your cluster.

**Follow logs for a deployment** (resolves pod label selector from the workload):

```bash
rstn deploy/my-app -n my-namespace -f
```

**Follow by label selector** (positional query omitted → implicit `.*`):

```bash
rstn -l app=nginx -n default -f
```

**One-shot logs from the last hour** (no follow):

```bash
rstn my-pod -n default --since 1h --no-follow
```

**Regex across all namespaces**:

```bash
rstn 'api.*' -A -f
```

**Tail with timestamps** (RFC3339 nano, stern default):

```bash
rstn deploy/api -n production -f -t
```

Run `rstn --help` for the full flag list (containers, since/since-time, filters, output format, and more).

User-supplied regular expressions (`--include`, `--exclude`, `--highlight`, pod query, and related flags) are limited to **1024 characters** per pattern and validated before connecting to the cluster.

## Stern alignment (spec summary)

Behavior is tracked against [stern](https://github.com/stern/stern); not every flag matches yet.

1. **Pod name query with `-l` / `--field-selector`**: when `--selector` / `-l` or `--field-selector` is set, the positional QUERY may be omitted (implicit `.*`). With `-l`, a positional `.` is still treated as `.*` in the runner (Rust regex `.` is one character).
2. **Namespaces**: repeat `-n` / `--namespace` and/or comma-separate in one value (`-n a,b`); trim, drop empties, dedupe while keeping first-seen order. `-A` / `--all-namespaces` conflicts with `-n` and ignores explicit namespaces. With neither, use the active kube context's namespace from kubeconfig (fallback `default`; kubeconfig read errors fail before run).
3. **`--max-log-requests`**: defaults match stern’s intended caps (`50` with follow / `5` with `--no-follow`) when omitted; when following, exceeding `--max-log-requests` concurrent openings ends with an error instead of blocking indefinitely.
4. **Timestamps / log window**: `-s` is a short alias for `--since` (relative duration or seconds). `--since-time` accepts an RFC3339 timestamp (kubectl parity; mutually exclusive with `--since`). `--previous` fetches logs from the previous terminated container instance. Line prefixes are **off** unless `-t` / `--timestamps` is set (bare flag → stern `default` / RFC3339 nano; `short` → `MM-DD HH:MM:SS`). `--timezone` defaults to **local** (stern parity); `utc` or an IANA zone overrides. `epoch` and `omit` are also accepted.
5. **Which containers**: repeatable `-E` / `--exclude-container` (comma-separated in one flag allowed); `--init-containers` / `--no-init-containers` and `--ephemeral-containers` / `--no-ephemeral-containers` follow stern-style defaults (both kinds included unless opted out); `--container-state running|waiting|terminated|all` limits streams using Pod container statuses (filtered modes skip containers whose status is not known yet).
6. **`-H` / `--highlight`**: merged with `-i/--include` and applied after **default formatter** lines (bold-red emphasis akin to stern); ineffective for `--format raw|json`.
7. **`--only-log-lines`**: stern suppresses +/- attach banners on stderr; rustern does not emit equivalents yet—the flag stays for parity and only logs at debug level internally.
8. **`kind/name` pod selection** (stern parity): supported kinds are `pod`, `replicationcontroller`, `service`, `daemonset`, `deployment`, `replicaset`, `statefulset`, and `job` (short aliases such as `deploy`, `rs`, `ds`, `sts`, `svc`, `rc`, `po` work too). For a **single** namespace, rustern **`GET`s** the workload and builds the label selector from its spec (including `matchExpressions`). With **multiple `-n` values**, it resolves per namespace and uses the common selector when they agree; otherwise it falls back to **`app=<name>`**. With **`-A` / `--all-namespaces`**, selectors can differ per namespace, so rustern always uses the legacy **`app=<name>`** fallback. Not supported today: `cronjob`, `horizontalpodautoscaler`, and other controller kinds stern does not list either.

## rustern-plus (not in stern)

- **`--exit-on REGEX`** (repeatable): exit code **1** on first raw log line matching any pattern (evaluated **before** `-i`/`-e`, so hidden lines still trigger exit). Useful for CI/smoke.
- **`--exit-on-level LEVEL`**: exit code **1** when classified log level is **at or above** `LEVEL` (`trace` < `debug` < `info` < `warn` < `error`). Uses `--level-key` when set; works with follow and one-shot.
- **`--stats`**: emit one human-readable runtime summary line to **stderr** every `--stats-interval` (default `30s`) without polluting stdout. Reports active log streams, forwarded lines per window, and lossy dropped lines when `--lossy` is enabled.
- **`--cursor-reconnect`**: opt-in reconnect for follow mode. rustern keeps the last seen timestamp per pod/container source in memory; if a stream disconnects, it re-opens with `sinceTime=<last_timestamp-1s overlap>` to avoid gaps at the seam. `--since` / `--since-time` / `--tail` only apply to the first open for a given pod UID, and the flag is ignored with `--no-follow`.

## Development

From the repo root:

```bash
cargo build --workspace --locked
cargo test -p rustern-core
```

**CI** (pull requests to `main`, see [`.github/workflows/ci.yml`](.github/workflows/ci.yml)):

| Job | Checks |
|-----|--------|
| `rust` | `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo build --workspace --locked`, `cargo test --workspace --locked` |
| `audit` | `cargo audit` (RustSec advisory-db) |

Internal design (runtime layers, mux/attach/watch, pipeline order, integration tests, criterion benches) lives in [`crates/rustern-core/README.md`](crates/rustern-core/README.md)—not duplicated here.

## License

[MIT](LICENSE)
