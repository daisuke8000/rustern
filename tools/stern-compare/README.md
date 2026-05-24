# stern-compare

Manual soak harness for comparing **rstn** and **stern** on the same namespace/selector in follow mode. Not run in CI — requires a live cluster and both binaries on `PATH`.

## Prerequisites

- `rstn` built (`cargo build --release` or `make release`)
- [stern](https://github.com/stern/stern) installed
- `kubectl` context pointing at a cluster with matching pods
- Optional: [hyperfine](https://github.com/sharkdp/hyperfine) for scripted wall-clock comparison

## Quick start

From the repository root:

```bash
cargo build --release -p stern-compare
cargo run -p stern-compare -- print -n default -l app=demo --seconds 30
```

Copy the printed shell snippets, or run the built-in sampler:

```bash
cargo run --release -p stern-compare -- run -n default -l app=demo --seconds 30
```

## Commands

| Subcommand | Purpose |
|------------|---------|
| `print` (default) | Emit `hyperfine` and `/usr/bin/time` + `ps` recipes for manual runs |
| `run` | Run each tool for `--seconds`, sample RSS via `ps`, print a summary table |

Shared flags:

- `-n` / `--namespace` — Kubernetes namespace (required)
- `-l` / `--selector` — label selector, e.g. `app=demo` (required)
- `[QUERY]` — optional pod name regex (default `.*`)
- `--seconds` — soak duration (default `30`)
- `--rstn` / `--stern` — binary names (default `rstn` / `stern`)

## What gets compared

Both tools receive the same follow-mode invocation:

```text
<tool> -n <namespace> -l <selector> [query]
```

Stdout and stderr are discarded so measurement focuses on client CPU/RSS, not terminal rendering.

## hyperfine (optional)

When `hyperfine` is on `PATH`, `print` emits a command like:

```bash
hyperfine --warmup 1 --shell=bash \
  'timeout 30s rstn -n default -l app=demo ".*" >/dev/null 2>&1' \
  'timeout 30s stern -n default -l app=demo ".*" >/dev/null 2>&1'
```

Adjust namespace, selector, and duration to match your workload.

## `/usr/bin/time` + RSS (fallback)

`print` also emits a small bash loop that samples peak RSS with `ps` while each tool runs under `timeout`. `run` automates the same sampling.

## Recording results

Post a short note to [Issue #65](https://github.com/daisuke8000/rustern/issues/65) with cluster size (pod/container count), command line, and peak RSS / wall time for each tool.
