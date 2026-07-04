# rustern-core benchmarks

Criterion benches for hot-path throughput (`hot_path`) and mux forward pressure (`mux_forward`).

## Run

From the workspace root:

```bash
cargo bench -p rustern-core --features bench --bench hot_path
cargo bench -p rustern-core --features bench --bench mux_forward
```

Compile without executing (CI / quick check):

```bash
cargo bench -p rustern-core --features bench --no-run
```

Filter a group:

```bash
cargo bench -p rustern-core --features bench --bench mux_forward -- multistream_matrix
```

## Coverage matrix (DSK-99)

| Bench group | What it measures | Production gap closed |
|-------------|------------------|------------------------|
| `multistream_matrix` | `streams ∈ {1,16,128,512}` × `consumer ∈ {unbounded,slow}` × `policy ∈ {blocking,lossy}`; reports throughput and tier drop counts via `MuxMetrics` / `LossyMetrics` | Single-stream mux benches; no slow-terminal matrix |
| `cursor_tracking_ab` | Pipeline apply with vs without cursor-update channel send per line | `CursorTrackingStream` hot path not previously benchmarked |
| `render_task_sink` / `render_task_duplex` | Real `render_task` + `BufWriter` + optional `flush_ticker` | Load tests counted channel messages only |
| `mux_render_task_e2e` | mux → forward → `render_task` (sink or duplex reader) | End-to-end render writer path |

## Still requires DSK-97 (MuxForwardCore)

These paths remain **stubbed or approximated** until production-shape wiring lands:

- **Attach → `CursorTrackingStream` → mux** as a single assembled entry (benches use synthetic streams or a local `BenchCursorTrackingStream` wrapper).
- **`stats: Some(RunStats)`** on all tiers during benches (production always enables stats in `run.rs`; matrix benches use `MuxMetrics::new(None)` / `LossyMetrics::new(None)` — drop counters still work, `RunStats` interval reporting does not).
- **PodLogSource through attach semaphore** with cursor reconnect processor running.
- **Formatter matrix** (jq Replace/Append, json/extjson/raw) — tracked under separate perf issues; not in DSK-99 scope.

After DSK-97 merges, extend `mux_render_task_e2e` to call the shared assembly helper instead of hand-wired channels.

## Baseline and regression comparison

Save a baseline (example name: `main`):

```bash
cargo bench -p rustern-core --features bench --bench hot_path -- --save-baseline main
```

Compare a branch against that baseline:

```bash
cargo bench -p rustern-core --features bench --bench hot_path -- --load-baseline main --baseline main
```

Baselines are stored under `target/criterion/`. Commit baselines only when the team explicitly wants checked-in numbers; otherwise keep them local.

## Interpreting Criterion output

- Criterion reports mean/median and confidence intervals per benchmark.
- Treat a **≥ 5% slowdown** in mean time (or throughput drop) versus the loaded baseline as a regression worth investigating before merging perf-sensitive changes.
- Small noise on fast nanosecond-scale benches is normal; rerun with `--sample-size` or more iterations if borderline.
- Compare like-for-like: same machine load, same `cargo bench` profile (`--release` by default for benches).
- `multistream_matrix` with `consumer=Slow` and `streams=512` is intentionally slow; use it for backpressure characterization, not micro-regression gates.

## CI (optional)

A lightweight pattern is to run benches on a labeled runner without failing the job, and archive `target/criterion/` as an artifact for manual diff. Failing CI on micro-regressions is usually too noisy unless the runner is dedicated and baselines are pinned.

Example (manual / nightly):

```yaml
- run: cargo bench -p rustern-core --features bench --bench hot_path -- --save-baseline ci-${{ github.sha }}
```

Regression gate (only when a stable baseline artifact exists):

```yaml
- run: cargo bench -p rustern-core --features bench --bench hot_path -- --load-baseline main --baseline main
```
