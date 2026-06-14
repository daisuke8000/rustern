# rustern-core benchmarks

Criterion benches for hot-path throughput (`hot_path`) and mux forward pressure (`mux_forward`).

## Run

From the workspace root:

```bash
cargo bench -p rustern-core --bench hot_path
```

Filter a group:

```bash
cargo bench -p rustern-core --bench hot_path -- pipeline_spec_apply
```

## Baseline and regression comparison

Save a baseline (example name: `main`):

```bash
cargo bench -p rustern-core --bench hot_path -- --save-baseline main
```

Compare a branch against that baseline:

```bash
cargo bench -p rustern-core --bench hot_path -- --load-baseline main --baseline main
```

Baselines are stored under `target/criterion/`. Commit baselines only when the team explicitly wants checked-in numbers; otherwise keep them local.

## Interpreting Criterion output

- Criterion reports mean/median and confidence intervals per benchmark.
- Treat a **≥ 5% slowdown** in mean time (or throughput drop) versus the loaded baseline as a regression worth investigating before merging perf-sensitive changes.
- Small noise on fast nanosecond-scale benches is normal; rerun with `--sample-size` or more iterations if borderline.
- Compare like-for-like: same machine load, same `cargo bench` profile (`--release` by default for benches).

## CI (optional)

A lightweight pattern is to run benches on a labeled runner without failing the job, and archive `target/criterion/` as an artifact for manual diff. Failing CI on micro-regressions is usually too noisy unless the runner is dedicated and baselines are pinned.

Example (manual / nightly):

```yaml
- run: cargo bench -p rustern-core --bench hot_path -- --save-baseline ci-${{ github.sha }}
```

Regression gate (only when a stable baseline artifact exists):

```yaml
- run: cargo bench -p rustern-core --bench hot_path -- --load-baseline main --baseline main
```
