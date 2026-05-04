# rustern
Kubernetes multi pod and container log tailing in Rust inspired by the original stern

## Stern alignment (spec summary)

Behavior is tracked against [stern](https://github.com/stern/stern); not every flag matches yet.

1. **Pod name query with `-l`**: when `--selector` / `-l` is set, a positional query of `.` is treated as `.*` (Rust regex `.` is one character). Optional positional when only `-l` is set is future work.
2. **Namespaces**: repeat `-n` / `--namespace` and/or comma-separate in one value (`-n a,b`); trim, drop empties, dedupe while keeping first-seen order. `-A` / `--all-namespaces` conflicts with `-n` and ignores explicit namespaces. With neither, watch `default` (kube context namespace is not used yet).
3. **`--max-log-requests`**: target stern defaults (50 with follow, 5 with `--no-follow`) and error-on-limit behavior when following; today the default is `32` with semaphore-only limiting.
