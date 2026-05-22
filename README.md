# rustern
Kubernetes multi pod and container log tailing in Rust inspired by the original stern

## Stern alignment (spec summary)

Behavior is tracked against [stern](https://github.com/stern/stern); not every flag matches yet.

1. **Pod name query with `-l`**: when `--selector` / `-l` is set, a positional query of `.` is treated as `.*` (Rust regex `.` is one character). Optional positional when only `-l` is set is future work.
2. **Namespaces**: repeat `-n` / `--namespace` and/or comma-separate in one value (`-n a,b`); trim, drop empties, dedupe while keeping first-seen order. `-A` / `--all-namespaces` conflicts with `-n` and ignores explicit namespaces. With neither, watch `default` (kube context namespace is not used yet).
3. **`--max-log-requests`**: defaults match stern’s intended caps (`50` with follow / `5` with `--no-follow`) when omitted; when following, exceeding `--max-log-requests` concurrent openings ends with an error instead of blocking indefinitely.
4. **Timestamps / `--since`**: `-s` is a short alias for `--since`. Line prefixes are **off** unless `-t` / `--timestamps` is set (bare flag → stern `default` / RFC3339 nano; `short` → `MM-DD HH:MM:SS`). `--timezone` defaults to **local** (stern parity); `utc` or an IANA zone overrides. `epoch` and `omit` are also accepted.
5. **Which containers**: repeatable `-E` / `--exclude-container` (comma-separated in one flag allowed); `--init-containers` / `--no-init-containers` and `--ephemeral-containers` / `--no-ephemeral-containers` follow stern-style defaults (both kinds included unless opted out); `--container-state running|waiting|terminated|all` limits streams using Pod container statuses (filtered modes skip containers whose status is not known yet).
6. **`-H` / `--highlight`**: merged with `-i/--include` and applied after **default formatter** lines (bold-red emphasis akin to stern); ineffective for `--format raw|json`.
7. **`--only-log-lines`**: stern suppresses +/- attach banners on stderr; rustern does not emit equivalents yet—the flag stays for parity and only logs at debug level internally.
8. **`kind/name` pod selection**: for a single namespace (`-n` exactly once when not `-A`), rustern resolves list/watch selectors with **`GET`** of the workload (`deploy/api`, …). With `-A` or multiple namespaces, selectors can differ per namespace, so rustern falls back to the legacy **`app=<name>`** string.

