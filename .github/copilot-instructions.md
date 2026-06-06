# rustern — code review guidelines (humans and CodeRabbit)

rustern is a multi-pod Kubernetes log tailer. **stern/kubectl behavioral parity** is the north star before stern-plus features.

## Do not comment on

- Formatting or style already enforced in CI (`cargo fmt --check`).
- Naming nits, import order, or redundant comments unless they hide a bug.
- Suggesting Japanese comments in source (English only in code).

## Prioritize

1. **Correctness** — log streaming, filtering, and CLI behavior vs stern/kubectl expectations.
2. **Async and cancellation** — tokio tasks, stream teardown, no blocking in async paths.
3. **Kubernetes client usage** — watches, reconnects, namespace/selector handling, API errors.
4. **Resource lifecycle** — leaks, duplicate attaches, mux backpressure.
5. **Security** — credential handling, RBAC assumptions, logging sensitive data; CodeRabbit config enables secret and dependency scanning (CI runs `cargo audit`).

## Architecture boundaries

- `src/cli` owns argument parsing and validation only; keep Kubernetes, streaming, rendering, and filtering behavior out of CLI code.
- `src/run_defaults` and `src/run_config` own resolution from CLI inputs into run configuration; preserve stern/kubectl defaults and keep this layer free of async runtime orchestration.
- `crates/rustern-core/src/discovery` owns Kubernetes object discovery and pod/container reconciliation; selector, namespace, and watch behavior should stay here rather than leaking into pipeline or render code.
- `crates/rustern-core/src/source` owns log source construction and Kubernetes log stream requests; keep per-line filtering, formatting, and presentation out of source adapters.
- `crates/rustern-core/src/pipeline` owns line transformations, filtering, classification, jq, JSON annotation, and exit-trigger decisions; avoid Kubernetes API calls or terminal rendering here.
- `crates/rustern-core/src/runtime` owns orchestration, task lifecycle, attach/mux/watch flow, cancellation, and backpressure; avoid embedding CLI parsing or display formatting policy in runtime glue.
- `crates/rustern-core/src/render` owns output formatting and terminal/color concerns; it should consume resolved events and metadata without reaching back into discovery, source, or CLI defaults.

## Code style

- Keep module ownership tight; flag changes that move behavior across layers without a clear reason.
- Prefer narrow `pub(crate)` surfaces, typed configuration, and explicit `Result` errors over broad public APIs, stringly-typed plumbing, or `unwrap`/`expect` on production paths.
- Avoid drive-by refactors, compatibility shims for unshipped branch work, and comments that restate names; source comments must be English and should explain non-obvious invariants or rationale.
- For public pull requests, reference Linear only by issue ID (e.g., `DSK-123`); do not include Linear URLs.
- Branch naming for feature work should follow `feat/dsk-<id>-<description>` to preserve issue traceability.

## Pull requests

- One kind of change per PR when possible.
- Behavior changes should cite tests, issue, or documented parity rationale.
- Keep review comments in **Japanese**; quote identifiers, types, and APIs in **English** as in the codebase.

## Merge expectations

- Do not treat CI green alone as sufficient; serious issues should be fixed or explicitly declined with rationale.
