# rustern — code review guidelines (humans and CodeRabbit)

rustern is a multi-pod Kubernetes log tailer. **stern/kubectl behavioral parity** is the north star before stern-plus features.

## Do not comment on

- Formatting or style already enforced in CI (`cargo fmt --all --check`).
- Naming nits, import order, or redundant comments unless they hide a bug.
- Suggesting Japanese comments in source (English only in code).
- Test-helper ergonomics or fixture lifetime patterns when behavior is already correct and resources are intentionally kept alive.

## Prioritize

1. **Correctness** — log streaming, filtering, and CLI behavior vs stern/kubectl expectations.
2. **Async and cancellation** — tokio tasks, stream teardown, no blocking in async or stream polling paths.
3. **Kubernetes client usage** — watches, reconnects, namespace/selector handling, API errors.
4. **Resource lifecycle** — leaks, duplicate attaches, mux backpressure, unbounded in-memory caches across watch resets.
5. **Security** — credential handling, RBAC assumptions, logging sensitive data; CodeRabbit config enables secret and dependency scanning (CI runs `cargo audit`).

## Architecture boundaries

- `src/cli` owns argument parsing and validation only; keep Kubernetes, streaming, rendering, and filtering behavior out of CLI code.
- `src/run_defaults` and `src/run_config` own resolution from CLI inputs into run configuration; preserve stern/kubectl defaults and keep this layer free of async runtime orchestration.
- `src/run_resolution.rs` is the testable run-resolution seam; keep it deterministic and free of live Kubernetes access or runtime orchestration.
- `crates/rustern-core/src/discovery` owns Kubernetes object discovery and pod/container reconciliation; selector, namespace, and watch behavior should stay here rather than leaking into pipeline or render code.
- `crates/rustern-core/src/source` owns log source construction and Kubernetes log stream requests; keep per-line filtering, formatting, and presentation out of source adapters.
- `crates/rustern-core/src/pipeline` owns line transformations, filtering, classification, jq, JSON annotation, and exit-trigger decisions; avoid Kubernetes API calls or terminal rendering here.
- `crates/rustern-core/src/runtime` owns orchestration, task lifecycle, attach/mux/watch flow, cancellation, and backpressure; avoid embedding CLI parsing or display formatting policy in runtime glue.
- `crates/rustern-core/src/render` owns output formatting and terminal/color concerns; it should consume resolved events and metadata without reaching back into discovery, source, or CLI defaults.

## Review themes

General rules distilled from recurring review feedback—not implementation recipes.

### Process and config

- Keep review tooling config minimal; remove stale or non-functional settings.
- Durable review standards belong here; `.coderabbit.yaml` should only add schema settings and path-specific review deltas.
- Source comments English; review comments Japanese.
- Reference tracker issues by ID only in public repo text; use consistent branch prefixes with issue traceability.

### Parity and defaults

- Changes to CLI defaults or query resolution need regression tests for combined flags and edge cases.

### Caches and metadata

- In-memory caches must be pruned or reset at defined lifecycle boundaries.
- Require complete identity fields for Kubernetes objects; reject missing required metadata instead of silent defaults.
- Expose state through narrow module APIs; do not leak internal collection types to callers.

### Pipeline and exit behavior

- Transformation stage order can change observable exit and filter behavior; treat ordering as part of stern parity.
- Keep non-public surfaces `pub(crate)` unless expanding the API intentionally.

### Async and shared state

- Do not block in async tasks or stream polling; prefer non-blocking synchronization with bounded retry and clear logging.
- Distinguish unrecoverable lock errors from transient contention.

### Tests

- Compare unordered collections with set semantics, not slice order.
- Prefer mock API servers, `run_with_client()` seams, or validation/unit tests over live kubeconfig-dependent black-box tests.
- Long-lived fixtures and background drain tasks are often intentional—do not flag as dead code without evidence.

### Triage

- **Fix**: parity regressions, async blocking, unbounded cache growth, weak metadata validation, exit/filter semantic changes.
- **Decline with rationale**: test-helper style, intentional fixture lifetimes, behavior-neutral refactors within PR scope.

## Code style

- Keep module ownership tight; flag changes that move behavior across layers without a clear reason.
- Prefer narrow `pub(crate)` surfaces, typed configuration, and explicit `Result` errors over broad public APIs, stringly-typed plumbing, or `unwrap`/`expect` on production paths.
- Avoid drive-by refactors, compatibility shims for unshipped branch work, and comments that restate names; source comments must be English and should explain non-obvious invariants or rationale.
- Behavior-neutral refactors: prefer `pub(crate)` concrete types over premature traits; one kind of change per PR.

## Pull requests

- One kind of change per PR when possible.
- Behavior changes should cite tests, issue, or documented parity rationale.
- Keep review comments in **Japanese**; quote identifiers, types, and APIs in **English** as in the codebase.

## Merge expectations

- Do not treat CI green alone as sufficient; serious issues should be fixed or explicitly declined with rationale.
- `Review skipped` on the CodeRabbit check does not imply zero actionable inline comments—read all threads.
