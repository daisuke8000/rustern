# rustern — code review guidelines (humans and CodeRabbit)

rustern is a multi-pod Kubernetes log tailer. **stern/kubectl behavioral parity** is the north star before stern-plus features.

## Do not comment on

- Formatting or style already enforced in CI (`cargo fmt --check`).
- Naming nits, import order, or redundant comments unless they hide a bug.
- Suggesting Japanese comments in source (English only in code).
- Optional `TestOrchestratorBuilder` sugar or post-build test fixture mutation when behavior is already correct.

## Prioritize

1. **Correctness** — log streaming, filtering, and CLI behavior vs stern/kubectl expectations.
2. **Async and cancellation** — tokio tasks, stream teardown, **no blocking `Mutex::lock` in async or `poll_next` paths**.
3. **Kubernetes client usage** — watches, reconnects, namespace/selector handling, API errors.
4. **Resource lifecycle** — leaks, duplicate attaches, mux backpressure, **pod_meta cache growth across watch resets**.
5. **Security** — credential handling, RBAC assumptions, logging sensitive data; CodeRabbit config enables secret and dependency scanning (CI runs `cargo audit`).

## Architecture boundaries

- `src/cli` owns argument parsing and validation only; keep Kubernetes, streaming, rendering, and filtering behavior out of CLI code.
- `src/run_defaults` and `src/run_config` own resolution from CLI inputs into run configuration; preserve stern/kubectl defaults and keep this layer free of async runtime orchestration.
- `crates/rustern-core/src/discovery` owns Kubernetes object discovery and pod/container reconciliation; selector, namespace, and watch behavior should stay here rather than leaking into pipeline or render code.
- `crates/rustern-core/src/source` owns log source construction and Kubernetes log stream requests; keep per-line filtering, formatting, and presentation out of source adapters.
- `crates/rustern-core/src/pipeline` owns line transformations, filtering, classification, jq, JSON annotation, and exit-trigger decisions; avoid Kubernetes API calls or terminal rendering here.
- `crates/rustern-core/src/runtime` owns orchestration, task lifecycle, attach/mux/watch flow, cancellation, and backpressure; avoid embedding CLI parsing or display formatting policy in runtime glue.
- Within runtime, **`WatchAdmission`** owns pod/container admission (`admit_pod`, `admit_streams`, `collect_snapshot`); **`AttachDeps`** owns client, mux, cursor reconnect, semaphore, and **`PodMetaCache`**; compose as **`PodWatchCtx { admission, attach }`**. Do not re-expand flat orchestration structs or leak `HashMap` cache shapes outside `pod_meta_cache.rs`.
- **`PodMetaCache`** is the only seam for pod metadata lookup/enrichment; attach code should depend on `&ContextName` + `&PodMetaCache`, not the full `PodWatchCtx`.
- **`ReconnectCursorStore`** (`cursor_store.rs`): sync `std::sync::Mutex` only with `try_lock` + bounded yield/spin from async callers; never block in stream `poll_next`.
- `crates/rustern-core/src/render` owns output formatting and terminal/color concerns; it should consume resolved events and metadata without reaching back into discovery, source, or CLI defaults.

## Cumulative CodeRabbit learnings (PRs #82–#97)

Patterns from accepted fixes and intentional declines—use when reviewing or implementing.

### Config and review process (#82, #87)

- Keep `.coderabbit.yaml` minimal; remove stale comments for settings that do not exist.
- **Source comments**: English only. **Review comments**: Japanese (`language: ja-JP`).
- Public PRs: reference Linear by ID (`DSK-123`) only—no Linear URLs in GitHub text.
- Branch naming: `feat|fix|refactor|chore/dsk-<id>-<short-description>`.
- **Linear Issue Planner**: top-level issue comment with only `@coderabbitai plan` (thread replies are rejected). Put refresh context in the issue description; substantive replans use Plan Web UI chat + **Redo**.

### Stern/kubectl parity (#88)

- `--field-selector` with an explicit `.` pod query must still normalize `.` → `.*` in `resolve_pod_query`; add regression tests for selector + query combinations.

### Pod metadata cache (#91, #92, #67)

- **Lifecycle**: `Event::Init` clears cache; `InitDone` prunes locators outside the admitted snapshot; filter reject / delete removes entries. TTL/LRU is out of scope unless watch boundaries are insufficient.
- **`PodLocator::try_from_pod`**: require `name`, `namespace`, and `uid`—no `unwrap_or_default("")` for namespace.
- **`admit_pod`**: return `false` when `metadata.name` is missing (and `namespace` when `allowed_ns` is set).
- Encapsulate cache in **`PodMetaCache`** methods; callers must not touch inner `HashMap`.

### Pipeline exit ordering (#93, #60)

- Keep pipeline stage helpers `pub(crate)` unless there is a deliberate public API need.
- `FilterOn::Original` + `--exit-on-level`: include/exclude must not be evaluated only after jq in ways that change stern-compatible exit semantics; stage order must be explicit and tested.

### Reconnect cursor store (#95, #61)

- Do not use blocking `Mutex::lock` from async contexts or `Stream::poll_next`.
- Use `try_lock` with bounded retries/`yield_now`; on exhaustion log at `warn` with key context.
- Distinguish **poisoned** mutex (dedicated warn, no retry) from **contention** (retry then warn).

### Runtime tests (#96, #64)

- **`TestOrchestratorFixture`** `_keepalive` holds `tower_test::mock` handle and mux receiver drain task; keep fixture alive for the full test. Tuple `(fixture, Arc<PodWatchCtx>)` with `_fixture` binding is intentional—not dead code.
- Default mux channel needs a background drain; dropping mock handle immediately can hang tests.
- Stream/container assertions: use **`HashSet`**, not order-dependent `Vec` equality.
- Decline nitpicks on `base_fixture` + cloned `PodWatchCtx` when fixture outlives the test and `root_child` is not shared.

### Review triage

- **Fix**: parity regressions, async blocking, cache leaks across watch resets, missing pod metadata guards, pipeline exit semantics.
- **Decline with rationale**: trivial test-builder sugar, intentional fixture lifetime patterns, behavior-neutral refactors scoped to one PR.

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
