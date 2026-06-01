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

## Pull requests

- One kind of change per PR when possible.
- Behavior changes should cite tests, issue, or documented parity rationale.
- Keep review comments in **Japanese**; quote identifiers, types, and APIs in **English** as in the codebase.

## Merge expectations

- Do not treat CI green alone as sufficient; serious issues should be fixed or explicitly declined with rationale.
