# PR Mental Model (CodeRabbit HTML generation)

Generate a single standalone HTML file that explains **why** the PR changes the
system—not a file-by-file diff recap.

## Output

- Path: `.github/pr-artifacts/pr-<PR_NUMBER>-mental-model.html`
- One file only; inline CSS; mermaid CDN only when a diagram needs branches/parallelism
- Language: Japanese prose; code identifiers in English
- Do **not** modify source code, tests, or config outside that HTML file

## Fixed section order

1. Title + PR metadata (repo, URL, author, diff size)
2. One-line summary (how the world changes)
3. Alerts (only if prerequisites, incidents, or reverts matter)
4. **5–7 change-intent cards** (each must include「なぜこの形か」)
5. Data flow / layer diagram (only when multiple layers are involved)
6. Scope boundary (in scope / out of scope)
7. Review focus (3–5 bullets; no deep triage)
8. Front-end PRs only: file overview table at the end

## Intent cards

- Decompose from PR description **and** diff; do not walk Files Changed order
- 5–7 intents; re-split if fewer than 5 or more than 7
- Tag each intent with one of: 主目的, 設計判断, バグ予防, 責務移動, ついでの整合, 堅牢化, 汎用化, 命名, リリース運用
- Mark inferred intents with `<span class="inferred">推測</span>`
- Every card requires「なぜこの形か」—not WHAT-only restatements

Pick presentation per intent (do not uniformize):

| Expression | Use when |
|---|---|
| Before/After columns | Clear structural change with a real before |
| After-only snippet | New files or new logic |
| Why box (highlighted) | Trade-offs, design reasoning |
| Timeline | Revert, hotfix, incident sequence |
| ASCII `<pre>` | Backend layers; keep lightweight |
| mermaid | Complex branching; not for simple linear flows |
| Table | Layer × file × intent overview |
| Cross-file list | Front-end intent spanning multiple files |

Front-end PR (`.tsx`/`.jsx`/`.vue`/`.svelte`): intent-first, cross-file lists per
card; file overview table only at the end.

Backend PR (Rust/Go): file mentions inside intents are fine; layer badges when useful.

## HTML minimum

```html
<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <title>PR Mental Model — #<number></title>
  <style>/* inline */</style>
</head>
<body>...</body>
</html>
```

CSS: readable line-height (1.7+), padding 24px+, max-width ~900–1000px, intent cards
bordered,「なぜこの形か」visually distinct (accent background or left border).

## Avoid

- File-order narration as the primary structure
- WHAT-only bullets (“renamed X”, “added 2 props”)
- Empty Before columns for new files
- Forcing every intent into Before/After grids
- Loading mermaid without a diagram
- Deep review triage (intent/behavior tiers, attention budget)—out of scope

## Large diffs

If diff exceeds ~5000 lines, prioritize non-generated files; skip lockfiles,
snapshots, and obvious codegen from intent narrative.
