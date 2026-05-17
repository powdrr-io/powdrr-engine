---
trigger: always_on
---

# Powdrr Engine Agent Notes

## What This Doc Is
Repo-wide instructions for agent work in this checkout of `powdrr-engine`.

- Keep this file limited to repo-wide rules.
- Put surface-specific workflow details in the nearest relevant doc when the repo grows that structure.
- Treat `README.md` as the source of truth for operator setup and service-specific commands.

## Non-Negotiable Invariants
- **Never implement changes from the primary checkout at repo root.**
- **The primary checkout must stay clean.**
- Every implementation task must run in its own linked worktree under `.worktrees/`.
- A worktree is not ready for Rust commands until `scripts/setup-worktree.sh` has run successfully there.
- Read-only exploration may happen from the primary checkout, but stop and create or resume a worktree before the first file edit, commit, generated repo-tracked output, or "ready" claim.
- Never commit directly to `main`.
- Never rewrite shared history unless the user explicitly asks for it.

## Primary Checkout Contract
- The repo root is a launcher checkout only.
- Keep the root checkout on `main`.
- Keep `git status --porcelain` empty in the root checkout.
- If the root checkout is dirty, stop and ask the user whether to commit, stash, discard, or move the work before starting a new implementation task.
- Do not park feature branches, ad hoc experiments, or generated outputs in the root checkout.
- Shared repo-owned caches such as `.cargo-target/` may live at the repo root, but they must stay ignored and must not become part of an implementation diff.

## Worktree Policy
- Create one isolated worktree per change.
- Default branch naming: `<runtime>/<task-slug>`, for example `codex/fix-router-timeout`.
- Default worktree path: `.worktrees/<runtime>-<task-slug>`, for example `.worktrees/codex-fix-router-timeout`.
- If the repo cannot create slash-separated branch names in a given environment, use a flat fallback such as `codex-fix-router-timeout`, but keep the worktree path under `.worktrees/`.
- If a task expands materially or splits into separate concerns, create a new worktree instead of broadening the existing diff indefinitely.
- Remove finished worktrees after the branch is merged or no longer needed.

## Standard Workflow
1. Explore from the root checkout if needed, but do not edit there.
2. Verify the root checkout is clean and on `main`.
3. Create or resume a task worktree under `.worktrees/`.
4. Run `scripts/setup-worktree.sh` immediately in that worktree. This is required so `target/` points at the shared repo-level `.cargo-target/` cache instead of consuming duplicate disk in each checkout.
5. Use `scripts/cargo-worktree.sh` as the default entrypoint for Cargo commands in the worktree. If plain `cargo` is used, it must still run from that bootstrapped worktree.
6. Do all edits, tests, commits, and generated tracked outputs inside that worktree only.
7. Validate the touched surface before calling the change ready.
8. Report exactly which checks passed, failed, or were not run.

Example:

```bash
mkdir -p .worktrees
git fetch origin
git worktree add -b codex/<task-slug> .worktrees/codex-<task-slug> origin/main
cd .worktrees/codex-<task-slug>
scripts/setup-worktree.sh
scripts/cargo-worktree.sh check -p <crate>
```

Fallback when slash-separated branch names are blocked locally:

```bash
mkdir -p .worktrees
git fetch origin
git worktree add -b codex-<task-slug> .worktrees/codex-<task-slug> origin/main
cd .worktrees/codex-<task-slug>
scripts/setup-worktree.sh
scripts/cargo-worktree.sh check -p <crate>
```

## Change Approach
1. Explore first. Read the relevant crates and existing patterns before editing.
2. Tests next. Add or update tests close to the behavior being changed.
3. Implementation after that. Keep diffs focused and avoid unrelated cleanup.
4. Validation before handoff. Do not describe work as done if required checks are still failing.

## Validation Expectations
- Run targeted checks while iterating.
- Bootstrap or re-check the worktree setup before Rust validation if `target/` is missing or looks wrong.
- Prefer `scripts/cargo-worktree.sh nextest run` for the fast default test loop, and keep `cargo test --doc` as a separate step.
- Prefer `scripts/cargo-worktree.sh check -p <crate>` or `scripts/cargo-worktree.sh test -p <crate>` over whole-workspace commands during the edit loop.
- Run formatting before handoff: `cargo fmt --all`.
- Run the most relevant crate-level or workspace-level tests for the touched code.
- When test isolation is unclear, default to the repo guidance in `README.md`:
  `RUST_BACKTRACE=1 scripts/cargo-worktree.sh test -- --nocapture --test-threads=1`
- If the change touches Elasticsearch or integration behavior, read `README.md` first and run the required local dependencies.
- In handoff, list the exact commands run and their outcomes.

## Safety Rules
- Never overwrite or revert user changes you did not make unless explicitly asked.
- Never use destructive Git commands such as `git reset --hard` or `git checkout -- <path>` unless explicitly requested.
- Prefer small, reviewable diffs.
- Update tests and docs in the same change when behavior or workflow changes.
- Open PRs in ready-for-review form by default. Do not create draft PRs unless the user explicitly asks for a draft.
- Once a PR is open, check for merge conflicts against its base branch and resolve them immediately instead of leaving the PR in a conflicted state.

## Maintenance
- Update this file when the repo-wide workflow changes.
- If the project later adds surface-specific docs, keep this file short and move narrow guidance closer to the owning code.
