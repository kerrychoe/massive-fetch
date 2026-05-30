# massive-fetch — Project Working Agreement

This file is project-level memory. It governs *how* work proceeds on this repo.
The *what* lives in `SPEC.md` (the design contract) and `DESIGN_LOG.md` (the
reasoning behind it).

## Workflow rules

- **One slice per session.** Implement a single slice from `SPEC.md` §13 per
  session. Do **not** start the next slice in the same session.

- **Plan before code.** Before writing any code for a new slice, present a plan
  to the user containing:
  1. A summary of what the slice requires.
  2. The list of files to create or modify, with one-sentence descriptions.
  3. The dependencies to add to `pyproject.toml`.
  4. The exact acceptance test that will be run.

- **Wait for approval.** Do not create or modify any files until the user
  explicitly approves the plan.

- **Verify, then show.** After implementation, run the slice's acceptance test
  and show the full output.

- **No commit without approval.** Do not `git commit` without explicit user
  approval.

- **Push on approval.** When the user approves the commit, push to
  `origin/main`.

- **Respect slice boundaries.** Defer features explicitly assigned to later
  slices in `SPEC.md` §13, even if they seem natural to implement alongside the
  current slice.

## Escalating to the outside reviewer

The user runs a second, independent reviewer (claude.ai) for the calls an
in-session reviewer is weakest at. Do NOT rely on the user to remember when to
consult it — surface those moments yourself.

End every plan with an "Outside-reviewer flags" section listing which decisions
warrant the outside eye, each with a one-line reason — or "none" if the slice is
pure routine reuse.

Tag a decision as an outside-reviewer moment when any of these hold:
- Architectural fork — more than one defensible structure that will outlive the
  slice (schema, retry/concurrency strategy, an abstraction extraction, storage
  layout); i.e. anything that would become a DESIGN_LOG "Decision."
- The work contradicts or must deviate from the SPEC (acceptance can't be met as
  written, a SPEC reconciliation, or you want to change the contract).
- A refactor touches committed, tested code, where passing tests could mask a
  behavior change.
- A choice establishes or inverts a pattern that affects future slices.
- You see a real tradeoff with no clear winner, or are genuinely uncertain.
- Anything risking data loss, irreversibility, or security.

Do NOT flag routine work — plumbing that reuses a proven pattern, mechanical edits,
or "tests pass" verification. Over-flagging defeats the purpose; if a checkpoint is
all routine, say so in one line.

Mid-execution, if you hit one of these triggers the plan didn't anticipate, stop
and surface it as an outside-reviewer moment with the reason — don't decide it
yourself.
