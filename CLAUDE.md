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
