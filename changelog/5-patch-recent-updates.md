# Patch — Recent updates (unreleased)

Date: 2026-02-09

Summary
-------
Small fixes, behavior tweaks, and stability improvements landed on main. Highlights include better tracking of which task produced a resource, fixes to scheduling behavior, improved seed-step handling, and a number of demos and build/debug improvements.

Commits included
----------------
- 0fe0e9b — fix
  - Miscellaneous fixes and stability tweaks applied to the main pipeline.

- eefb97c — two demos
  - Added two demo manifests to make examples easier to run and test.

- 270c0fc — track task that created a resource
  - Resources now record which task created them (created_by_task_id). This enables correct upstream-to-downstream scheduling and better provenance.

- c9d2fc2 — pass task id to output dir
  - The task ID is now propagated into output paths so consumers can associate outputs with the producing task.

- 069f728 — allow writing to subdirectories
  - Support writing outputs into subdirectories under task outputs.

- 089fecd — debug and demo recipes
  - Misc debug and demo improvements to make development and demonstration easier.

- 24e90e9 — remove this
  - Cleanup; removed unused/obsolete code.

- 1cf48a7 — separated these
  - Internal refactor to separate concerns (non-user-facing change).

- 580283e — force close db if interrupt
  - Ensure DB is cleanly closed on interrupt signals to avoid WAL corruption.

- ab00612 — panic if no new resources
  - Changed behavior to surface an error when steps unexpectedly produce no resources (prevents silent failures).

- 90929cb — update
  - Minor updates and polish (internal).

- 6c7e1c6 — changed seed step process
  - Steps with no inputs are treated as seed steps (executed when no resources are present), simplifying manifest behavior.

- 8d25e5d — removed this
  - Further cleanup of unused code.

- 4afafc4 — do not silently fail
  - Improved error handling to avoid silent failures and make debugging easier.

- 4d9eabb — export tar file
  - Added/updated an export-to-tar capability for pipeline outputs.

Testing / Verification
----------------------
- Run a demo manifest (for example: `go run . run -manifest "./demo/Parallel fan-out with controlled dependency.toml" -db /tmp/tmp.db`) and verify the following:
  - On first run, seed steps produce resources and downstream tasks are scheduled and executed.
  - On re-running the same manifest without changing inputs, identical outputs are de-duplicated (same hash) and no extra downstream tasks are scheduled unless outputs change.

Notes
-----
- This changelog entry is intended as a top-level summary. See individual commits for implementation details.
- If you want this entry split into a minor vs patch release or formatted for a specific release process (e.g., keep a single CHANGELOG.md vs per-release files), tell me and I can update the format.
