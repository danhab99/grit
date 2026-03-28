# Removed SQLite

Date: 2026-03-28

## Type
minor

## Summary

Grit no longer depends on SQLite. The storage layer has been fully migrated to BadgerDB, resulting in a single embedded database with no CGO requirement. Additionally, CSV files can now be ingested directly as pipeline resources.

## Changes

- **Dropped SQLite dependency** — Grit no longer requires CGO or a C compiler to build. The entire storage layer runs on BadgerDB.
- **CSV ingest** — Manifests can now reference CSV files directly. Each row is ingested as a resource, making it easy to seed a pipeline from tabular data.
- **Simpler data directory** — The `sqlite/` and `objects_db/` subdirectories are replaced by a single `db/` directory under the repo path.
- **Breaking: existing databases are incompatible** — The on-disk format has changed. Pipelines must be re-run from scratch after upgrading.

