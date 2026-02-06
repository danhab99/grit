# Patch: Refactor Main Architecture

## Changes

### Main Entry Point
- **Restored main.go**: Removed Nix-based code generation for main.go
  - Reverted to manual subcommand dispatch structure
  - Cleaner, more maintainable approach without code generation dependencies
  - Removed `cmd/run/main.go` and `cmd/export/main.go` to fix package conflicts
- **Subcommand architecture**: Now supports three main subcommands
  - `grit run`: Execute the pipeline with manifest
  - `grit export`: Export resources from database (by name or hash, with tarball support)
  - `grit status`: Display pipeline status and statistics

### Export Command Enhancements
- **Refactored export command**: Split monolithic export.go into focused modules
  - `cmd/export/name.go`: Handle listing resources by name
  - `cmd/export/hash.go`: Handle exporting individual resource by hash
  - `cmd/export/tar.go`: New functionality to export resources as tarball
- **New tarball export**: Added ability to export all resources with a given name as a tar archive

### Database Functions
- **New function**: `GetAllResourceNames()` returns channel of distinct resource names
  - Supports listing all available resource names in the database
  - Consistent with existing channel-based API patterns

### Build System
- **Dynamic versioning**: Version now calculated from changelog directory structure
  - Versions automatically increment based on changelog entries
  - Simplified version management in flake.nix
- **Nix improvements**: Formatting and refactoring of Nix configuration files
  - Updated `nix/checks.nix`, `nix/gen-main.nix`, and `nix/lib.nix`
  - Better organization and maintainability
