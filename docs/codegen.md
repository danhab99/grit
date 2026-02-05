# Automatic main.go Generation

This project uses a Nix-based code generation system to automatically create `main.go` from the `cmd/` directory structure.

## Why?

- **Zero boilerplate**: Adding a new subcommand requires no manual `main.go` edits
- **Declarative**: The file structure IS the configuration
- **Type-safe**: Can't forget to register a command
- **Self-documenting**: Descriptions live with the code

## How It Works

```
cmd/
├── export/
│   └── export.go          // Description: Export resources from the database
├── run/
│   └── run.go             // Description: Run the pipeline
└── status/
    └── status.go          // Description: Show pipeline status and statistics

                ↓
        [nix/gen-main.nix]
                ↓
             main.go        (auto-generated with all commands)
```

The Nix expression:
1. Reads all directories under `cmd/`
2. Extracts `// Description:` comments from each `<name>.go` file
3. Generates:
   - Import statements: `"grit/cmd/export"`, `"grit/cmd/run"`, etc.
   - Switch cases for each command
   - Usage text with aligned descriptions

## Usage

### Development (regenerate manually)

```bash
nix eval --impure --raw \
  --expr 'let flake = builtins.getFlake (toString ./.); in flake.lib.generate-main ./cmd' \
  > main.go
```

### Production (automatic during build)

```bash
nix build
# main.go is automatically generated from cmd/ structure
```

## Adding a New Command

1. Create `cmd/newcommand/newcommand.go`:
   ```go
   // Description: What this command does
   package newcommand
   
   func RegisterFlags(fs *flag.FlagSet) { ... }
   func Execute() { ... }
   ```

2. Regenerate:
   ```bash
   nix eval --impure --raw \
     --expr 'let flake = builtins.getFlake (toString ./.); in flake.lib.generate-main ./cmd' \
     > main.go
   ```

3. Done! The command is now available:
   ```bash
   ./grit newcommand --help
   ```

## Implementation

- **Generator**: [nix/gen-main.nix](nix/gen-main.nix)
- **Flake integration**: [flake.nix](flake.nix) - see `packages.default.src`
- **Example commands**: [cmd/](cmd/)

## Template

Every command must follow this pattern:

```go
// Description: Brief description (appears in --help)
package commandname

import (
    "flag"
    "fmt"
    "os"
)

var (
    // Flags as package-level variables
    flag1 *string
)

func RegisterFlags(fs *flag.FlagSet) {
    flag1 = fs.String("flag1", "default", "description")
}

func Execute() {
    // Command implementation
}
```

## Benefits Over Manual Approach

| Manual main.go | Auto-generated |
|----------------|----------------|
| Add command dir | Add command dir |
| Write command code | Write command code |
| Edit main.go imports | ✅ Automatic |
| Add switch case | ✅ Automatic |
| Update help text | ✅ Automatic |
| Keep description in sync | ✅ Always in sync |

## Notes

- The generated `main.go` includes a warning comment: **DO NOT EDIT MANUALLY**
- Command descriptions are extracted from the first line: `// Description: ...`
- Commands are sorted alphabetically in the help text
- The original manual `main.go` is backed up as `main.go.backup`
