
build:
  nix eval --impure --raw --expr 'let flake = builtins.getFlake (toString ./.); in flake.lib.generate-main ./cmd' > main.go
  go build
