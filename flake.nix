{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    (flake-utils.lib.eachSystem flake-utils.lib.defaultSystems (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
        lib = pkgs.lib;

        grit = import ./nix/lib.nix { inherit lib; };
      in
      {
        packages = {
          default = pkgs.buildGoModule {
            pname = "grit";
            version = import ./changelog;
            src = self;
            vendorHash = "sha256-6zbliSeUKwMS4NEyJCxT8z1IzDFxH47KQ78V6jGpsG4=";
            subPackages = [ "." ];

            GO_PATH = "${self.outPath}/.go";
            CGO_CFLAGS = "-U_FORTIFY_SOURCE";
            CGO_CPPFLAGS = "-U_FORTIFY_SOURCE";
          };
        };

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go
            gopls
            delve
            sqlite
            sqlite-web
            pandoc
            texliveFull
            just
          ];

          shellHook = ''
            export OUTPUT_DIR=$(mktemp -d)
          '';

          CGO_CFLAGS = "-U_FORTIFY_SOURCE";
          CGO_CPPFLAGS = "-U_FORTIFY_SOURCE";
        };
      }
    ))
    // (
      let
        pkgs = import nixpkgs {
          system = "x86_64-linux";
          config.allowUnfree = true;
        };
        lib = pkgs.lib;
      in
      {
        lib = import ./nix/lib.nix { inherit lib pkgs; };
      }
    );
}
