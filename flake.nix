{
  description = "A devShell example";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    nix-pre-commit.url = "github:jmgilman/nix-pre-commit";
  };

  outputs = {
    nixpkgs,
    rust-overlay,
    flake-utils,
    nix-pre-commit,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        overlays = [(import rust-overlay)];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        config = {
          repos = [
            {
              repo = "local";
              hooks = [
                {
                  id = "cargo clippy";
                  entry = "cargo clippy";
                  language = "system";
                  files = "\\.rs";
                  pass_filenames = false;
                }
                {
                  id = "cargo fmt";
                  entry = "cargo fmt";
                  language = "system";
                  files = "\\.rs";
                  pass_filenames = false;
                  args = ["--"];
                }
                {
                  id = "cargo test";
                  entry = "cargo test";
                  language = "system";
                  files = "\\.rs";
                  pass_filenames = false;
                  args = ["--" "--ignored"];
                }
              ];
            }
          ];
        };
      in {
        devShells.default = with pkgs;
          mkShell {
            shellHook =
              (nix-pre-commit.lib.${system}.mkConfig {
                inherit pkgs config;
              })
              .shellHook;

            buildInputs = [
              rust-bin.nightly.latest.default
              rust-analyzer
              cargo-watch
              nixd
              alejandra
            ];
          };
      }
    );
}
