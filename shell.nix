# save this as shell.nix
{ pkgs ? import <nixpkgs> { overlays = [ (import (builtins.fetchTarball "https://github.com/oxalica/rust-overlay/archive/master.tar.gz")) ]; }}:

pkgs.mkShell {
  packages = [ pkgs.rust-bin.stable.latest.default pkgs.rust-analyzer ];
}
