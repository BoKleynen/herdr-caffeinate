{
  description = "ThreatCast";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";

    flake-utils.url = "github:numtide/flake-utils";

    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      rust-overlay,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            rust-overlay.overlays.default
          ];
          config.allowUnfree = true;
        };
      in
      rec {
        formatter = pkgs.nixfmt;
        devShells.default = pkgs.mkShell {
          packages = [
            (pkgs.rust-bin.stable."1.98.1".default.override {
          extensions = [
            "rust-src"
            "rust-analyzer"
          ];
        })
          ];
        };
      }
    );
}
