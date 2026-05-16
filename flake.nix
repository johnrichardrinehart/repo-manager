{
  description = "Repository placement and lifecycle manager";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{ self, flake-parts, ... }:
    let
      lib = inputs.nixpkgs.lib;
      appSource = lib.fileset.toSource {
        root = ./.;
        fileset = lib.fileset.unions [
          ./Cargo.lock
          ./Cargo.toml
          ./api
          ./build.rs
          ./crates
          ./src
          ./nix
        ];
      };
    in
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];

      imports = [ inputs.flake-parts.flakeModules.partitions ];

      partitionedAttrs = {
        checks = "dev";
        devShells = "dev";
        formatter = "dev";
      };

      partitions.dev = {
        extraInputsFlake = ./dev;
        module = import ./nix/flake/dev-partition.nix;
      };

      perSystem =
        { pkgs, system, ... }:
        {
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [
              inputs.rust-overlay.overlays.default
              self.overlays.default
            ];
          };

          legacyPackages = pkgs;
          packages = import "${appSource}/nix/packages" { inherit pkgs; };
        };

      flake.overlays.default = import "${appSource}/nix/overlays/default.nix";
    };
}
