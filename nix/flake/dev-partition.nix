{ inputs, ... }:

{
  imports = [
    inputs.git-hooks.flakeModule
    inputs.treefmt-nix.flakeModule
  ];

  perSystem =
    { config, pkgs, ... }:
    {
      treefmt = {
        projectRootFile = "flake.nix";
        programs = {
          nixfmt.enable = true;
          rustfmt = {
            enable = true;
            edition = (builtins.fromTOML (builtins.readFile ../../Cargo.toml)).package.edition;
          };
          taplo.enable = true;
        };
      };

      pre-commit.check.enable = false;
      pre-commit.settings.hooks = {
        treefmt.enable = true;

        clippy = {
          enable = true;
          entry =
            let
              rust = pkgs.rust-bin.stable.latest.default;
            in
            toString (
              pkgs.writeShellScript "clippy-hook" ''
                ${rust}/bin/cargo clippy --all-targets -- -D warnings
              ''
            );
          files = "\\.rs$";
          pass_filenames = false;
        };

        cargo-test = {
          enable = true;
          entry =
            let
              rust = pkgs.rust-bin.stable.latest.default;
            in
            toString (
              pkgs.writeShellScript "cargo-test-hook" ''
                ${rust}/bin/cargo test --all-targets
              ''
            );
          files = "\\.rs$";
          pass_filenames = false;
        };
      };

      devShells.default = pkgs.mkShell {
        shellHook = config.pre-commit.installationScript;
        buildInputs = with pkgs; [
          (rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
            ];
          })
          cargo-edit
          curl
          git
          ghq
          sqlite
        ];
      };
    };
}
