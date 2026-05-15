{
  lib,
  rustPlatform,
  makeWrapper,
  curl,
  git,
  ghq,
}:

rustPlatform.buildRustPackage {
  pname = "repo-manager";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = ../../..;
    fileset = lib.fileset.unions [
      ../../../Cargo.lock
      ../../../Cargo.toml
      ../../../crates
      ../../../src
    ];
  };

  cargoLock.lockFile = ../../../Cargo.lock;

  nativeBuildInputs = [ makeWrapper ];

  nativeCheckInputs = [
    git
    ghq
  ];

  postInstall = ''
    wrapProgram "$out/bin/repo" \
      --prefix PATH : ${
        lib.makeBinPath [
          curl
          git
          ghq
        ]
      }
  '';

  meta = with lib; {
    description = "Opinionated repository placement and lifecycle manager";
    license = licenses.mit;
    mainProgram = "repo";
    platforms = platforms.linux ++ platforms.darwin;
  };
}
