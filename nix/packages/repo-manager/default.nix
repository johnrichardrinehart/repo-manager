{
  lib,
  rustPlatform,
  makeWrapper,
  curl,
  git,
  ghq,
  libnotify,
}:

rustPlatform.buildRustPackage {
  pname = "repo-manager";
  version = "0.1.0";
  outputs = [
    "out"
    "repod"
  ];

  src = lib.fileset.toSource {
    root = ../../..;
    fileset = lib.fileset.unions [
      ../../../Cargo.lock
      ../../../Cargo.toml
      ../../../api
      ../../../build.rs
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

    mkdir -p "$repod/bin"
    mv "$out/bin/repod" "$repod/bin/"
    wrapProgram "$repod/bin/repod" \
      --prefix PATH : ${
        lib.makeBinPath [
          git
          libnotify
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
