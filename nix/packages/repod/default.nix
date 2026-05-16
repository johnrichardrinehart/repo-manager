{
  lib,
  rustPlatform,
  makeWrapper,
  git,
  libnotify,
}:

rustPlatform.buildRustPackage {
  pname = "repod";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = ../../..;
    fileset = lib.fileset.unions [
      ../../../Cargo.lock
      ../../../Cargo.toml
      ../../../api
      ../../../crates
    ];
  };

  cargoLock.lockFile = ../../../Cargo.lock;

  cargoBuildFlags = [
    "-p"
    "repod"
  ];

  cargoTestFlags = [
    "-p"
    "repod"
    "-p"
    "repo-manager-core"
  ];

  nativeBuildInputs = [ makeWrapper ];

  nativeCheckInputs = [ git ];

  postInstall = ''
    wrapProgram "$out/bin/repod" \
      --prefix PATH : ${
        lib.makeBinPath [
          git
          libnotify
        ]
      }
  '';

  meta = with lib; {
    description = "repo-manager RPC daemon";
    license = licenses.mit;
    mainProgram = "repod";
    platforms = platforms.linux ++ platforms.darwin;
  };
}
