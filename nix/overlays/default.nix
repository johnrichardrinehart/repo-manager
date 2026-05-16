final: _prev: {
  repo-manager = final.callPackage ../packages/repo-manager { };
  repod = final.symlinkJoin {
    name = "${final.repo-manager.pname}-${final.repo-manager.version}-repod";
    paths = [ final.repo-manager.repod ];
    meta = (final.repo-manager.meta or { }) // {
      description = "repo-manager RPC daemon";
      mainProgram = "repod";
    };
  };
}
