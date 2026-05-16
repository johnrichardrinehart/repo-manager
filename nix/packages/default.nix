{ pkgs }:

let
  repo-manager = pkgs.repo-manager or (pkgs.callPackage ./repo-manager { });
  repod =
    pkgs.repod or (pkgs.symlinkJoin {
      name = "${repo-manager.pname}-${repo-manager.version}-repod";
      paths = [ repo-manager.repod ];
      meta = (repo-manager.meta or { }) // {
        description = "repo-manager RPC daemon";
        mainProgram = "repod";
      };
    });
in
{
  inherit repo-manager;
  inherit repod;
  default = repo-manager;
}
