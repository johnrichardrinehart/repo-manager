{ pkgs }:

let
  repo-manager = pkgs.repo-manager or (pkgs.callPackage ./repo-manager { });
  repod = pkgs.repod or (pkgs.callPackage ./repod { });
in
{
  inherit repo-manager;
  inherit repod;
  default = repo-manager;
}
