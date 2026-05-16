{ pkgs }:

let
  repo-manager = pkgs.repo-manager or (pkgs.callPackage ./repo-manager { });
in
{
  inherit repo-manager;
  repod = repo-manager.repod;
  default = repo-manager;
}
