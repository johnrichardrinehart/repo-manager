{ pkgs }:

{
  inherit (pkgs) repo-manager;
  default = pkgs.repo-manager;
}
