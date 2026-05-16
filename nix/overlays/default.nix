final: _prev: {
  repo-manager = final.callPackage ../packages/repo-manager { };
  repod = final.repo-manager.repod;
}
