{ pkgs }:
let

  # INSPIRATION
  # https://github.com/fdb-rs/fdb/blob/main/nix/ci/fdb-7.1/default.nix

  version = "7.3.71";
  isDir = false;

  fdb-client-lib-dir = pkgs.callPackage ./client-lib.nix {
    sha256 = "sha256-gZsbGnHC78vVtNgcOkRQ5VM9y6BFyUznH2cE6FRyut8=";
    inherit version isDir;
  };

  fdb-cli = pkgs.callPackage ./app.nix {
    name = "cli";
    sha256 = "sha256-ldutrIeoXeK3LmmEUEJgVDhdM2v68Ry4Lhk2FcKrmAY=";
    inherit version;
  };


in {
    inherit fdb-cli fdb-client-lib-dir;
}
