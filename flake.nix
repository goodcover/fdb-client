{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:nixos/nixpkgs";
    graal.url = "github:nixos/nixpkgs/336eda0d07dc5e2be1f923990ad9fdb6bc8e28e3";
  };

  outputs = { self, flake-utils, nixpkgs, graal }:
    flake-utils.lib.eachDefaultSystem (system:
      let

        inherit (nixpkgs) lib;

        graalpkgs = import graal { inherit system; };

        jdk-overlay = self: super: {
          jdk = graalpkgs.graalvm-ce;
          jre = graalpkgs.graalvm-ce;
        };

        isLinux = lib.hasPrefix "x86_64-linux" system;

        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            jdk-overlay
          ];
        };

        foundation73 = pkgs.callPackage ./nix/fdb { };

        # FDB vars and packages only for Linux
        FDB_LIBRARY_PATH_FDB_C = if isLinux then "${foundation73.fdb-client-lib-dir}/libfdb_c.so.${foundation73.fdb-client-lib-dir.version}" else "";

        fdbPkgs = if isLinux then [ foundation73.fdb-cli ] else [];

      in
      {
        devShell = pkgs.mkShell {
          buildInputs = [
            graalpkgs.graalvm-ce
            pkgs.mill
          ] ++ fdbPkgs;

          FDB_LIBRARY_PATH_FDB_C = FDB_LIBRARY_PATH_FDB_C;

          JAVA_OPTS = ''${lib.optionalString (FDB_LIBRARY_PATH_FDB_C != "") "-DFDB_LIBRARY_PATH_FDB_C=${FDB_LIBRARY_PATH_FDB_C}"}
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
          '';

          shellHook = ''
            export REPO_ROOT=$PWD
            [[ -f "$SECURE_SOURCE/mill/source-me.sh" ]] && source "$SECURE_SOURCE/mill/source-me.sh"
          '';
        };
      }
    );
}
