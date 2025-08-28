# This derivation can be used to extract out
#   - `fdbbackup`
#   - `fdbcli`
#   - `fdbmonitor`
#   - `fdbserver`
# from GitHub and patch the app using  `autoPatchelfHook`.
#
# In this derivation, we don't care where the app will eventually get
# placed in the file system.



{ stdenv
, autoPatchelfHook
, fetchurl
, name
, sha256
, zlib
, xz
, version
}:

stdenv.mkDerivation {
  pname = "fdb-${name}";

  inherit version;

  src = fetchurl {
    url = "https://github.com/apple/foundationdb/releases/download/${version}/fdb${name}.x86_64";
    inherit sha256;
  };

  nativeBuildInputs = [
    autoPatchelfHook
    zlib
    xz
  ];

  unpackPhase = ":";

  installPhase = ''
    mkdir -p $out/bin/
    cp $src $out/fdb${name}
    cp $src $out/bin/fdb${name}
    chmod 755 $out/fdb${name}
    chmod 755 $out/bin/fdb${name}
  '';
}
