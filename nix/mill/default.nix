{ pkgs, stdenvNoCC, jdk, fetchurl, makeWrapper }:
let
  version = "1.2.0-RC1-42-dc1f60";
in
stdenvNoCC.mkDerivation {
  pname = "mill";
  inherit version;

  # The platform-independent "universal" launcher (a self-executing jar).
  # Pinned to the exact version our build files declare in `//| mill-version`,
  # so `mill` on PATH is always the right one — no runtime downloads.
  src = fetchurl {
    url = "https://repo1.maven.org/maven2/com/lihaoyi/mill-dist/${version}/mill-dist-${version}.exe";
    hash = "sha256-aZX/w+FnH2fSi7U9ZEZMLrIyL7jz8Tyr73G24Gdsehs=";
  };

  dontUnpack = true;

  nativeBuildInputs = [ makeWrapper ];

  installPhase = ''
    runHook preInstall
    install -Dm444 $src $out/share/mill/mill-dist.jar
    makeWrapper ${jdk}/bin/java $out/bin/mill \
      --add-flags "-jar $out/share/mill/mill-dist.jar"
    runHook postInstall
  '';

  meta = with pkgs.lib; {
    homepage = "https://mill-build.org";
    description = "Mill JVM build tool (pinned dist launcher)";
  };
}
