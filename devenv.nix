# Reproducible development environment for deephaven-core, via devenv.sh
# (https://devenv.sh) instead of a hand-rolled Nix flake -- see this repo's
# git history for an earlier flake.nix-based attempt and why it was set
# aside (nested rootless-Podman-in-Podman for Docker-API access turned out
# to be genuinely unreliable inside a sandboxed container; devenv.sh
# doesn't help with that either, so this version simply assumes your host
# already has a working Docker or Podman install for anything that needs
# one -- see the "Docker-API access" note near the bottom).
#
# This is deliberately narrow: it provisions the tools a human needs on
# PATH to run `./gradlew`, work on the web client, or build the C++/Python
# clients -- it does not try to replace Gradle's own JDK toolchain
# provisioning (org.gradle.toolchains.foojay-resolver-convention, declared
# in settings.gradle), which already downloads whatever per-subproject JDK
# (11-25) a given build target requests. Pinning every one of those here
# too would just be a second, competing source of truth for the same
# versions -- so only the *bootstrap* JDK needed to launch Gradle itself is
# pinned below.
#
# Usage:
#   devenv shell           # enter the environment (Java/Node/Python/C++
#                           # toolchains all present at once -- devenv
#                           # doesn't have the flake-style per-language
#                           # `nix develop .#foo` split, everything
#                           # declared here is just always on PATH)
#   direnv users: `echo "eval \"\$(devenv direnvrc)\"" >> .envrc && echo "use devenv" >> .envrc && direnv allow`
#                           # (an .envrc with exactly this is already
#                           # checked in -- just run `direnv allow`)
#
# Every shell entry also vendors the exact Gradle distribution
# gradle-wrapper.properties pins into the Nix store, pre-seeds
# `./gradlew`'s cache with it, and isolates toolchain resolution to just
# the JDK this file provides -- see nix/gradle-wrapper.nix for all of
# that. devenv has no built-in equivalent for either (confirmed against
# its current source/docs before writing this), so it's carried over
# unchanged from the flake.nix-based attempt.
{ pkgs, ... }:
let
  # Gradle 9.7.1 (this repo's wrapper version, see
  # gradle/wrapper/gradle-wrapper.properties) requires Java 17+ just to
  # launch. 21 is what .devcontainer/project.Dockerfile installs today --
  # keep them in sync if that ever changes.
  bootstrapJdk = pkgs.temurin-bin-21;

  gradleWrapper = import ./nix/gradle-wrapper.nix {
    inherit pkgs;
    wrapperPropertiesFile = ./gradle/wrapper/gradle-wrapper.properties;
  };
in
{
  languages.java = {
    enable = true;
    jdk.package = bootstrapJdk; # also sets JAVA_HOME
    # Not using languages.java.gradle -- we run the repo's own ./gradlew,
    # and a second Nix-provided `gradle` binary on PATH bound to the same
    # JDK would just be a confusing, unused alternative sitting alongside
    # it.
  };

  languages.javascript = {
    enable = true;
    # Track web/client-api/types/.nvmrc. devenv has no .nvmrc
    # auto-detection (confirmed against its source) -- version pinning is
    # just "pick the matching nixpkgs package."
    package = pkgs.nodejs_24;
  };

  languages.python = {
    enable = true;
    # Matches python-version in .github/workflows/quick-ci.yml. Using an
    # explicit package (rather than `languages.python.version = "3.12"`)
    # avoids pulling in the extra nixpkgs-python devenv.yaml input that
    # form requires, for the same pin.
    package = pkgs.python312;
  };

  languages.cplusplus.enable = true; # LSP (ccls) + debugger only -- the
  # actual C/C++ toolchain packages are plain Nix packages below, same as
  # a raw `nix develop` shell; devenv's C/C++ language modules don't do
  # compiler/dependency selection themselves (confirmed against source).

  packages = with pkgs; [
    git
    jq
    curl

    # py-server's jpy (its JNI bridge) compiles a native extension against
    # languages.python's interpreter, so a compiler toolchain is required
    # alongside it.
    gcc
    gnumake

    # Mirrors cpp-client/README.md's `apt install` line.
    cmake
    zlib
    bzip2
    openssl
    pkg-config
  ] ++ gradleWrapper.extraBuildInputs;

  enterShell = gradleWrapper.isolatedHomeHook + gradleWrapper.warmupHook + ''
    echo "deephaven-core dev shell (bootstrap JDK $(java -version 2>&1 | head -1))"
    echo "Run: ./gradlew server-jetty-app:run"
  '';

  # Docker-API access (Testcontainers-based `testOutOfBand` tests in
  # extensions/kafka, extensions/iceberg/s3, etc.; the bmuschko
  # gradle-docker-plugin's :docker-* subprojects) needs a real Docker or
  # Podman install already running on your host -- this file doesn't
  # provision one. devenv's own containers.* option builds OCI images from
  # this environment; it isn't a Docker-API-compatible daemon/socket, so it
  # doesn't help here. (A previous flake.nix-based attempt tried to get a
  # rootless Podman-in-Podman Docker API working from inside Nix itself --
  # see this repo's git history -- and hit real, unresolved
  # kernel-namespace limits when nested inside a sandboxed container. On a
  # plain host, that whole problem doesn't apply: just install Docker or
  # Podman normally.)
}
