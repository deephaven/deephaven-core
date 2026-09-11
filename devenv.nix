# Reproducible development environment for deephaven-core, via devenv.sh
# (https://devenv.sh).
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
# its current source/docs).
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

  # Auto-detects a rootless Podman API socket so Docker-API-consuming
  # Gradle tasks (Testcontainers, the bmuschko gradle-docker-plugin) work
  # without a per-machine DOCKER_HOST hardcoded anywhere -- the socket's
  # path is $XDG_RUNTIME_DIR/podman/podman.sock, i.e. it embeds your UID
  # (e.g. /run/user/1001/podman/podman.sock), so a value that works on one
  # contributor's machine won't work on another's.
  #
  # `podman info`'s `.Host.RemoteSocket.Path` is Podman's own reported
  # socket location -- confirmed directly against podman-info(1)'s
  # documented output -- already accounting for XDG_RUNTIME_DIR/whatever
  # the podman.socket systemd unit is actually configured with, so
  # querying it beats guessing the path by hand. This only ever *reads*
  # that value; it never starts or configures the socket/service itself --
  # it assumes you already have `podman.socket` (or Docker) running
  # normally, and just wires the resulting env vars up for you.
  #
  # Only kicks in when DOCKER_HOST isn't already set (never overrides an
  # explicit choice) and the reported path is an actual live socket, not
  # just Podman's unconditionally-computed default (e.g. if podman.socket
  # is installed but not currently running).
  podmanDockerHostHook = ''
    if [[ -z "''${DOCKER_HOST:-}" ]] && command -v podman >/dev/null 2>&1; then
      _podman_sock="$(podman info --format '{{.Host.RemoteSocket.Path}}' 2>/dev/null || true)"
      # Depending on podman version/rootless-vs-rootful setup, this value
      # may already carry a "unix://" scheme prefix or may be a bare
      # filesystem path -- normalize to a bare path before testing/using it.
      _podman_sock="''${_podman_sock#unix://}"
      if [[ -n "$_podman_sock" && -S "$_podman_sock" ]]; then
        export DOCKER_HOST="unix://$_podman_sock"
        export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="$_podman_sock"
      fi
      unset _podman_sock
    fi
  '';
in
{
  languages.java = {
    enable = true;
    jdk.package = bootstrapJdk; # also sets JAVA_HOME (overridden below on
    # Darwin -- see env.JAVA_HOME)
    # Not using languages.java.gradle -- we run the repo's own ./gradlew,
    # and a second Nix-provided `gradle` binary on PATH bound to the same
    # JDK would just be a confusing, unused alternative sitting alongside
    # it.
  };

  # devenv's languages.java module sets JAVA_HOME to bootstrapJdk.home
  # unconditionally (cachix/devenv's languages/java.nix). On Linux that IS
  # the real JDK home (temurin-bin's Linux layout is flat), matching what a
  # running JVM reports as its own `java.home` system property. On Darwin,
  # though, nixpkgs' temurin-bin output is a symlink farm: the real JDK
  # content lives nested at
  # $out/Library/Java/JavaVirtualMachines/<name>-<major>.jdk/Contents/Home,
  # and $out itself (== .home == what devenv sets JAVA_HOME to) is just
  # top-level symlinks into that directory (confirmed against
  # pkgs/development/compilers/temurin-bin/jdk-darwin-base.nix). A JVM
  # launched through those symlinks reports its own `java.home` as the
  # *resolved* nested path, not $out -- so Gradle's toolchain detection
  # sees two different Location strings for the exact same JDK ("Detected
  # by: environment variable 'JAVA_HOME'" at $out, "Detected by: Current
  # JVM" at the nested Contents/Home) and lists it twice.
  #
  # nixpkgs' Darwin JDK builder already computes that nested bundle
  # directory itself and exposes it via a `bundle` passthru (added by
  # nixpkgs#375212, "treewide: standardize JDKs on darwin") -- appending
  # Contents/Home to that gives the exact canonical path a running JVM
  # will report, without us hand-guessing the vendor/version-specific
  # "<name>-<major>.jdk" bundle name. Only present on Darwin (the Linux
  # builder has no `bundle` attribute at all), so its presence is what to
  # branch on. mkForce is needed because languages.java already sets this
  # option (a plain conflicting assignment would otherwise error).
  env.JAVA_HOME = pkgs.lib.mkForce (
    if bootstrapJdk ? bundle
    then "${bootstrapJdk.bundle}/Contents/Home"
    else bootstrapJdk.home
  );

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
  '' + podmanDockerHostHook;

  # Docker-API access (Testcontainers-based `testOutOfBand` tests in
  # extensions/kafka, extensions/iceberg/s3, etc.; the bmuschko
  # gradle-docker-plugin's :docker-* subprojects) needs a real Docker or
  # Podman install already running on your host -- this file doesn't
  # provision one, it only wires up DOCKER_HOST for whatever's already
  # there (see podmanDockerHostHook above). devenv's own containers.*
  # option builds OCI images from this environment; it isn't a
  # Docker-API-compatible daemon/socket, so it wouldn't help here anyway.
}
