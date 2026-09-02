# Vendors the Gradle distribution a project's gradle-wrapper.properties
# pins, and isolates Gradle's toolchain resolution to whatever this shell
# provides. Specific to this repo (not published as a standalone/reusable
# plugin) -- see the "extract into its own plugin?" discussion in this
# repo's history if that ever becomes worth revisiting for a second
# consumer.
#
# Usage from devenv.nix:
#   gradleWrapper = import ./nix/gradle-wrapper.nix {
#     inherit pkgs;
#     wrapperPropertiesFile = ./gradle/wrapper/gradle-wrapper.properties;
#   };
# Returns:
#   distExtracted     -- derivation: the unpacked Gradle distribution
#   warmupHook        -- shellHook fragment: pre-seeds ./gradlew's cache
#   isolatedHomeHook  -- shellHook fragment: isolates GRADLE_USER_HOME and
#                        disables toolchain auto-detect/auto-download
#   extraBuildInputs  -- packages the two hooks above need on PATH (bc)
{ pkgs, wrapperPropertiesFile }:
let
  # ---- Vendor the Gradle wrapper's distribution ------------------------
  #
  # `./gradlew` downloads its own Gradle distribution on first run --
  # normally a good thing (Gradle's own toolchain auto-provisioning,
  # org.gradle.toolchains.foojay-resolver-convention, already handles
  # per-subproject JDKs the same way, so there's no need to duplicate that
  # in Nix) -- but it means a fresh shell still needs network access for
  # that one download. Since the exact version and checksum are already
  # pinned in gradle-wrapper.properties, we can fetch and unpack that same
  # file as a Nix derivation (reusing its checksum, not a new trust
  # decision) and pre-seed the wrapper's on-disk cache, so `./gradlew`
  # finds it already there and skips the download.
  # gradle-wrapper.properties stays the single source of truth -- read
  # here, never duplicated -- so a version bump there just changes what
  # gets fetched, with nothing to keep in sync by hand.
  wrapperProps = pkgs.lib.splitString "\n" (builtins.readFile wrapperPropertiesFile);
  wrapperProp = key:
    let
      prefix = key + "=";
      matches = builtins.filter (pkgs.lib.hasPrefix prefix) wrapperProps;
    in
    pkgs.lib.removePrefix prefix (builtins.head matches);

  # Java .properties escapes ":" as "\:" -- unescape it back to a URL.
  distUrl = builtins.replaceStrings [ "\\:" ] [ ":" ] (wrapperProp "distributionUrl");
  distSha256 = wrapperProp "distributionSha256Sum";

  # ".../gradle-9.7.1-all.zip" -> zipBase "gradle-9.7.1-all", dirName "gradle-9.7.1"
  zipBase = pkgs.lib.removeSuffix ".zip" (pkgs.lib.last (pkgs.lib.splitString "/" distUrl));
  dirName = pkgs.lib.removeSuffix "-bin" (pkgs.lib.removeSuffix "-all" zipBase);

  distZip = pkgs.fetchurl {
    url = distUrl;
    sha256 = distSha256;
  };

  # The wrapper's on-disk layout is $GRADLE_USER_HOME/wrapper/dists/
  # <zipBase>/<hash>/<dirName>, where <hash> is base36(md5(distributionUrl))
  # -- an internal, undocumented detail of Gradle's wrapper
  # (org.gradle.wrapper.PathAssembler), confirmed empirically against a
  # real `./gradlew` run rather than assumed. If a future Gradle wrapper
  # version changes that scheme, this degrades gracefully: the pre-seeded
  # cache dir just won't be found, and `./gradlew` falls back to its
  # normal download.
  distExtracted = pkgs.runCommand "gradle-dist-${dirName}"
    { nativeBuildInputs = [ pkgs.unzip ]; }
    ''
      unzip -q ${distZip} -d "$TMPDIR/unpacked"
      mv "$TMPDIR/unpacked/${dirName}" "$out"
    '';

  # Only the base36-of-MD5 conversion happens at shell-hook runtime (via bc
  # -- Nix's own integers are too narrow for a 128-bit hash); the MD5
  # itself is computed at eval time with Nix's builtin hasher.
  distMd5Hex = builtins.hashString "md5" distUrl;

  warmupHook = ''
    _gradle_home="''${GRADLE_USER_HOME:-$HOME/.gradle}"
    _gradle_hash_hex=$(printf '%s' "${distMd5Hex}" | tr 'a-f' 'A-F')
    _gradle_hash_digits=$(BC_LINE_LENGTH=0 bc <<< "obase=36; ibase=16; $_gradle_hash_hex")
    _gradle_hash_dir=""
    _gradle_b36chars='0123456789abcdefghijklmnopqrstuvwxyz'
    for _d in $_gradle_hash_digits; do
      _gradle_hash_dir="''${_gradle_hash_dir}''${_gradle_b36chars:$((10#$_d)):1}"
    done
    _gradle_dist_dir="$_gradle_home/wrapper/dists/${zipBase}/$_gradle_hash_dir"
    if [[ ! -e "$_gradle_dist_dir/${zipBase}.zip.ok" ]]; then
      mkdir -p "$_gradle_dist_dir"
      ln -sfn "${distExtracted}" "$_gradle_dist_dir/${dirName}"
      touch "$_gradle_dist_dir/${zipBase}.zip.ok"
    fi
    unset _gradle_home _gradle_hash_hex _gradle_hash_digits _gradle_hash_dir _gradle_b36chars _gradle_dist_dir _d
  '';

  # ---- Isolate toolchain resolution from host-installed JDKs ------------
  #
  # Gradle's toolchain auto-detection scans common host locations
  # (/usr/lib/jvm, etc.) in addition to whatever's actually running the
  # build, so `./gradlew javaToolchains` sees both the Nix JDK *and* any
  # host-installed ones. `org.gradle.java.installations.auto-detect=false`
  # turns off that scanning, and `...auto-download=false` also stops
  # Gradle from downloading a toolchain it can't find -- so whatever JDK(s)
  # this shell put on PATH are the *only* ones available. Anything that
  # requests another version (e.g. this repo's nightly CI matrix, which
  # runs with -PtestRuntimeVersion=17/25 -- see
  # .github/workflows/nightly-check-ci.yml) will fail with "no matching
  # toolchain found" here rather than silently downloading one; re-run
  # with -Porg.gradle.java.installations.auto-download=true if you need to
  # reproduce that locally.
  #
  # Confirmed empirically (this repo's own `javaToolchains` task) that
  # these must be set as a Gradle *project property* (gradle.properties /
  # -P), not a JVM system property: neither `GRADLE_OPTS=-D...` nor the
  # `ORG_GRADLE_PROJECT_<dotted.key>` env var convention are honored for
  # these keys, only an actual gradle.properties file or `-P`/`-D` passed
  # directly on the command line.
  #
  # The consuming project's own (committed, shared) ./gradle.properties
  # isn't the place for this -- it'd apply to every contributor and CI,
  # not just Nix shell users. Gradle's *per-user*
  # $GRADLE_USER_HOME/gradle.properties would work and stay out of the
  # repo, but since GRADLE_USER_HOME defaults to the same ~/.gradle
  # whether or not you're in this shell, writing there would silently
  # change toolchain behavior for this user's *other* Gradle projects too,
  # and outside this shell. Instead, GRADLE_USER_HOME is pointed at a
  # Nix-shell-only directory holding just our own gradle.properties, with
  # everything else (caches, the vendored wrapper distribution above,
  # daemon, etc.) symlinked back to the real one -- so nothing is
  # duplicated or re-downloaded, only the settings file differs, and only
  # for the duration of this shell.
  isolatedHomeHook = ''
    _gradle_real_home="''${GRADLE_USER_HOME:-$HOME/.gradle}"
    _gradle_isolated_home="''${XDG_CACHE_HOME:-$HOME/.cache}/deephaven-core-nix-gradle-home"
    mkdir -p "$_gradle_real_home" "$_gradle_isolated_home"
    shopt -s nullglob
    # Always share these two -- the large, expensive-to-rebuild ones --
    # even on a from-scratch $_gradle_real_home that doesn't have them yet.
    for _d in caches wrapper; do
      ln -sfn "$_gradle_real_home/$_d" "$_gradle_isolated_home/$_d"
    done
    # Mirror whatever else already exists (daemon, jdks, ...), except
    # gradle.properties itself -- that's the one file we deliberately
    # don't want to inherit.
    for _entry in "$_gradle_real_home"/*; do
      _name="$(basename "$_entry")"
      if [[ "$_name" != "gradle.properties" && ! -e "$_gradle_isolated_home/$_name" ]]; then
        ln -sfn "$_entry" "$_gradle_isolated_home/$_name"
      fi
    done
    shopt -u nullglob
    {
      echo "org.gradle.java.installations.auto-detect=false"
      # Only whatever JDK(s) this shell provides are available as a
      # toolchain -- anything requesting another version (e.g. this repo's
      # nightly CI matrix, which runs with -PtestRuntimeVersion=17/25) will
      # fail with "no matching toolchain found" here rather than
      # downloading one.
      echo "org.gradle.java.installations.auto-download=false"
    } > "$_gradle_isolated_home/gradle.properties"
    export GRADLE_USER_HOME="$_gradle_isolated_home"
    unset _gradle_real_home _gradle_isolated_home _entry _name _d
  '';
in
{
  inherit distExtracted warmupHook isolatedHomeHook;
  extraBuildInputs = [ pkgs.bc ];
}
