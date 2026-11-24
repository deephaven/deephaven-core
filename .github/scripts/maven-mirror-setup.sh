#!/usr/bin/env bash
#
# Routes Gradle's Maven repository resolution through internal mirrors, for CI only.
#
# Gradle's `org.gradle.mirror.maven.settings` feature (https://docs.gradle.org/current/userguide/securing_repositories.html#sec:prefer_internal_mirrors)
# applies the <mirrors> and <servers> of ~/.m2/settings.xml to every declared Maven repository, matched by
# the mirror's <mirrorOf>. Nothing about the mirror appears in the build scripts, and a developer without a
# ~/.m2/settings.xml (the local default) resolves directly, unchanged.
#
# Each mirror is configured by environment variables, which the workflows populate from GitHub variables and secrets:
#
#   MAVEN_CENTRAL_MIRROR_URL / _USERNAME / _PASSWORD   mirrors Maven Central (the repository whose id is "central")
#
# A mirror whose URL is unset is skipped, so forks and pull requests without access to the secrets build normally.
# If no mirror is configured, this script does nothing.
#
# To mirror another repository declared in settings.gradle (e.g. the 'jitpack' or 'confluent' repositories), add
# another `add_mirror <id> <mirrorOf> <env-prefix>` call below; <mirrorOf> is the repository's Gradle name.
# Only repositories matched by a <mirrorOf> are redirected; the rest are untouched.
set -euo pipefail

m2_dir="${HOME}/.m2"
settings="${m2_dir}/settings.xml"
mirrors=""
servers=""

# add_mirror <mirror id> <mirrorOf> <env var prefix>
add_mirror() {
  local id="$1" of="$2" prefix="$3"
  local url_var="${prefix}_URL" user_var="${prefix}_USERNAME" pass_var="${prefix}_PASSWORD"
  local url="${!url_var:-}"
  if [[ -z "${url}" ]]; then
    echo "maven-mirror-setup: ${url_var} is not set; not mirroring '${of}'"
    return 0
  fi
  mirrors+="    <mirror><id>${id}</id><url>${url}</url><mirrorOf>${of}</mirrorOf></mirror>"$'\n'
  if [[ -n "${!user_var:-}" ]]; then
    servers+="    <server><id>${id}</id><username>${!user_var}</username><password>${!pass_var:-}</password></server>"$'\n'
  fi
  echo "maven-mirror-setup: mirroring '${of}' through ${url}"
}

add_mirror maven-central-mirror central MAVEN_CENTRAL_MIRROR

if [[ -z "${mirrors}" ]]; then
  exit 0
fi

if [[ -e "${settings}" ]]; then
  echo "maven-mirror-setup: ${settings} already exists; refusing to overwrite it" >&2
  exit 1
fi

umask 077
mkdir -p "${m2_dir}"
cat > "${settings}" <<XML
<settings>
  <mirrors>
${mirrors}  </mirrors>
  <servers>
${servers}  </servers>
</settings>
XML

gradle_home="${GRADLE_USER_HOME:-${HOME}/.gradle}"
mkdir -p "${gradle_home}"
echo "org.gradle.mirror.maven.settings=true" >> "${gradle_home}/gradle.properties"
