# syntax=docker/dockerfile:1.7

#
# Builds the Deephaven C++ client dependencies image: a pinned C++ toolchain
# plus all vcpkg-built dependencies (arrow, grpc, protobuf, ...), ready for
# the :cpp-client:cppClient Gradle task to build the client itself on top.
#
# This image is content-addressed: the Gradle build hashes this Dockerfile,
# vcpkg.json and the custom triplets into the image tag, pulls the matching image from ghcr.io when it exists (anonymous, no
# credentials needed), and only builds it locally on a miss. CI publishes the
# image on pushes to main, so developers should almost never build it.
#
# The toolchain stage mirrors the retired cpp-clients-multi-base image from
# deephaven/deephaven-base-images: the R toolchain and R packages are included
# because :R:rClient builds FROM the resulting cpp-client image, and mono is
# included so vcpkg's nuget binary caching (GitHub Packages) works on Linux.
#

# Pinned by digest on purpose: vcpkg binary cache keys (ABI hashes) include the
# compiler version, so an unpinned base would silently invalidate the shared
# cache whenever upstream rebuilds the tag.
FROM ubuntu:24.04@sha256:33ceb71981b602c1a7443a53469e4dba065f7503eab3078a2d7a57a2ab987517 AS toolchain
ARG DEBIAN_FRONTEND=noninteractive
ARG PREFIX=/opt/deephaven

# Toolchain, vcpkg host requirements (git/curl/zip/unzip/tar/pkg-config), and
# host tools some vcpkg ports require but refuse to fetch themselves on Linux
# (thrift needs flex/bison; GitHub-hosted runners preinstall these, which is
# why host-side CI builds never noticed). mono runs vcpkg's nuget.exe.
# Noble's cmake (3.28) suits everyone: arrow requires >= 3.25, while cmake 4.x
# breaks dependencies whose cmake_minimum_required is too old.
RUN set -eux; \
    apt-get -qq update; \
    TZ=Etc/UTC apt-get -qq -y --no-install-recommends install \
        locales \
        tzdata \
        ca-certificates \
        curl \
        wget \
        gpg \
        git \
        g++ \
        make \
        cmake \
        build-essential \
        gzip \
        zip \
        unzip \
        tar \
        pkg-config \
        ninja-build \
        flex \
        bison \
        autoconf \
        automake \
        libtool \
        zlib1g-dev \
        libssl-dev \
        dwz \
        mono-complete \
        ; \
    echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen; \
    locale-gen en_US.UTF-8; \
    rm -rf /var/lib/apt/lists/*

# R toolchain: :R:rClient builds FROM the cpp-client image, which builds FROM
# this image. Package list matches the retired cpp-clients-multi-base.
RUN set -eux; \
    apt-get -qq update; \
    apt-get -qq -y --no-install-recommends install libuv1-dev libxml2-dev; \
    wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc \
        | gpg --dearmor -o /usr/share/keyrings/r-project.gpg; \
    echo "deb [signed-by=/usr/share/keyrings/r-project.gpg] https://cloud.r-project.org/bin/linux/ubuntu noble-cran40/" \
        > /etc/apt/sources.list.d/r-project.list; \
    apt-get -qq update; \
    apt-get -qq -y install r-base r-recommended pandoc; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    NCPUS=$(getconf _NPROCESSORS_ONLN); \
    MAKE="make -j${NCPUS}" R --no-save --no-restore <<'EOF'
status = tryCatch(
  {
     install.packages(c("Rcpp", "arrow", "R6", "dplyr", "testthat", "xml2", "lubridate", "zoo", "knitr", "rmarkdown"), repos="https://cloud.r-project.org", quiet=TRUE)
     0
  },
  error=function(e) { print(e); 1 },
  warning=function(w) { print(w); 2 }
)
print(paste0('status=', status))
quit(save='no', status=status)
EOF

#
# Dependencies stage: vcpkg at the pinned registry baseline, then build (or
# fetch from the nuget binary cache) everything in vcpkg.json.
#
FROM toolchain AS deps
ARG PREFIX=/opt/deephaven
# The commit of microsoft/vcpkg to use: must match the builtin-baseline in
# vcpkg.json (the Gradle build passes it in from that file). Pinning the
# tool to the baseline keeps ABI hashes stable.
ARG VCPKG_BASELINE
ARG TARGET_TRIPLET=x64-linux-dynamic-release
# NuGet binary caching against GitHub Packages. Requires authentication even
# for reads, so it is only enabled when a token is provided (CI). Without a
# token, vcpkg builds everything from source.
ARG GH_PACKAGES_FEED=
ARG GH_PACKAGES_USERNAME=deephaven
ARG VCPKG_NUGET_MODE=read

# Full clone on purpose: builtin-baseline versioning resolves port versions
# through the clone's own git history ("failed to unpack tree object" /
# "vcpkg was cloned as a shallow repository" otherwise). The layer is cached
# and only rebuilt when the baseline changes.
RUN set -eux; \
    [ -n "$VCPKG_BASELINE" ]; \
    git clone -q https://github.com/microsoft/vcpkg.git /opt/vcpkg; \
    git -C /opt/vcpkg -c advice.detachedHead=false checkout -q "$VCPKG_BASELINE"; \
    /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics

COPY vcpkg.json /opt/dh-manifest/
COPY custom-triplets/ /opt/dh-manifest/custom-triplets/

# The cache mount persists vcpkg's local binary cache (completed package
# archives) on the build host, outside the docker layer system: a deps build
# that fails partway through, or a Dockerfile edit, does not throw away the
# packages already built. Restores are keyed by vcpkg's ABI hash, so they are
# exact-match only. On fresh machines (and CI runners) it is simply empty.
#
# No 'set -x' here: the nuget commands would echo the token into the build log.
RUN --mount=type=secret,id=gh_packages_token \
    --mount=type=cache,target=/root/.cache/vcpkg/archives \
    set -eu; \
    binary_sources=""; \
    if [ -s /run/secrets/gh_packages_token ] && [ -n "$GH_PACKAGES_FEED" ]; then \
        echo "Configuring nuget binary caching against $GH_PACKAGES_FEED (mode: $VCPKG_NUGET_MODE)"; \
        nuget_exe=$(/opt/vcpkg/vcpkg fetch nuget | tail -n 1); \
        mono "$nuget_exe" sources add \
            -Source "$GH_PACKAGES_FEED" \
            -StorePasswordInClearText \
            -Name GitHubPackages \
            -UserName "$GH_PACKAGES_USERNAME" \
            -Password "$(cat /run/secrets/gh_packages_token)" \
            -NonInteractive >/dev/null; \
        mono "$nuget_exe" setapikey "$(cat /run/secrets/gh_packages_token)" \
            -Source "$GH_PACKAGES_FEED" -NonInteractive >/dev/null; \
        binary_sources="default,readwrite;nuget,$GH_PACKAGES_FEED,$VCPKG_NUGET_MODE"; \
    else \
        echo "No GitHub Packages token/feed provided; building from source (local archives cache still applies)"; \
    fi; \
    cd /opt/dh-manifest; \
    VCPKG_BINARY_SOURCES="$binary_sources" /opt/vcpkg/vcpkg install \
        --triplet "$TARGET_TRIPLET" \
        --overlay-triplets=/opt/dh-manifest/custom-triplets \
        --x-install-root=/opt/vcpkg_installed \
        --x-abi-tools-use-exact-versions \
        --clean-after-build; \
    rm -rf /opt/vcpkg/downloads /opt/vcpkg/buildtrees /opt/vcpkg/packages

# Expose the dependencies under ${PREFIX} the same way build-dependencies.sh
# used to, so downstream consumers (R's Makevars uses $DHCPP/lib,
# $DHCPP/include and $DHCPP/lib/pkgconfig) keep working unchanged. Symlinks
# keep the image lean; consumers that need a self-contained tree should
# dereference on export (tar -h).
RUN set -eux; \
    mkdir -p "$PREFIX/include" "$PREFIX/lib" "$PREFIX/bin" "$PREFIX/log"; \
    ln -s /opt/vcpkg_installed/"$TARGET_TRIPLET"/include/* "$PREFIX/include/"; \
    ln -s /opt/vcpkg_installed/"$TARGET_TRIPLET"/lib/* "$PREFIX/lib/"; \
    { \
        echo "DHCPP=\"$PREFIX\"; export DHCPP"; \
        echo "CMAKE_PREFIX_PATH=\"$PREFIX\"; export CMAKE_PREFIX_PATH"; \
        echo 'NCPUS=`getconf _NPROCESSORS_ONLN`; export NCPUS'; \
        echo "LD_LIBRARY_PATH=\"$PREFIX/lib\"; export LD_LIBRARY_PATH"; \
    } > "$PREFIX/env.sh"

ENV DH_PREFIX=/opt/deephaven \
    LD_LIBRARY_PATH=/opt/deephaven/lib \
    VCPKG_ROOT=/opt/vcpkg \
    DH_VCPKG_INSTALLED=/opt/vcpkg_installed \
    DH_VCPKG_TARGET_TRIPLET=${TARGET_TRIPLET}
