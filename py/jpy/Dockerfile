# azul/zulu-open-jdk-debian bumped OS versions
# https://github.com/zulu-openjdk/zulu-openjdk/commit/8e242a8838b8a5f068193719695b2ed918d402c5
# which causes downstream dependency failures (no more python3.7 for example).
# For now, we are pinning to a known working version, which used to be their 8u302 tag.
FROM docker.io/azul/zulu-openjdk-debian@sha256:75c283c625e4403fbf10a60642fc2fcac70b5ac55213f77c6453bbe1fda4a8c7 as runtime_reqs
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      python3.7 \
      libpython3.7 \
      python3-pip \
      ; \
    rm -rf /var/lib/apt/lists/*; \
    pip3 install --upgrade pip

FROM runtime_reqs as build_reqs
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      maven \
      build-essential \
      python3.7-dev \
      python3-wheel \
      python3-setuptools \
      ; \
    rm -rf /var/lib/apt/lists/*

FROM build_reqs as sources
WORKDIR /usr/src/app
COPY . .

FROM sources as build
ARG DEEPHAVEN_VERSION
RUN set -eux; \
    test -n "${DEEPHAVEN_VERSION}"; \
    python3.7 setup.py bdist_wheel

FROM runtime_reqs
COPY --from=build /usr/src/app/dist/ .
RUN set -eux; \
    pip3 install *.whl; \
    rm *.whl