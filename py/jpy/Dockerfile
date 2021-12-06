FROM deephaven/java-and-python:local-build
RUN set -eux; \
    echo 'deb http://deb.debian.org/debian buster-backports main' > /etc/apt/sources.list.d/backports.list; \
    apt-get -qq update; \
    apt-get -qq -y --no-install-recommends -t buster-backports install maven; \
    apt-get -qq -y --no-install-recommends install \
      build-essential \
      python3.7-dev \
      ; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app
COPY . .

ARG DEEPHAVEN_VERSION
RUN set -eux; \
    test -n "${DEEPHAVEN_VERSION}"; \
    python3.7 setup.py bdist_wheel
