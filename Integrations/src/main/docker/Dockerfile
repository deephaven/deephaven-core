FROM deephaven/java-and-python:local-build as install-wheels

RUN set -eux; \
    apt-get -qq update; \
    apt-get -qq -y --no-install-recommends install liblzo2-2 curl; \
    rm -rf /var/lib/apt/lists/*

COPY deephaven-jpy-wheel/ /deephaven-jpy-wheel
RUN set -eux; \
    python3 -m pip install -q --no-cache-dir /deephaven-jpy-wheel/*.whl; \
    rm -r /deephaven-jpy-wheel

COPY deephaven-wheel/ /deephaven-wheel
RUN set -eux; \
    python3 -m pip install -q --no-cache-dir /deephaven-wheel/*.whl; \
    rm -r /deephaven-wheel

COPY deephaven2-wheel/ /deephaven2-wheel
RUN set -eux; \
    python3 -m pip install -q --no-cache-dir /deephaven2-wheel/*.whl; \
    rm -r /deephaven2-wheel

FROM deephaven/java-and-python:local-build
COPY --from=install-wheels / /