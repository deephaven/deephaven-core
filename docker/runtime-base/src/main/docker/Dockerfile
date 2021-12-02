FROM deephaven/java-and-python:local-build as install-wheels

RUN set -eux; \
    apt-get -qq update; \
    apt-get -qq -y --no-install-recommends install liblzo2-2 curl; \
    rm -rf /var/lib/apt/lists/*

# To update the requirements.txt:
# 1. Comment out BLOCK 1
# 2. Remove the --no-index arguments from the RUNs
# 3. Run `./gradlew docker-runtime-base:build`
# 4. Run `docker run --rm -it deephaven/runtime-base:local-build pip freeze | grep -v " @ file" > docker/runtime-base/src/main/docker/requirements.txt`
# 5. Replace the --no-index arguments from the RUNs
# 6. Uncomment BLOCK 1

# START BLOCK 1
COPY requirements.txt requirements.txt
RUN set -eux; \
    python3 -m pip install --no-cache-dir -r requirements.txt; \
    rm requirements.txt
# END BLOCK 1

COPY deephaven-jpy-wheel/ /deephaven-jpy-wheel
RUN set -eux; \
    python3 -m pip install -q --no-index --no-cache-dir /deephaven-jpy-wheel/*.whl; \
    rm -r /deephaven-jpy-wheel

COPY deephaven-wheel/ /deephaven-wheel
RUN set -eux; \
    python3 -m pip install -q --no-index --no-cache-dir /deephaven-wheel/*.whl; \
    rm -r /deephaven-wheel

COPY deephaven2-wheel/ /deephaven2-wheel
RUN set -eux; \
    python3 -m pip install -q --no-index --no-cache-dir /deephaven2-wheel/*.whl; \
    rm -r /deephaven2-wheel

FROM deephaven/java-and-python:local-build
COPY --from=install-wheels / /