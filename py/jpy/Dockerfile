FROM deephaven/python-dev-base:local-build
WORKDIR /usr/src/app
COPY . .

ARG DEEPHAVEN_VERSION
RUN set -eux; \
    test -n "${DEEPHAVEN_VERSION}"; \
    python setup.py bdist_wheel
