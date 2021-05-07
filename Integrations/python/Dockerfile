FROM docker.io/library/python:3.7.10 as sources
WORKDIR /usr/src/app
COPY . .

# TODO (core#241): Integration/python tests via docker
#FROM sources as test
#ARG DEEPHAVEN_VERSION
#RUN set -eux; \
#    test -n "${DEEPHAVEN_VERSION}"; \
#    python setup.py test

FROM sources as build
ARG DEEPHAVEN_VERSION
RUN set -eux; \
    test -n "${DEEPHAVEN_VERSION}"; \
    python setup.py bdist_wheel