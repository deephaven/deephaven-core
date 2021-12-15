FROM deephaven/alpine:local-build as install

# Note(deephaven-core#599): Consider moving grpcwebproxy DL into gradle

ARG TARGETARCH
COPY arch/${TARGETARCH} /arch
RUN set -eux; \
    GRPCWEBPROXY_VERSION=v0.13.0 /arch/install.sh; \
    rm -r /arch

COPY contents/ .

FROM deephaven/alpine:local-build
COPY --from=install / /
EXPOSE 8080 8443
ENTRYPOINT ["/sbin/tini", "--", "/docker-entrypoint.sh"]
