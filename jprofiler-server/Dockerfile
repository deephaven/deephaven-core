ARG DEEPHAVEN_SERVER_IMAGE
FROM ${DEEPHAVEN_SERVER_IMAGE}

# Replace with the desired version of jprofiler below.
RUN set -eux; \
    curl -o /tmp/jprofiler.tgz https://download-gcdn.ej-technologies.com/jprofiler/jprofiler_linux_13_0.tar.gz; \
    tar -xzf /tmp/jprofiler.tgz -C /opt; \
    rm -f /tmp/jprofiler.tgz
