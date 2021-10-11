FROM deephaven/grpc-api:local-build

COPY requirements.txt /tmp/requirements.txt
RUN set -eux; \
    pip install -r /tmp/requirements.txt; \
    rm /tmp/requirements.txt