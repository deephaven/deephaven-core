FROM deephaven/grpc-api:local-build

USER root

COPY requirements.txt /tmp/requirements.txt

RUN set -eux; \
    pip install -r /tmp/requirements.txt; \
    rm /tmp/requirements.txt

# Add the extra libraries to the classpath
COPY libs/ /app/libs

# Add the application directory
COPY app.d/ /app.d

USER 1000