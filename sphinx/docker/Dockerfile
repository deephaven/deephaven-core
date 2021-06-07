# Start with an image that has what we need to build the documentation
# A better image would be something with the wheels, but not named as the base grpc-api image
FROM deephaven/runtime-base:local-build

# install sphinx
RUN set -eux; \
    pip3 install sphinx==3.5.4

# Copy config and templates
COPY source /source

# set up a directory to dump output in
RUN set -eux; \
  mkdir /build; \
  sphinx-build -b html /source /build
