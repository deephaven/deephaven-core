FROM deephaven/server-base:local-build

# Note: all of the pip installs have the --no-index flag, with the expectation that all external dependencies are
# already satisfied via the base image. If that is not the case, we want the install to error out, and we'll need to
# update the base image with the extra requirements before proceeding here.

COPY wheels/ /wheels
RUN set -eux; \
    python -m pip install -q --no-index --no-cache-dir /wheels/*.whl; \
    rm -r /wheels
