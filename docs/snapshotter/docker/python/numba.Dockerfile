# check InvalidDefaultArgInFrom=skip
ARG DEEPHAVEN_SERVER_IMAGE
FROM ${DEEPHAVEN_SERVER_IMAGE}

# The 41.3 through 42.x server images don't include Numba, so install it here.
# Server images from 43.0 include Numba, and this install is then a no-op.
RUN pip install --no-cache-dir numba
