# check InvalidDefaultArgInFrom=skip
ARG DEEPHAVEN_SERVER_IMAGE
FROM ${DEEPHAVEN_SERVER_IMAGE}

# Numba does not yet support NumPy 2.5, but the base server image ships NumPy 2.5.
# Pin NumPy below 2.5 so the server's Python interpreter loads a numba-compatible
# version at startup. The Numba examples then install numba at runtime against this
# NumPy version and import successfully.
RUN pip install --no-cache-dir 'numpy<2.5'
