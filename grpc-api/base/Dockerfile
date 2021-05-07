FROM deephaven/deephaven-jpy-wheel as deephaven-jpy-wheel

FROM deephaven/deephaven-wheel as deephaven-wheel

FROM deephaven/java-and-python
COPY --from=deephaven-jpy-wheel /usr/src/app/dist/ .
COPY --from=deephaven-wheel /usr/src/app/dist/ .
RUN set -eux; \
  pip3 install setuptools wheel; \
  pip3 install *.whl; \
  rm *.whl