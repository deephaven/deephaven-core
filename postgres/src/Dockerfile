FROM postgres:latest
RUN mkdir -p /tmp/psql_data/

COPY crypto_trades.csv /tmp/psql_data/
COPY gen_test_data.sql /docker-entrypoint-initdb.d/
