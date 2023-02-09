CREATE TABLE CRYPTO_TRADES(
    T_DATE date,
    T_TS bigint,
    T_ID varchar(20),
    T_INSTRUMENT varchar(10),
    T_EXCHANGE text,
    T_PRICE double precision,
    T_SIZE real
);

COPY CRYPTO_TRADES
FROM '/tmp/psql_data/crypto_trades.csv'
DELIMITER ','
CSV HEADER;
