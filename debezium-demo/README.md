Debezium - Kafka Demo
=====================

Start docker-compose with the compose file in this
directory, then start a DH Web console in python and try:

```
import deephaven.ConsumeCdc as cc
server_name = 'mysql'; db_name='shop'; table_name = 'purchases'
purchases = cc.consumeToTable(
    {'bootstrap.servers' : 'redpanda:9092',
     'schema.registry.url' : 'http://redpanda:8081'},
    cc.cdc_short_spec(server_name,
    db_name,
    table_name))
```

If you browse `loadgen/generate_load.py`, you can find
other table names in the file to play with.

At the beginning of the file the constant `purchaseGenEveryMS`
controls how frequently purchases are triggered.

Attributions
============

Files in this directory are based on demo code by
Debezium, Redpanda, and Materialize

* Debezium https://github.com/debezium/debezium
* Redpanda https://github.com/vectorizedio/redpanda
* Materialize https://github.com/MaterializeInc/materialize
