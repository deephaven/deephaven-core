def get_server_config():
    return {
        'bootstrap.servers' : 'demo-kafka.c.deephaven-oss.internal:9092',
        'schema.registry.url' : 'http://demo-kafka.c.deephaven-oss.internal:8081'
    }

def get_trades(*, offsets, table_type):
    from deephaven import KafkaTools as kt
    return kt.consumeToTable(
        get_server_config(),
        'io.deephaven.crypto.kafka.TradesTopic',
        key = kt.IGNORE,
        value = kt.avro('io.deephaven.crypto.kafka.TradesTopic-io.deephaven.crypto.Trade'),
        offsets=offsets,
        table_type=table_type)

def get_quotes(*, offsets, table_type):
    from deephaven import KafkaTools as kt
    return kt.consumeToTable(
        get_server_config(),
        'io.deephaven.crypto.kafka.QuotesTopic',
        key = kt.IGNORE,
        value = kt.avro('io.deephaven.crypto.kafka.QuotesTopic-io.deephaven.crypto.Quote'),
        offsets=offsets,
        table_type=table_type)
