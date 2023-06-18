import deephaven.stream.kafka.cdc as cc
from deephaven import kafka_consumer as ck
from deephaven import kafka_producer as pk
from deephaven.stream.kafka.consumer import TableType, KeyValueSpec
from deephaven import dtypes as dh
from deephaven import agg as agg
from deephaven.experimental import time_window

server_name = 'mysql'
db_name='shop'

kafka_base_properties = {
    'group.id' : 'dh-server',
    'bootstrap.servers' : 'redpanda:9092',
    'schema.registry.url' : 'http://redpanda:8081',
}


def make_cdc_table(table_name:str):
    return cc.consume(
        kafka_base_properties,
        cc.cdc_short_spec(server_name,
                          db_name,
                          table_name)
    )

users = make_cdc_table('users')
items = make_cdc_table('items')
purchases = make_cdc_table('purchases')

consume_properties = {
    **kafka_base_properties,
    **{
        'deephaven.partition.column.name' : '',
        'deephaven.timestamp.column.name' : '',
        'deephaven.offset.column.name' : ''
    }
}

pageviews = ck.consume(
    consume_properties,
    topic = 'pageviews',
    offsets = ck.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec = KeyValueSpec.IGNORE,
    value_spec = ck.json_spec([ ('user_id', dh.int_),
                      ('url', dh.string),
                      ('channel', dh.string),
                      ('received_at', dh.DateTime) ]),
    table_type = TableType.Append
)

pageviews_stg = pageviews \
    .update_view([
        'url_path = url.split(`/`)',
        'pageview_type = url_path[1]',
        'target_id = Long.parseLong(url_path[2])'
    ]).drop_columns('url_path')

purchases_by_item = purchases.agg_by(
    [
        agg.sum_(['revenue = purchase_price']),
        agg.count_('orders'),
        agg.sum_(['items_sold = quantity'])
    ],
    'item_id'
)

pageviews_by_item = pageviews_stg \
    .where(['pageview_type = `products`']) \
    .count_by('pageviews', ['item_id = target_id'])

item_summary = items \
    .view(['item_id = id', 'name', 'category']) \
    .natural_join(purchases_by_item, on = ['item_id']) \
    .natural_join(pageviews_by_item, on = ['item_id']) \
    .drop_columns('item_id') \
    .move_columns_down(['revenue', 'pageviews']) \
    .update_view(['conversion_rate = orders / (double) pageviews'])

# These two 'top_*' tables match the 'Business Intelligence: Metabase' / dashboard
# part of the original example.
top_viewed_items = item_summary \
        .sort_descending('pageviews') \
        .head(20)

top_converting_items = item_summary \
    .sort_descending('conversion_rate') \
    .head(20)

minute_in_nanos = 60 * 1000 * 1000 * 1000

profile_views_per_minute_last_10 = \
    time_window(
        pageviews_stg.where(['pageview_type = `profiles`']),
        ts_col='received_at',
        window=10*minute_in_nanos,
        bool_col='in_last_10min'
    ).where(
        ['in_last_10min = true']
    ).update_view(
        ['received_at_minute = lowerBin(received_at, minute_in_nanos)']
    ).view(
        ['user_id = target_id',
        'received_at_minute']
    ).count_by(
        'pageviews',
        ['user_id',
        'received_at_minute']
    ).sort(
        ['user_id',
        'received_at_minute']
    )

profile_views = pageviews_stg \
    .view([
        'owner_id = target_id',
        'viewer_id = user_id',
        'received_at'
    ]).sort([
        'received_at'
    ]).tail_by(10, 'owner_id')

profile_views_enriched = profile_views \
    .natural_join(users, on = ['owner_id = id'], joins = ['owner_email = email']) \
    .natural_join(users, on = ['viewer_id = id'], joins = ['viewer_email = email']) \
    .move_columns_down('received_at')

dd_flagged_profiles = ck.consume(
    consume_properties,
    topic = 'dd_flagged_profiles',
    offsets = ck.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec = KeyValueSpec.IGNORE,
    value_spec = ck.simple_spec('user_id_str', dh.string),
    table_type = TableType.Append
).view(['user_id = Long.parseLong(user_id_str.substring(1, user_id_str.length() - 1))'])  # strip quotes

dd_flagged_profile_view = dd_flagged_profiles \
    .join(pageviews_stg, 'user_id')

high_value_users = purchases \
    .update_view([
        'purchase_total = purchase_price * quantity'
    ]).agg_by([
            agg.sum_(['lifetime_value = purchase_total']),
            agg.count_('purchases')
        ],
        'user_id'
    ) \
    .where(['lifetime_value > 10000']) \
    .natural_join(users, 
                  ['user_id = id'], ['email']) \
    .view(['id = user_id', 'email', 'lifetime_value', 'purchases'])  # column rename and reorder

schema_namespace = 'io.deephaven.examples'

cancel_callback = pk.produce(
    high_value_users,
    kafka_base_properties,
    topic = 'high_value_users_sink',
    key_spec = pk.avro_spec(
        'high_value_users_sink_key',
        publish_schema = True,
        schema_namespace = schema_namespace,
        include_only_columns = [ 'user_id' ]
    ),
    value_spec = pk.avro_spec(
        'high_value_users_sink_value',
        publish_schema = True,
        schema_namespace = schema_namespace,
        column_properties = {
            "lifetime_value.precision" : "12",
            "lifetime_value.scale" : "4"
        }
    ),
    last_by_key_columns = True
)

hvu_test = ck.consume(
    consume_properties,
    topic = 'high_value_users_sink',
    offsets = ck.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec = KeyValueSpec.IGNORE,
    value_spec = ck.avro_spec('high_value_users_sink_value'),
    table_type = TableType.Append
)

pageviews_summary = pageviews_stg \
    .agg_by(
        [
            agg.count_('total'),
            agg.max_(['max_received_at = received_at'])
        ]) \
    .update(['dt_ms = (DateTimeUtils.now() - max_received_at)/1_000_000.0'])
