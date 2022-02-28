import deephaven.ConsumeCdc as cc
import deephaven.ConsumeKafka as ck
import deephaven.ProduceKafka as pk
import deephaven.Types as dh
from deephaven import Aggregation as agg, as_list
import deephaven.TableManipulation.WindowCheck as wck

server_name = 'mysql'
db_name='shop'

kafka_base_properties = {
    'group.id' : 'dh-server',
    'bootstrap.servers' : 'redpanda:9092',
    'schema.registry.url' : 'http://redpanda:8081',
}


def make_cdc_table(table_name:str):
    return cc.consumeToTable(
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

pageviews = ck.consumeToTable(
    consume_properties,
    topic = 'pageviews',
    offsets = ck.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key = ck.IGNORE,
    value = ck.json([ ('user_id', dh.long_),
                      ('url', dh.string),
                      ('channel', dh.string),
                      ('received_at', dh.datetime) ]),
    table_type = 'append'
)

pageviews_stg = pageviews \
    .updateView(
        'url_path = url.split(`/`)',
        'pageview_type = url_path[1]',
        'target_id = Long.parseLong(url_path[2])'
    ).dropColumns('url_path')

purchases_by_item = purchases.aggBy(
    as_list([
        agg.AggSum('revenue = purchase_price'),
        agg.AggCount('orders'),
        agg.AggSum('items_sold = quantity')
    ]),
    'item_id'
)

pageviews_by_item = pageviews_stg \
    .where('pageview_type = `products`') \
    .countBy('pageviews', 'item_id = target_id')

item_summary = items \
    .view('item_id = id', 'name', 'category') \
    .naturalJoin(purchases_by_item, 'item_id') \
    .naturalJoin(pageviews_by_item, 'item_id') \
    .dropColumns('item_id') \
    .moveColumnsDown('revenue', 'pageviews') \
    .updateView('conversion_rate = orders / (double) pageviews')

# These two 'top_*' tables match the 'Business Intelligence: Metabase' / dashboard
# part of the original example.
top_viewed_items = item_summary \
    .sortDescending('pageviews') \
    .head(20)

top_converting_items = item_summary \
    .sortDescending('conversion_rate') \
    .head(20)

minute_in_nanos = 60 * 1000 * 1000 * 1000

profile_views_per_minute_last_10 = \
    wck.addTimeWindow(
        pageviews_stg.where('pageview_type = `profiles`'),
        'received_at',
        10*minute_in_nanos,
        'in_last_10min'
    ).where(
        'in_last_10min = true'
    ).updateView(
        'received_at_minute = lowerBin(received_at, minute_in_nanos)'
    ).view(
        'user_id = target_id',
        'received_at_minute'
    ).countBy(
        'pageviews',
        'user_id',
        'received_at_minute'
    ).sort(
        'user_id',
        'received_at_minute'
    )

profile_views = pageviews_stg \
    .view(
        'owner_id = target_id',
        'viewer_id = user_id',
        'received_at'
    ).sort(
        'received_at'
    ).tailBy(10, 'owner_id')

profile_views_enriched = profile_views \
    .naturalJoin(users, 'owner_id = id', 'owner_email = email') \
    .naturalJoin(users, 'viewer_id = id', 'viewer_email = email') \
    .moveColumnsDown('received_at')

dd_flagged_profiles = ck.consumeToTable(
    consume_properties,
    topic = 'dd_flagged_profiles',
    offsets = ck.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key = ck.IGNORE,
    value = ck.simple('user_id_str', dh.string),
    table_type = 'append'
).view('user_id = Long.parseLong(user_id_str.substring(1, user_id_str.length() - 1))')  # strip quotes

dd_flagged_profile_view = dd_flagged_profiles \
    .join(pageviews_stg, 'user_id')

high_value_users = purchases \
    .updateView(
        'purchase_total = purchase_price * quantity'
    ).aggBy(
        as_list([
            agg.AggSum('lifetime_value = purchase_total'),
            agg.AggCount('purchases'),
        ]),
        'user_id'
    ) \
    .where('lifetime_value > 10000') \
    .naturalJoin(users, 'user_id = id', 'email') \
    .view('id = user_id', 'email', 'lifetime_value', 'purchases')  # column rename and reorder

schema_namespace = 'io.deephaven.examples'

cancel_callback = pk.produceFromTable(
    high_value_users,
    kafka_base_properties,
    topic = 'high_value_users_sink',
    key = pk.avro(
        'high_value_users_sink_key',
        publish_schema = True,
        schema_namespace = schema_namespace,
        include_only_columns = [ 'user_id' ]
    ),
    value = pk.avro(
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

hvu_test = ck.consumeToTable(
    consume_properties,
    topic = 'high_value_users_sink',
    offsets = ck.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key = ck.IGNORE,
    value = ck.avro('high_value_users_sink_value'),
    table_type = 'append'
)

pageviews_summary = pageviews_stg \
    .aggBy(
        as_list([
            agg.AggCount('total'),
            agg.AggMax('max_received_at = received_at')])) \
    .updateView('dt_ms = (DateTime.now() - max_received_at)/1_000_000.0')
