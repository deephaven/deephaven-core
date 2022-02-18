--
-- Materialize demo script.
-- See https://github.com/MaterializeInc/ecommerce-demo/blob/main/README_RPM.md
--

CREATE SOURCE purchases
FROM KAFKA BROKER 'redpanda:9092' TOPIC 'mysql.shop.purchases'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://redpanda:8081'
ENVELOPE DEBEZIUM;

CREATE SOURCE items
FROM KAFKA BROKER 'redpanda:9092' TOPIC 'mysql.shop.items'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://redpanda:8081'
ENVELOPE DEBEZIUM;

CREATE SOURCE users
FROM KAFKA BROKER 'redpanda:9092' TOPIC 'mysql.shop.users'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://redpanda:8081'
ENVELOPE DEBEZIUM;

CREATE SOURCE json_pageviews
FROM KAFKA BROKER 'redpanda:9092' TOPIC 'pageviews'
FORMAT BYTES;

CREATE VIEW pageview_stg AS
    SELECT
        *,
        regexp_match(url, '/(products|profiles)/')[1] AS pageview_type,
        (regexp_match(url, '/(?:products|profiles)/(\d+)')[1])::INT AS target_id
    FROM (
        SELECT
            (data->'user_id')::INT AS user_id,
            data->>'url' AS url,
            data->>'channel' AS channel,
            (data->>'received_at')::double AS received_at
        FROM (
            SELECT CAST(data AS jsonb) AS data
            FROM (
                SELECT convert_from(data, 'utf8') AS data
                FROM json_pageviews
            )
        )
    );

CREATE MATERIALIZED VIEW purchases_by_item AS
     SELECT
         item_id,
         SUM(purchase_price) as revenue,
         COUNT(id) AS orders,
         SUM(quantity) AS items_sold
     FROM purchases GROUP BY 1;

CREATE MATERIALIZED VIEW pageviews_by_item AS
    SELECT
        target_id as item_id,
        COUNT(*) AS pageviews
    FROM pageview_stg
    WHERE pageview_type = 'products'
    GROUP BY 1;

CREATE MATERIALIZED VIEW item_summary AS
    SELECT
        items.name,
        items.category,
        SUM(purchases_by_item.items_sold) as items_sold,
        SUM(purchases_by_item.orders) as orders,
        SUM(purchases_by_item.revenue) as revenue,
        SUM(pageviews_by_item.pageviews) as pageviews,
        SUM(purchases_by_item.orders) / SUM(pageviews_by_item.pageviews)::FLOAT AS conversion_rate
    FROM items
    JOIN purchases_by_item ON purchases_by_item.item_id = items.id
    JOIN pageviews_by_item ON pageviews_by_item.item_id = items.id
    GROUP BY 1, 2;

CREATE MATERIALIZED VIEW profile_views_per_minute_last_10 AS
    SELECT
        target_id as user_id,
        date_trunc('minute', to_timestamp(received_at)) as received_at_minute,
        COUNT(*) as pageviews
    FROM pageview_stg
    WHERE
      pageview_type = 'profiles' AND
      mz_logical_timestamp() < (received_at*1000 + 600000)::numeric
    GROUP BY 1, 2;

CREATE MATERIALIZED VIEW profile_views AS
    SELECT
        target_id AS owner_id,
        user_id AS viewer_id,
        received_at AS received_at
    FROM (SELECT DISTINCT target_id FROM pageview_stg) grp,
    LATERAL (
        SELECT user_id, received_at FROM pageview_stg
        WHERE target_id = grp.target_id
        ORDER BY received_at DESC LIMIT 10
    );

CREATE MATERIALIZED VIEW profile_views_enriched AS
    SELECT
        owner.id as owner_id,
        owner.email as owner_email,
        viewers.id as viewer_id,
        viewers.email as viewer_email,
        profile_views.received_at
    FROM profile_views
    JOIN users owner ON profile_views.owner_id = owner.id
    JOIN users viewers ON profile_views.viewer_id = viewers.id;

CREATE MATERIALIZED SOURCE dd_flagged_profiles
FROM KAFKA BROKER 'redpanda:9092' TOPIC 'dd_flagged_profiles'
FORMAT TEXT
ENVELOPE UPSERT;

CREATE MATERIALIZED VIEW dd_flagged_profile_view AS
    SELECT pageview_stg.*
    FROM dd_flagged_profiles
    JOIN pageview_stg ON user_id = btrim(text, '"')::INT;

CREATE MATERIALIZED VIEW high_value_users AS
  SELECT
    users.id,
    users.email,
    SUM(purchase_price * quantity)::int AS lifetime_value,
    COUNT(*) as purchases
  FROM users
  JOIN purchases ON purchases.user_id = users.id
  GROUP BY 1,2
  HAVING SUM(purchase_price * quantity) > 10000;

CREATE SINK high_value_users_sink
    FROM high_value_users
    INTO KAFKA BROKER 'redpanda:9092' TOPIC 'high-value-users-sink'
    WITH (reuse_topic=true, consistency_topic='high-value-users-sink-consistency')
    FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://redpanda:8081';

CREATE MATERIALIZED SOURCE hvu_test
FROM KAFKA BROKER 'redpanda:9092' TOPIC 'high-value-users-sink'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://redpanda:8081';

-- SELECT * FROM hvu_test LIMIT 2;

CREATE MATERIALIZED VIEW pageviews_summary AS
    SELECT
        COUNT(*) AS total, MAX(received_at) AS max_received_at
    FROM pageview_stg;

/*
SELECT
    total,
    to_timestamp(max_received_at) max_received_ts,
    mz_logical_timestamp()/1000.0 AS logical_ts_ms,
    mz_logical_timestamp()/1000.0 - max_received_at AS dt_ms
FROM pageviews_summary;
*/
