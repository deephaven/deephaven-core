# **Slick data ingestion**

This notebook will demonstrate the value of the Deephaven-uri package canonicalizes a [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier) format for
[Deephaven target](src/main/java/io/deephaven/uri/DeephavenTarget.java) and
[resource](src/main/java/io/deephaven/uri/StructuredUri.java) identification.
\
The goal is to provide a string format that can be easy referenced and shared across client and server contexts.
\
When using URI, the resource format allows for a concise representation of resource identification, both local and remote. 

| URI                                               | Deephaven target                               | Type                    | Type-specific information                        |
|-------------------------------------------------- | ---------------------------------------------- | ----------------------- | ------------------------------------------------ |
| `dh:///scope/my_table`                            | (none)                                         | scope                   | variableName=`my_table`                          |
| `dh://example.com/scope/my_table`                 | `dh://example.com`                             | remote + scope          | variableName=`my_table`                          |
| `dh:///app/my_app/field/my_field`                 | (none)                                         | app                     | applicationId=`my_app` fieldName=`my_field`      |
| `dh://example.com/app/my_app/field/my_field`      | `dh://example.com`                             | remote + app            | applicationId=`my_app` fieldName=`my_field`      |
| `dh://example.com/field/my_field`                 | `dh://example.com`                             | remote + field          | applicationId=`example.com` fieldName=`my_field` |
| `dh://gateway?uri=dh://host/scope/my_table`       | `dh://gateway` (local) + `dh://host` (gateway) | remote + remote + scope | variableName=`my_table`                          |
| `custom:///foo?bar=baz`                           | (none)                                         | custom                  | (custom)                                         |
| `dh://host?uri=custom%3A%2F%2F%2Ffoo%3Fbar%3Dbaz` | `dh://host`                                    | remote + custom         | (custom)                                         |

\
\
\
Start by importing some requisite packages. 

```python
from deephaven.ResolveTools import resolve
```
\
\
In the prior notebook you created a ticking table of crypto prices.  We are going to create a URI based on that table. 
\

```python
from deephaven import ConsumeKafka as ck

def get_trades(*, offsets, table_type):
    return ck.consumeToTable(
        {  'bootstrap.servers' : 'demo-kafka.c.deephaven-oss.internal:9092',
          'schema.registry.url' : 'http://demo-kafka.c.deephaven-oss.internal:8081' },
        'io.deephaven.crypto.kafka.TradesTopic',
        key = ck.IGNORE,
        value = ck.avro('io.deephaven.crypto.kafka.TradesTopic-io.deephaven.crypto.Trade'),
        offsets=offsets,
        table_type=table_type)

latest_offset = get_trades(offsets=ck.ALL_PARTITIONS_SEEK_TO_END, table_type='stream')

```


[Deephaven URIs](src/main/java/io/deephaven/uri/DeephavenUri.java) are those that have the scheme `dh` and no host,
or `dh`/`dh+plain` and a host; and represent Deephaven-specific concepts. Unless configured otherwise, Deephaven servers
should be able to resolve Deephaven URIs into appropriate resources.

\
\
\

Create a Deephaven table `new_offest` using the `latest_offset` table from the scope.

```python
new_offest = resolve('dh:///scope/latest_offset')
```
\
\
\
Since this table `new_offest` is based on `latest_offset` as new data enters `latest_offset` the table `new_offest` will have access to those updates.

If `latest_offset` changes its definition after the URI such as with a filter, or manual update then the downstream URI will not update.

```python
latest_offset = latest_offset.where("KafkaOffset%2==0")
```

This is useful for data sets.... 
