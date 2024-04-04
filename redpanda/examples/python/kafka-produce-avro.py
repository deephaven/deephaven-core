#
# Test driver to produce kafka messages using an avro schema.
#
# To run this script, you need confluent-kafka libraries installed.
# To create a dedicated venv for it, you can do:
#
# $ cd $SOMEWHERE_YOU_WANT_THIS_TO_LIVE
# $ mkdir confluent-kafka
# $ python3 -m venv confluent-kafka
# $ cd confluent-kafka
# $ source bin/activate
# $ pip3 install confluent-kafka
# $ pip3 install avro
#
# Note: On a Mac you may need to install the librdkafka package.
# You can use "brew install librdkafka" if the pip3 command fails
# with an error like "librdkafka/rdkafka.h' file not found"
# as found at confluentinc/confluent-kafka-python#166.
# You may also need the following (be sure to substitute the right version of librdkafka):
# export C_INCLUDE_PATH=/opt/homebrew/Cellar/librdkafka/1.9.0/include
# export LIBRARY_PATH=/opt/homebrew/Cellar/librdkafka/1.9.0/lib
#
# Examples of use for DH testing together with web UI.
#
# == Common to all:
#
#  * Start the redpanda compose: (cd redpanda && docker compose up --build)
#  * From web UI do:
#
#    > from deephaven import kafka_consumer as kc
#    > from deephaven.stream.kafka.consumer import TableType
#
# == Example (1)
#
# Load a schema into schema registry for share_price_record.
# From the command line in the host (not on a docker image), run:
#
#    $ sh ./post-share-price-schema.sh
#
# The last command above should have loaded the avro schema in the file avro/share_price.json
# to the schema registry. You can check it was loaded visiting on the host the URL:
#   http://localhost:8081/subjects/share_price_record/versions/1
# That page should now list 'share_price_record' as an available schema.
#
# From the web IDE, run:
#
#    > t = kc.consume({'bootstrap.servers' : 'redpanda:29092', 'schema.registry.url' : 'http://redpanda:8081'}, 'share_price', value_spec=kc.avro_spec('share_price_record'), table_type=TableType.append())
#
# The last command above should create a table with columns: [ KafkaPartition, KafkaOffset, KafkaTimestamp, Symbol, Side, Qty, Price ]
# Run this script on the host (not on a docker image) to generate one row:
#
#    $ python3 ./kafka-produce-avro.py share_price 0 ../avro/share_price.json str:Symbol=MSFT str:Side=BUY double:Price=274.82 int:Qty=200
#
# You should see a new row show up in the web IDE with data matching the data sent above.
#
# == Example (2)
#
# Load a schema into schema registry for metric_sample_record.
# From the command line in the host (not on a docker image), run:
#
#    $ sh post-avro-schema.sh avro/metric_sample.json metric_sample_record
#
# The last command above should have loaded the avro schema in the file avro/metric_sample.json
# to the schema registry. You can check it was loaded visiting on the host the URL:
#   http://localhost:8081/subjects/metric_sample_record/versions/1
# That page should now list 'metric_sample_record' as an available schema.
#
# From the web IDE, run:
#
#    > t = kc.consume({'bootstrap.servers' : 'redpanda:29092', 'schema.registry.url' : 'http://redpanda:8081'}, 'metric_sample', value_spec=kc.avro_spec('metric_sample_record'), table_type=TableType.append())
#
# The last command above should create a table with columns: [ KafkaPartition, KafkaOffset, KafkaTimestamp, KafkaKey, Timestamp, Metric, Tags, Samples ]
# Run this script on the host (not on a docker image) to generate one row:
#
#
#    $ python3 ./kafka-produce-avro.py metric_sample 0 ../avro/metric_sample.json timestamp:Timestamp=now str:Metric=latency str:Tags=a=1,b=2,c=3 []double:Samples=1.0,2.0,3.0
#
# You should see a new row show up in the web IDE with data matching the data sent above.
#

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

import datetime
import sys
import time

value_arg_form = "python_type:field=value"

if len(sys.argv) < 5:
    print("Usage: " + sys.argv[0] + " topic-name partition avro-schema-file-path " +
          value_arg_form + " [ " + value_arg_form + " ...]", file=sys.stderr)
    sys.exit(1)

topic_name = sys.argv[1]
partition = int(sys.argv[2])

with open(sys.argv[3], 'r') as file:
    value_schema_str = file.read()

value_schema = avro.loads(value_schema_str)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

avroProducer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'on_delivery': delivery_report,
    'schema.registry.url': 'http://localhost:8081'
}, default_value_schema=value_schema)

def wrong_form(value_arg):
    print(sys.argv[0] + ": Error, argument " + value_arg +
          " is not of the form " + value_arg_form + ".", file=sys.stderr)
    sys.exit(1)

def is_array(ptype: str):
    return len(ptype) > 2 and ptype[0:2] == "[]"

def normalize_type(ptype: str):
    if is_array(ptype):
        ptype = ptype[2:]
        prefix = "[]"
    else:
        prefix = ""
    if ptype == "timestamp":
        return prefix + ptype
    if ptype == "str" or ptype == "string":
        return prefix + "str"
    if ptype == "bool" or ptype == "boolean":
        return prefix + "bool"
    if ptype == "double" or ptype == "float":
        return prefix + "float"
    if ptype == "long" or ptype == "short" or ptype == "int":
        return prefix + "int"
    raise "Unhandled type"

def value_from_str(ptype: str, svalue: str):
    # Strictly speaking we are calling for a python type here (eg, "str", "int", "float", "bool").
    # We allow other type names for ease of use for us, people accustumed to Java.
    if (ptype == "str"):
        return svalue
    if (ptype == "bool"):
        return (svalue == "true" or svalue == "True")
    if ptype == "timestamp":
        if svalue == "now":
            return datetime.datetime.now(tz=datetime.timezone.utc)
        raise "Unsupported timestamp."
    # Do a python cast of the string value to the right type via exec
    cast = ptype + "('" + svalue + "')"
    return eval(cast)

value = {}
for value_arg in sys.argv[4:]:
    s = value_arg.split(':', 1)
    if len(s) != 2:
        wrong_form(value_arg)
    ptype = normalize_type(s[0])
    field_eq_value = s[1]
    s = field_eq_value.split('=', 1)
    if len(s) != 2:
        wrong_form(value_arg)
    if is_array(ptype):
        svalues = s[1].split(',')
        value[s[0]] = [ value_from_str(ptype[2:], svalue) for svalue in svalues ]
    else:
        value[s[0]] = value_from_str(ptype, s[1])

avroProducer.produce(topic=topic_name, partition=partition, key=None, value=value)
avroProducer.flush()
