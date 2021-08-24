#
# Test driver to produce kafka messages using an avro schema.
#
# To run this script, you need confluent-kafka libraries installed.
# To create a dedicated venv for it, you can do:
#
# $ mkdir confluent-kafka; cd confluent-kafka
# $ python3 -m venv confluent-kafka
# $ cd confluent-kafka
# $ source bin/activate
# $ pip3 install confluent-kafka
#
# Note: On a Mac you may need to install the librdkafka package.
# You can use "brew install librdkafka" if the pip3 command fails
# with an error like "librdkafka/rdkafka.h' file not found"
# as found at confluentinc/confluent-kafka-python#166.
#
# Examples of use for DH testing together with web UI.
#
# == Common to all:
#
#  * Start the redpanda-apicurio compose: (cd redpanda-apicurio && docker-compose up --build)
#  * From web UI do: > from deephaven import KafkaTools
#
# == Example (1)
#
# Load a schema into schema registry for share_price_record.
# From the command line in the host (not on a docker image), run:
# $ sh ../post-share-price-schema.sh
#
# The last command above should have loaded the avro schema in the file avro/share_price.json
# to the apicurio registry. You can check it was loaded visiting on the host the URL:
#   http://localhost:8081/ui/artifacts
# That page should now list 'share_price_record' as an available schema.
#
# From the web IDE, run:
# > from deephaven import KafkaTools
# > s = getAvroSchema('http://registry:8080/api/ccompat', 'share_price_record', '1')
# > t = consumeToTable({'bootstrap.servers' : 'redpanda:29092', 'schema.registry.url' : 'http://registry:8080/api/ccompat'}, 'share_price', value_avro_schema=s, table_type='append')
#
# The last command above should create a table with columns: [ KafkaPartition, KafkaOffset, KafkaTimestamp, Symbol, Price ]
#
# Run this script on the host (not on a docker image) to generate one row:
# $ python3 ./kafka-produce-avro.py share_price 0 avro/share_price.json str:Symbol=MSFT str:Side=BUY double:Price=274.82 int:Qty=200
#
# You should see a new row show up in the web IDE with data matching the data sent above.
#
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

import sys

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
    'schema.registry.url': 'http://localhost:8081/api/ccompat'
}, default_value_schema=value_schema)

def wrong_form(value_arg):
    print(sys.argv[0] + ": Error, argument " + value_arg +
          " is not of the form " + value_arg_form + ".", file=sys.stderr)
    sys.exit(1)

value = {}
for value_arg in sys.argv[4:]:
    s = value_arg.split(':', 1)
    if len(s) != 2:
        wrong_form(value_arg)
    ptype = s[0]
    field_eq_value = s[1]
    s = field_eq_value.split('=', 1)
    if len(s) != 2:
        wrong_form(value_arg)
    # Strictly speaking we are calling for a python type here (eg, "str", "int", "float", "bool").
    # We allow other type names for ease of use for us, people accostumed to Java.
    if (ptype == "str" or ptype == "string"):
        value[s[0]] = s[1]
    elif (ptype == "bool" or ptype == "boolean"):
        value[s[0]] = (s[1] == "true" or s[1] == "True")
    else:
        if ptype == "double":
            ptype = "float"
        elif ptype == "long" or ptype == "short":
            ptype = "int"
        # Do a python cast of the string value to the right type via exec
        cast = ptype + "('" + s[1] + "')"
        exec("v=" + cast)
        value[s[0]] = v

avroProducer.produce(topic=topic_name, partition=partition, key=None, value=value)
avroProducer.flush()
