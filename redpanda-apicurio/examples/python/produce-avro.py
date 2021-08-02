from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

import sys

value_arg_form = "type:field=value"

if len(sys.argv) < 4:
    print("Usage: " + sys.argv[0] + " topic-name avro-schema-file-path " +
          value_arg_form + " [ " + value_arg_form + " ...]", file=sys.stderr)
    sys.exit(1)

topic_name = sys.argv[1]

with open(sys.argv[2], 'r') as file:
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
for value_arg in sys.argv[3:]:
    s = value_arg.split(':', 1)
    if len(s) != 2:
        wrong_form(value_arg)
    type_for_value_as_string = s[0]
    field_eq_value = s[1]
    s = field_eq_value.split('=', 1)
    if len(s) != 2:
        wrong_form(value_arg)
    if (type_for_value_as_string == "str"):
        value[s[0]] = s[1]
    else:
        cast = type_for_value_as_string + "('" + s[1] + "')"
        exec("v=" + cast)
        value[s[0]] = v

avroProducer.produce(topic=topic_name, key=None, value=value)
avroProducer.flush()
