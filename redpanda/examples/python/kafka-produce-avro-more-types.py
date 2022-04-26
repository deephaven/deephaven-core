from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

import datetime
import sys
import time

value_arg_form = "python_type:field=value"

if len(sys.argv) < 4:
    print("Usage: " + sys.argv[0] + " topic-name partition avro-schema-file-path ", file=sys.stderr)
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

t2 = datetime.datetime.now(tz=datetime.timezone.utc)
t1 = t2 - datetime.timedelta(seconds=1)
t0 = t1 - datetime.timedelta(seconds=5)

value = {
    "OneMap" : { "a" : 1, "b" : 2 },
    "OneUnion" : 6,
    "OneEnum" : "HEARTS",
    "OneArray" : [ [ 1, 2 ], [ 3, 4 ] ],
    "AnotherArray" : [ t0, t1, t2 ],
}
avroProducer.produce(topic=topic_name, partition=partition, key=None, value=value)
avroProducer.flush()
