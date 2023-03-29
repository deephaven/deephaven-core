#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides utilities useful for working with Kafka. """

from typing import Dict

import jpy

from deephaven import DHError
from deephaven.jcompat import j_properties


_JKafkaTools = jpy.get_type("io.deephaven.kafka.KafkaTools")

def topics(kafka_config: Dict):
    """Returns a list of topic names available from Kafka.

    Args:
        kafka_config (Dict): configuration for the associated Kafka consumer.
        The content is used to call the constructor of
        org.apache.kafka.clients.Admin; pass any Admin specific desired configuration here
    """
    try:
        kafka_config = j_properties(kafka_config)
        return _JKafkaTools.listTopics(kafka_config)
    except Exception as e:
        raise DHError(e, "failed to list Kafka topics") from e
