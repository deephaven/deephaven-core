#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

"""The kafka module provides the utilities to both consume and produce Kakfa streams."""

import jpy

from deephaven import DHError
from deephaven.jcompat import j_properties

_JKafkaTools = jpy.get_type("io.deephaven.kafka.KafkaTools")


def topics(kafka_config: dict) -> list[str]:
    """Returns a list of topic names available from Kafka.

    Args:
        kafka_config (dict): configuration for the associated Kafka consumer.
            The content is used to call the constructor of org.apache.kafka.clients.Admin;
            pass any Admin specific desired configuration here

    Returns:
        a list of topic names

    Raises:
        DHError
    """
    try:
        j_kafka_config = j_properties(kafka_config)
        jtopics = _JKafkaTools.listTopics(j_kafka_config)
        return list(jtopics)
    except Exception as e:
        raise DHError(e, "failed to list Kafka topics") from e
