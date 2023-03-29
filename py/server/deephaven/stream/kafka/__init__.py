#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" The kafka module provides the utilities to both consume and produce Kakfa streams. """

from typing import Dict, List

import jpy

from deephaven import DHError
from deephaven.jcompat import j_list_to_list, j_properties

_JKafkaTools = jpy.get_type("io.deephaven.kafka.KafkaTools")

def topics(kafka_config: Dict) -> List[str]:
    """Returns a list of topic names available from Kafka.

    Args:
        kafka_config (Dict): configuration for the associated Kafka consumer.
            The content is used to call the constructor of org.apache.kafka.clients.Admin;
            pass any Admin specific desired configuration here

    Returns:
        a list of topic names

    Raises:
        DHError
    """
    try:
        kafka_config = j_properties(kafka_config)
        jtopics = _JKafkaTools.listTopics(kafka_config)
        return list(jtopics)
    except Exception as e:
        raise DHError(e, "failed to list Kafka topics") from e
