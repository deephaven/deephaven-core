#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import jpy

from typing import Optional

from . import JsonValueType, json as json_


def json(json_value: JsonValueType, factory: Optional[jpy.JType] = None) -> jpy.JType:
    """Creates a jackson JSON named object processor provider.

    Args:
        json_value(JsonValueType): the JSON value
        factory(Optional[jpy.JType]): the factory (java type "com.fasterxml.jackson.core.JsonFactory"), by default is None

    Returns:
        the jackson JSON named object processor provider
    """
    # todo: should be on the classpath by default, but doesn't have to be
    # todo: would be nice if this code could live in the JSON jar
    _JProvider = jpy.get_type("io.deephaven.json.jackson.JacksonProvider")
    return (
        _JProvider.of(json_(json_value).j_options, factory)
        if factory
        else _JProvider.of(json_(json_value).j_options)
    )


def bson(json_value: JsonValueType, factory: Optional[jpy.JType] = None) -> jpy.JType:
    """Creates a jackson BSON named object processor provider.

    Args:
        json_value(JsonValueType): the JSON value
        factory(Optional[jpy.JType]): the factory (java type "de.undercouch.bson4jackson.BsonFactory"), by default is None

    Returns:
        the jackson BSON named object processor provider
    """
    # todo: not on the classpath by default
    # todo: would be nice if this code could live in the BSON jar
    _JProvider = jpy.get_type("io.deephaven.bson.jackson.JacksonBsonProvider")
    return (
        _JProvider.of(json_(json_value).j_options, factory)
        if factory
        else _JProvider.of(json_(json_value).j_options)
    )
