#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

"""A JSON processor provider implementation using Jackson."""

import jpy

from typing import Optional

from . import JsonValueType, json


def provider(
    json_value: JsonValueType, factory: Optional[jpy.JType] = None
) -> jpy.JType:
    """Creates a jackson JSON named object processor provider.

    Args:
        json_value(JsonValueType): the JSON value
        factory(Optional[jpy.JType]): the factory (java type "com.fasterxml.jackson.core.JsonFactory"), by default is None

    Returns:
        the jackson JSON named object processor provider
    """
    _JProvider = jpy.get_type("io.deephaven.json.jackson.JacksonProvider")
    return (
        _JProvider.of(json(json_value).j_options, factory)
        if factory
        else _JProvider.of(json(json_value).j_options)
    )
