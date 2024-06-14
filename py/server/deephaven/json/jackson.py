#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

"""A JSON processor provider implementation using Jackson (https://github.com/FasterXML/jackson)."""

import jpy

from typing import Optional

from . import JsonValueType, json_val


def provider(
    json_value_type: JsonValueType, factory: Optional[jpy.JType] = None
) -> jpy.JType:
    """Creates a Jackson JSON named object processor provider.

    Args:
        json_value_type (JsonValueType): the JSON value
        factory (Optional[jpy.JType]): the factory (java type "com.fasterxml.jackson.core.JsonFactory"), by default is
            None which will use a default factory

    Returns:
        the jackson JSON named object processor provider
    """
    _JProvider = jpy.get_type("io.deephaven.json.jackson.JacksonProvider")
    return (
        _JProvider.of(json_val(json_value_type).j_value, factory)
        if factory
        else _JProvider.of(json_val(json_value_type).j_value)
    )
