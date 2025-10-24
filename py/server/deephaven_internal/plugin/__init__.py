#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
from typing import TYPE_CHECKING, cast

import jpy

if TYPE_CHECKING:
    from typing_extensions import TypeAlias  # novermin  # noqa

_JCallbackAdapter = cast(
    type, jpy.get_type("io.deephaven.server.plugin.python.CallbackAdapter")
)  # type: TypeAlias


def initialize_all_and_register_into(callback: _JCallbackAdapter):
    try:
        from . import register
    except ModuleNotFoundError as e:
        # deephaven.plugin is an optional dependency, so if it can't be found, there are no Deephaven python plugins
        # to register
        if e.name == "deephaven.plugin":
            return
        raise e
    register.initialize_all_and_register_into(callback)
