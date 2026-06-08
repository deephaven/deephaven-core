#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""Internal storage for the plugin authorization transformer.

The transformer is a ``io.deephaven.plugin.options.PluginOptions$AuthorizationTransformer`` handed to Python from the
Java plugin-registration bridge at server startup (see ``register.initialize_all_and_register_into``). The public
``deephaven.plugin_authorization`` module reads it from here. This module is internal; plugins should use
``deephaven.plugin_authorization``.
"""

from typing import Any, Optional

_j_transformer: Optional[Any] = None


def set_transformer(j_transformer: Any) -> None:
    """Stash the Java authorization transformer. Called once during plugin registration."""
    global _j_transformer
    _j_transformer = j_transformer


def get_transformer() -> Any:
    """Return the Java authorization transformer.

    Raises:
        RuntimeError: if no transformer has been registered (e.g. accessed outside a server that initialized the
            Python plugin bridge).
    """
    if _j_transformer is None:
        raise RuntimeError(
            "The plugin authorization transformer is not initialized; it is only available within a Deephaven "
            "server that has registered its Python plugins."
        )
    return _j_transformer
