#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""Authorization helpers for Deephaven Python plugins.

A server-side plugin (an ``ObjectType``) may hand server objects (tables, etc.) to a client by passing them as
references to ``MessageStream.on_data``. By default those references are exported as-is. Plugins that want to apply the
server's authorization rules themselves can call :func:`transform` on each object before exporting it; this applies the
same transform the server uses when resolving tickets, in the *current* user's context.

A plugin that performs its own transformation this way should also declare ``authorization_export_behavior = "manual"``
on its ``ObjectType`` so that the server does not additionally transform the references (which would double-apply ACLs).

Notes:
    * :func:`transform` consults the authorization context that is active *when it is called*. Call it on the thread
      that is rendering for the intended user (for example, inside a ``deephaven.ui`` ``@ui.component``), so that the
      object is transformed for the viewer rather than the query owner.
    * The transform may produce a new derived object (e.g. an ACL-filtered table). Call it within a liveness scope that
      remains open until the object is exported.
    * In Deephaven Community the transform is the identity, so calling :func:`transform` is always safe; it only changes
      behavior under an authorization provider that implements transformations.
"""

from typing import Any, Optional

from deephaven._wrapper import javaify, pythonify
from deephaven_internal.plugin._authorization import get_transformer

__all__ = ["transform"]


def transform(obj: Any) -> Any:
    """Apply the server's authorization transform to ``obj`` for the current user's context.

    Args:
        obj (Any): the object to transform (typically a table or other exportable object). ``None`` is returned unchanged.

    Returns:
        the transformed object (which may be the same object, or a derived/filtered one), or ``None`` if the current
        user is not permitted to access ``obj``.

    Raises:
        RuntimeError: if the authorization transformer has not been initialized (i.e. this is not running inside a
            Deephaven server that registered its Python plugins).
    """
    if obj is None:
        return None
    result = get_transformer().transform(javaify(obj))
    return pythonify(result)
