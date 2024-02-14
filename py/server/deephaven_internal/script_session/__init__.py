#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

# Implementation utilities for io.deephaven.engine.util.PythonDeephavenSession
from jpy import JType

from deephaven import _wrapper


def create_change_list(from_snapshot, to_snapshot):
    changes = []
    for (name, new_value) in to_snapshot.items():
        if not isinstance(name, str):
            continue
        if name not in from_snapshot:
            changes.append(make_change_item(name, None, new_value))
        else:
            existing_value = from_snapshot[name]
            if new_value is not existing_value:
                changes.append(make_change_item(name, existing_value, new_value))
    for (name, existing_value) in from_snapshot.items():
        if not isinstance(name, str):
            continue
        if name in to_snapshot:
            continue  # already handled
        changes.append(make_change_item(name, existing_value, None))
    return changes


def make_change_item(name, existing_value, new_value):
    # TODO(deephaven-core#1775): multivariate jpy (unwrapped) return type into java
    # It would be great if we could handle the maybe unwrapping at this layer, but we are unable to currently handle it
    # properly at the java layer
    # return name, maybe_unwrap(existing_value), maybe_unwrap(new_value)
    return name, existing_value, new_value


def javaify(obj) -> JType:
    """
    Returns a JType object if the object is already a JType, or if the object can be unwrapped into a JType object;
    otherwise, wraps the object in a LivenessArtifact to ensure it is freed in Python correctly.

    :param object: the object to be unwrapped
    :return: the JType object
    """
    return _wrapper.javaify(obj)
