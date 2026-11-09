#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""Copies the ops of a base class onto each subclass and fills in their docstrings for that subclass.

The pydeephaven and deephaven packages each hold an identical copy of this file, because neither package can import
the other. A server test fails when the two copies differ, so edit both together.
"""

from __future__ import annotations

import functools
import operator
import sys
import types
from typing import Any, TypeVar, Union, get_args, get_origin, get_type_hints

T = TypeVar("T")


def _fill_block(doc: str, name: str, value: str) -> str:
    """Replaces each line that holds only ``{name}`` with the lines of ``value``, at that line's indent.

    Args:
        doc (str): the docstring to fill
        name (str): the placeholder name
        value (str): the lines to put in, such as "Raises:\\n    DHError". An empty string removes the placeholder
            line and the blank line before it.

    Returns:
        the filled docstring
    """
    placeholder = f"{{{name}}}"
    out: list[str] = []
    for line in doc.split("\n"):
        if line.strip() != placeholder:
            out.append(line)
            continue
        indent = line[: len(line) - len(line.lstrip())]
        if value:
            out.extend(f"{indent}{v}" if v else "" for v in value.split("\n"))
        elif out and not out[-1].strip():
            # Drop the blank line before the placeholder. Otherwise it doubles up with the blank line after it.
            out.pop()
    return "\n".join(out)


def bind_t(hint: Any, cls: type) -> Any:
    """Replaces T in a type hint with the class, such as ``Table | T`` with ``Table``.

    Args:
        hint (Any): the resolved type hint
        cls (type): the class that T stands for

    Returns:
        the hint with T replaced. A hint without T, or with only other TypeVars, comes back unchanged.
    """
    if hint is T:
        return cls
    # A plain class needs no binding. Without this check, a Generic subclass whose __init_subclass__ chain skips
    # Generic's hook keeps T in __parameters__. A Table hint would then come back as Table[Table].
    if isinstance(hint, type) and not isinstance(hint, types.GenericAlias):
        return hint
    params = getattr(hint, "__parameters__", ())
    if T not in params:
        return hint
    # Subscripting fills the hint's TypeVars in order. A union drops the duplicate, so Table | T becomes Table.
    return hint[tuple(cls if p is T else p for p in params)]


def to_pipe_union(hint: Any) -> Any:
    """Rewrites a ``typing.Union`` hint as an ``X | Y`` union, such as ``Optional[int]`` as ``int | None``.

    On Python 3.10, ``get_type_hints`` wraps the hint of a parameter that defaults to None in ``Optional``, and a
    union with a TypeVar, such as ``Table | T``, is a ``typing.Union``. Both would show as ``Union[...]`` in
    signatures and docs, while the source writes ``X | Y``.

    Args:
        hint (Any): the resolved type hint

    Returns:
        the hint as an ``X | Y`` union. A hint that is not a ``typing.Union``, or a union whose members cannot be
        joined with ``|``, comes back unchanged.
    """
    if get_origin(hint) is not Union:
        return hint
    try:
        return functools.reduce(
            operator.or_, (to_pipe_union(a) for a in get_args(hint))
        )
    except TypeError:
        # A member such as an unbound TypeVar or a forward reference string does not support |.
        return hint


def fill_doc(doc: str, doc_vars: dict[str, str], op_name: str = "") -> str:
    """Fills a docstring template from a class's ``_doc_vars``.

    Args:
        doc (str): the docstring template
        doc_vars (dict[str, str]): the values. A non-empty ``intro`` becomes the first paragraph. Every other key
            fills its ``{key}`` placeholder. A line that holds only the placeholder takes the value's lines at that
            line's indent, and an empty value removes that line.
        op_name (str): the op's name, which fills ``{op}`` in the docstring and in the values; defaults to an empty
            string, meaning ``{op}`` is left as is

    Returns:
        the filled docstring
    """
    for name, value in doc_vars.items():
        if name != "intro":
            doc = _fill_block(doc, name, value).replace(f"{{{name}}}", value)
    intro = doc_vars.get("intro", "")
    if intro:
        # Give the original first line the indent the other lines share. A docstring's first line has no indent.
        # Without this, it would be the least indented line, and Sphinx would show the rest as a quoted block.
        # A one-line docstring has no other lines. Without the default, min() would raise and break the import.
        indent = min(
            (
                len(line) - len(line.lstrip())
                for line in doc.splitlines()[1:]
                if line.strip()
            ),
            default=0,
        )
        doc = f"{intro}\n\n{' ' * indent}{doc}"
    return doc.replace("{op}", op_name) if op_name else doc


def copy_op(func: types.FunctionType, cls: type, doc: str) -> types.FunctionType:
    """Copies a base class op onto a subclass, with T bound to the subclass in its type hints.

    Args:
        func (types.FunctionType): the base class op
        cls (type): the subclass
        doc (str): the copy's docstring

    Returns:
        the copy, named and documented as the subclass's own method
    """
    # A new function on the same code keeps the base op's signature, defaults and annotations. A wrapper would show
    # (*args, **kwargs) instead.
    op = types.FunctionType(
        func.__code__,
        func.__globals__,
        func.__name__,
        func.__defaults__,
        func.__closure__,
    )
    op.__kwdefaults__ = func.__kwdefaults__
    # Resolve the string annotations here, with the subclass's module added. The copy keeps the base module's
    # globals, where the subclass may not exist yet or may exist only for type checking. Without this, Sphinx cannot
    # resolve the class names. Binding T to the subclass then keeps the docs from showing "-> T".
    hints = get_type_hints(
        func,
        globalns={
            **func.__globals__,
            **vars(sys.modules[cls.__module__]),
            cls.__name__: cls,
        },
    )
    # get_type_hints turns a None hint into NoneType, which help() would show as "-> NoneType".
    op.__annotations__ = {
        k: None if v is type(None) else to_pipe_union(bind_t(v, cls))
        for k, v in hints.items()
    }
    op.__qualname__ = f"{cls.__qualname__}.{func.__name__}"
    op.__module__ = cls.__module__
    op.__doc__ = doc
    return op


def copy_ops(
    base: type,
    cls: type,
    required: tuple[str, ...],
    defaults: dict[str, str] | None = None,
    skip: frozenset[str] = frozenset(),
) -> None:
    """Sets on a subclass its own copy of every public function op of the base, with the docstring filled in from
    the subclass's ``_doc_vars``.

    An op the subclass defines itself keeps the subclass's version. A subclass without its own ``_doc_vars`` gets no
    copies, so it inherits its parent's filled-in copies.

    Args:
        base (type): the base class that defines the ops
        cls (type): the subclass
        required (tuple[str, ...]): the ``_doc_vars`` keys every subclass must set
        defaults (dict[str, str] | None): the values for the keys a subclass may leave out; defaults to None,
            meaning every placeholder the subclass leaves out stays in the docstring
        skip (frozenset[str]): the public function names that are not ops; defaults to an empty set, meaning every
            public function is copied

    Raises:
        TypeError: when the subclass's ``_doc_vars`` lacks a required key, or an op is decorated
    """
    if "_doc_vars" not in vars(cls):
        return
    doc_vars: dict[str, str] = {**(defaults or {}), **vars(cls)["_doc_vars"]}
    for key in required:
        if key not in doc_vars:
            raise TypeError(
                f"{cls.__qualname__}._doc_vars needs a {key!r} value to fill the {{{key}}} placeholder"
            )
    for name, func in vars(base).items():
        if (
            name.startswith("_")
            or name in skip
            or not isinstance(func, types.FunctionType)
            or name in vars(cls)
        ):
            continue
        # The copy would take the wrapper's code. A decorated op would then show (*args, **kwargs) in its signature
        # and docs, or fail to resolve its hints in the decorator's module.
        if hasattr(func, "__wrapped__"):
            raise TypeError(
                f"{base.__qualname__}.{name} is decorated; decorated {base.__qualname__} ops are not supported"
            )
        setattr(
            cls, name, copy_op(func, cls, fill_doc(func.__doc__ or "", doc_vars, name))
        )
