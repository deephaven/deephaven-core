#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""Copies the ops of a base class onto each subclass and fills in their docstrings for that subclass.

The pydeephaven and deephaven packages each hold an identical copy of this file, because neither package can import
the other. A server test fails when the two copies differ, so edit both together.
"""

from __future__ import annotations

import re
import types
from typing import TypeVar

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


def bind_t_text(hint: str, cls: type) -> str:
    """Replaces T in a hint written as source text with the class name, such as ``"Table | T"`` with ``"Table"``.

    Args:
        hint (str): the hint as written in the source
        cls (type): the class that T stands for

    Returns:
        the hint with T replaced. A union keeps each member once, in its first position.
    """
    members: list[str] = []
    depth = 0
    start = 0
    # Split on the top-level "|" only, so a "|" inside brackets, such as in "Sequence[int | str]", stays in its member.
    for i, ch in enumerate(hint):
        if ch in "[(":
            depth += 1
        elif ch in "])":
            depth -= 1
        elif ch == "|" and depth == 0:
            members.append(hint[start:i])
            start = i + 1
    members.append(hint[start:])
    bound = (re.sub(r"\bT\b", cls.__name__, m.strip()) for m in members)
    return " | ".join(dict.fromkeys(bound))


def fill_doc(doc: str, doc_vars: dict[str, str], op_name: str = "") -> str:
    """Fills a docstring template from a class's ``_doc_vars``.

    Args:
        doc (str): the docstring template
        doc_vars (dict[str, str]): the values. A non-empty ``intro`` becomes the first paragraph, and may hold several
            lines. Every other key fills its ``{key}`` placeholder. A line that holds only the placeholder takes the
            value's lines at that line's indent, and an empty value removes that line.
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
        # An intro of several lines takes the same indent on its later lines, for the same reason.
        intro = intro.replace("\n", "\n" + " " * indent)
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

    Raises:
        TypeError: when the op has a hint that is not text
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
    for name, hint in func.__annotations__.items():
        # A hint that is an object would need resolving, and the copy would show it with full module paths, such as
        # "collections.abc.Sequence[str]", in tools that print the signature, such as the Web IDE's autocomplete.
        if not isinstance(hint, str):
            raise TypeError(
                f"{func.__qualname__} has a {name!r} hint that is not text; its module needs "
                f"'from __future__ import annotations'"
            )
    # Keep the source text, with T replaced, so signatures show "Sequence[str]".
    op.__annotations__ = {
        k: bind_t_text(v, cls) for k, v in func.__annotations__.items()
    }
    # get_type_hints resolves the text in the copy's globals, which are the base module's. A base module that imports
    # the subclass only for type checking would raise NameError there, so bind the subclass's name. A name that is
    # already bound keeps its first value. A second subclass with the same name, or a reloaded one, then resolves its
    # hints to the first class. No two subclasses in either package share a name.
    func.__globals__.setdefault(cls.__name__, cls)
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
        TypeError: when the subclass's ``_doc_vars`` lacks a required key, an op is decorated, or an op has a hint
            that is not text
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
