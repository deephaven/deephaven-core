#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import functools
import inspect
import re
import types
import unittest
from typing import Any, TypeVar, get_type_hints

from pydeephaven._doc_template import T, bind_t_text, fill_doc
from pydeephaven._table_interface import TableInterface
from pydeephaven._table_ops import TableOp
from pydeephaven.query import Query
from pydeephaven.table import InputTable, Table

OP_NAMES: list[str] = [
    name
    for name, func in vars(TableInterface).items()
    if isinstance(func, types.FunctionType)
    and not name.startswith("_")
    and name != "table_op_handler"
]

# The ops that return T follow the Returns template. An op that returns something else, such as None, writes its
# own Returns section or none, and only has to fill cleanly. The base hints are strings, from the __future__ import.
T_OP_NAMES: list[str] = [
    name
    for name in OP_NAMES
    if vars(TableInterface)[name].__annotations__.get("return") == "T"
]

# A leftover placeholder, such as {returns}. It does not match the {...} of a dict or set literal in an example,
# because those hold quotes, colons or spaces.
PLACEHOLDER = re.compile(r"\{[a-z_]+\}")


class TableInterfaceDocsTestCase(unittest.TestCase):
    def test_base_docstrings_use_the_template(self) -> None:
        for name in T_OP_NAMES:
            with self.subTest(op=name):
                self.assertIn(
                    "{returns}",
                    vars(TableInterface)[name].__doc__ or "",
                    "a TableInterface op that returns T needs a {returns} placeholder, filled per class from "
                    "_doc_vars",
                )

    def test_ops_are_filled_per_class(self) -> None:
        for cls in (Table, Query):
            for name in OP_NAMES:
                with self.subTest(cls=cls.__name__, op=name):
                    op = vars(cls)[name]
                    doc = inspect.getdoc(op) or ""
                    self.assertIsNone(PLACEHOLDER.search(doc))
                    if name in T_OP_NAMES:
                        self.assertIn(f"Returns:\n    {cls._doc_vars['returns']}", doc)
                    self.assertNotIn("\n\n\n", doc)
                    self.assertEqual(f"{cls.__name__}.{name}", op.__qualname__)

    def test_signatures_keep_source_text(self) -> None:
        # Signatures show the hints as the source wrote them, with T replaced, so tools that print them stay short.
        for cls in (Table, Query):
            for name in OP_NAMES:
                with self.subTest(cls=cls.__name__, op=name):
                    sig = inspect.signature(vars(cls)[name])
                    if name in T_OP_NAMES:
                        self.assertEqual(cls.__name__, sig.return_annotation)
                    self.assertNotRegex(str(sig), r"\bT\b")

    def test_hints_resolve(self) -> None:
        for cls in (Table, Query):
            for name in OP_NAMES:
                with self.subTest(cls=cls.__name__, op=name):
                    hints = get_type_hints(vars(cls)[name])
                    if name in T_OP_NAMES:
                        self.assertIs(cls, hints["return"])

    def test_bind_t_text(self) -> None:
        self.assertEqual("Query", bind_t_text("T", Query))
        # A union keeps each member once, so Table | T collapses to Table on Table.
        self.assertEqual("Table", bind_t_text("Table | T", Table))
        self.assertEqual("Table | Query | None", bind_t_text("Table | T | None", Query))
        # Only the top-level "|" splits members, and a name that merely contains T stays as it is.
        self.assertEqual(
            "dict[str, Query | int] | TableOp",
            bind_t_text("dict[str, T | int] | TableOp", Query),
        )

    def test_signatures_use_pipe_unions(self) -> None:
        for cls in (Table, Query):
            for name in OP_NAMES:
                with self.subTest(cls=cls.__name__, op=name):
                    sig = str(inspect.signature(vars(cls)[name]))
                    self.assertNotIn("Union[", sig)
                    self.assertNotIn("Optional[", sig)

    def test_raises_placeholder(self) -> None:
        for cls in (Table, Query):
            raises = cls._doc_vars.get("raises", "")
            for name in OP_NAMES:
                if "{raises}" not in (vars(TableInterface)[name].__doc__ or ""):
                    continue
                with self.subTest(cls=cls.__name__, op=name):
                    doc = inspect.getdoc(vars(cls)[name]) or ""
                    if raises:
                        self.assertIn(raises, doc)
                    else:
                        self.assertNotIn("Raises:", doc)

    def test_intro_is_its_own_paragraph(self) -> None:
        intro = Query._doc_vars["intro"]
        for name in OP_NAMES:
            with self.subTest(op=name):
                doc = inspect.getdoc(vars(Query)[name]) or ""
                self.assertTrue(doc.startswith(f"{intro}\n\n"))
                # The base docstring's first line must land at the same indent as its other lines. If it does not,
                # getdoc strips no indent and the section headers stay indented.
                if name in T_OP_NAMES:
                    self.assertIn("\nReturns:\n", doc)
                self.assertFalse(
                    (inspect.getdoc(vars(Table)[name]) or "").startswith(intro)
                )

    def test_fill_doc(self) -> None:
        template = "Summary.\n\n    Returns:\n        {returns}\n\n    {raises}\n    "
        self.assertEqual(
            "Summary.\n\n    Returns:\n        x\n\n    Raises:\n        E\n    ",
            fill_doc(template, {"returns": "x", "raises": "Raises:\n    E"}),
        )
        self.assertEqual(
            "Intro.\n\n    Summary.\n\n    Returns:\n        x\n    ",
            fill_doc(template, {"returns": "x", "raises": "", "intro": "Intro."}),
        )
        # The later lines of an intro take the docstring's indent, so getdoc strips it evenly.
        self.assertEqual(
            "Intro\n    more.\n\n    Summary.\n\n    Returns:\n        x\n    ",
            fill_doc(template, {"returns": "x", "raises": "", "intro": "Intro\nmore."}),
        )
        # A one-line docstring has no indent to copy.
        self.assertEqual(
            "Intro.\n\nReturns x.",
            fill_doc("Returns {returns}.", {"returns": "x", "intro": "Intro."}),
        )

    def test_doc_vars_without_returns(self) -> None:
        with self.assertRaisesRegex(TypeError, "returns"):

            class NoReturns(TableInterface["NoReturns"]):
                _doc_vars = {"intro": "Intro."}

                def table_op_handler(self, table_op: TableOp) -> "NoReturns":
                    return self

    def test_decorated_op_is_rejected(self) -> None:
        original = TableInterface.head

        @functools.wraps(original)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return original(*args, **kwargs)

        TableInterface.head = wrapper
        try:
            with self.assertRaisesRegex(TypeError, "head is decorated"):

                class Decorated(TableInterface["Decorated"]):
                    _doc_vars = {"returns": "self"}

                    def table_op_handler(self, table_op: TableOp) -> "Decorated":
                        return self

        finally:
            TableInterface.head = original

    def test_op_without_t_return(self) -> None:
        original = TableInterface.head

        def op(self: "Any") -> "None":
            """Does a thing.

            {raises}
            """

        TableInterface.head = op  # type: ignore[assignment]
        try:

            class NoneOp(TableInterface["NoneOp"]):
                _doc_vars = {"returns": "self", "intro": "Intro."}

                def table_op_handler(self, table_op: TableOp) -> "NoneOp":
                    return self

            # getdoc keeps a summary-only docstring's last indent line, so compare without trailing whitespace.
            self.assertEqual(
                "Intro.\n\nDoes a thing.", (inspect.getdoc(NoneOp.head) or "").rstrip()
            )
            self.assertEqual("None", inspect.signature(NoneOp.head).return_annotation)
        finally:
            TableInterface.head = original

    def test_t_is_bound_in_every_hint(self) -> None:
        original = TableInterface.head
        R = TypeVar("R")  # noqa: F841 - named only in a hint below, which stays text

        def op(
            self: "Any", other: "Table | T", others: "list[T]", extra: "dict[R, T]"
        ) -> "bool":
            """Returns {returns}."""
            return True

        TableInterface.head = op  # type: ignore[assignment]
        try:

            class Bound(TableInterface["Bound"]):
                _doc_vars = {"returns": "self"}

                def table_op_handler(self, table_op: TableOp) -> "Bound":
                    return self

            hints = Bound.head.__annotations__
            self.assertEqual("Table | Bound", hints["other"])
            self.assertEqual("list[Bound]", hints["others"])
            # Only T is replaced. Another TypeVar, such as R, stays.
            self.assertEqual("dict[R, Bound]", hints["extra"])
            self.assertEqual("bool", hints["return"])
        finally:
            TableInterface.head = original

    def test_object_hint_is_rejected(self) -> None:
        original = TableInterface.head

        def op(self: "Any", n: int) -> "Any":
            """Returns {returns}."""
            return self

        TableInterface.head = op  # type: ignore[assignment]
        try:
            with self.assertRaisesRegex(TypeError, "'n' hint that is not text"):

                class ObjectHint(TableInterface["ObjectHint"]):
                    _doc_vars = {"returns": "self"}

                    def table_op_handler(self, table_op: TableOp) -> "ObjectHint":
                        return self

        finally:
            TableInterface.head = original

    def test_input_table_reuses_table_ops(self) -> None:
        for name in OP_NAMES:
            with self.subTest(op=name):
                self.assertNotIn(name, vars(InputTable))
                self.assertIs(vars(Table)[name], getattr(InputTable, name))


if __name__ == "__main__":
    unittest.main()
