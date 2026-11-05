#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import inspect
import re
import types
import unittest

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

# A leftover placeholder, such as {returns}. It does not match the {...} of a dict or set literal in an example,
# because those hold quotes, colons or spaces.
PLACEHOLDER = re.compile(r"\{[a-z_]+\}")


class TableInterfaceDocsTestCase(unittest.TestCase):
    def test_base_docstrings_use_the_template(self) -> None:
        for name in OP_NAMES:
            with self.subTest(op=name):
                self.assertIn(
                    "{returns}",
                    vars(TableInterface)[name].__doc__ or "",
                    "a TableInterface op's docstring needs a {returns} placeholder, filled per class from _doc_vars",
                )

    def test_ops_are_filled_per_class(self) -> None:
        for cls in (Table, Query):
            for name in OP_NAMES:
                with self.subTest(cls=cls.__name__, op=name):
                    op = vars(cls)[name]
                    doc = inspect.getdoc(op) or ""
                    self.assertIsNone(PLACEHOLDER.search(doc))
                    self.assertIn(cls._doc_vars["returns"], doc)
                    self.assertNotIn("\n\n\n", doc)
                    self.assertEqual(f"{cls.__name__}.{name}", op.__qualname__)

    def test_signatures_use_real_types(self) -> None:
        for cls in (Table, Query):
            for name in OP_NAMES:
                with self.subTest(cls=cls.__name__, op=name):
                    sig = inspect.signature(vars(cls)[name])
                    self.assertIs(cls, sig.return_annotation)
                    for param in sig.parameters.values():
                        self.assertNotIsInstance(param.annotation, str)

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
                # The base docstring's first line must land at the same indent as its other lines.
                self.assertFalse(doc.splitlines()[2].startswith(" "))
                self.assertFalse(
                    (inspect.getdoc(vars(Table)[name]) or "").startswith(intro)
                )

    def test_one_line_docstring_with_intro(self) -> None:
        original = TableInterface.head.__doc__
        TableInterface.head.__doc__ = "Returns {returns}."
        try:

            class OneLine(TableInterface["OneLine"]):
                _doc_vars = {"returns": "self", "intro": "Intro."}

                def table_op_handler(self, table_op: TableOp) -> "OneLine":
                    return self

            self.assertEqual("Intro.\n\nReturns self.", OneLine.head.__doc__)
        finally:
            TableInterface.head.__doc__ = original

    def test_input_table_reuses_table_ops(self) -> None:
        for name in OP_NAMES:
            with self.subTest(op=name):
                self.assertNotIn(name, vars(InputTable))
                self.assertIs(vars(Table)[name], getattr(InputTable, name))


if __name__ == "__main__":
    unittest.main()
