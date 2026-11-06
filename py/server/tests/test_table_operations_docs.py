#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import inspect
import re
import types
import unittest
from pathlib import Path

from deephaven.table import PartitionedTableProxy, Table, _TableOperations
from deephaven.table_factory import InputTable
from tests.testbase import BaseTestCase

OP_NAMES: list[str] = [
    name
    for name, func in vars(_TableOperations).items()
    if isinstance(func, types.FunctionType) and not name.startswith("_")
]

# The ops whose first argument is the right, trigger or initial-groups table.
RIGHT_PARAMS: dict[str, str] = {
    "natural_join": "table",
    "exact_join": "table",
    "join": "table",
    "aj": "table",
    "raj": "table",
    "range_join": "table",
    "snapshot_when": "trigger_table",
}

# A leftover placeholder, such as {returns}. It does not match the {...} of a dict or set literal in an example,
# because those hold quotes, colons or spaces.
PLACEHOLDER = re.compile(r"\{[a-z_]+\}")


def _client_twin() -> Path:
    """Returns the path of the pydeephaven copy of _doc_template.py.

    The source tree has it at py/client next to py/server. The test container copies py/client into a client folder
    inside the server's folder.
    """
    server_root = Path(__file__).resolve().parents[1]
    for client_root in (server_root.parent / "client", server_root / "client"):
        twin = client_root / "pydeephaven" / "_doc_template.py"
        if twin.exists():
            return twin
    raise FileNotFoundError(f"no pydeephaven/_doc_template.py near {server_root}")


class TableOperationsDocsTestCase(BaseTestCase):
    def test_doc_template_matches_client_copy(self) -> None:
        self.assertEqual(
            _client_twin().read_text(),
            (
                Path(__file__).resolve().parents[1] / "deephaven" / "_doc_template.py"
            ).read_text(),
            "deephaven/_doc_template.py and pydeephaven/_doc_template.py must stay identical; edit both together",
        )

    def test_ops_are_filled_per_class(self) -> None:
        for cls in (Table, PartitionedTableProxy):
            for name in OP_NAMES:
                with self.subTest(cls=cls.__name__, op=name):
                    op = vars(cls)[name]
                    doc = inspect.getdoc(op) or ""
                    self.assertIsNone(PLACEHOLDER.search(doc))
                    self.assertIn(f"Returns:\n    {cls._doc_vars['returns']}", doc)
                    self.assertIn("Raises:\n    DHError", doc)
                    self.assertNotIn("\n\n\n", doc)
                    self.assertEqual(f"{cls.__name__}.{name}", op.__qualname__)

    def test_proxy_intro_names_the_op(self) -> None:
        for name in OP_NAMES:
            with self.subTest(op=name):
                doc = inspect.getdoc(vars(PartitionedTableProxy)[name]) or ""
                self.assertTrue(
                    doc.startswith(f"Applies the :meth:`~Table.{name}` table operation")
                )
                self.assertFalse(
                    (inspect.getdoc(vars(Table)[name]) or "").startswith("Applies the")
                )

    def test_right_note_only_on_proxy(self) -> None:
        for name in [*RIGHT_PARAMS, "agg_by"]:
            with self.subTest(op=name):
                proxy_doc = inspect.getdoc(vars(PartitionedTableProxy)[name]) or ""
                self.assertIn(
                    "When the other table is a PartitionedTableProxy, the "
                    f":meth:`~Table.{name}` table operation",
                    proxy_doc,
                )
                self.assertNotIn(
                    "PartitionedTableProxy", inspect.getdoc(vars(Table)[name]) or ""
                )

    def test_signatures_use_real_types(self) -> None:
        for cls in (Table, PartitionedTableProxy):
            for name in OP_NAMES:
                with self.subTest(cls=cls.__name__, op=name):
                    sig = inspect.signature(vars(cls)[name])
                    self.assertIs(cls, sig.return_annotation)
                    for param in sig.parameters.values():
                        self.assertNotIsInstance(param.annotation, str)

    def test_signatures_use_pipe_unions(self) -> None:
        for cls in (Table, PartitionedTableProxy):
            for name in OP_NAMES:
                with self.subTest(cls=cls.__name__, op=name):
                    sig = str(inspect.signature(vars(cls)[name]))
                    self.assertNotIn("Union[", sig)
                    self.assertNotIn("Optional[", sig)

    def test_right_table_type(self) -> None:
        for name, param in RIGHT_PARAMS.items():
            with self.subTest(op=name):
                self.assertIs(
                    Table,
                    inspect.signature(Table.__dict__[name])
                    .parameters[param]
                    .annotation,
                )
                self.assertEqual(
                    Table | PartitionedTableProxy,
                    inspect.signature(PartitionedTableProxy.__dict__[name])
                    .parameters[param]
                    .annotation,
                )

    def test_plain_class_hints_stay_plain(self) -> None:
        for cls in (Table, PartitionedTableProxy):
            for name in ("where_in", "where_not_in"):
                with self.subTest(cls=cls.__name__, op=name):
                    self.assertIs(
                        Table,
                        inspect.signature(vars(cls)[name])
                        .parameters["filter_table"]
                        .annotation,
                    )

    def test_generic_hook_runs(self) -> None:
        # Generic's __init_subclass__ comes after JObjectWrapper's in the MRO, so it runs only when
        # JObjectWrapper's passes the call on. Without it, T stays in __parameters__.
        for cls in (Table, PartitionedTableProxy, InputTable):
            with self.subTest(cls=cls.__name__):
                self.assertEqual((), cls.__parameters__)

    def test_table_subclasses_reuse_table_ops(self) -> None:
        for name in OP_NAMES:
            with self.subTest(op=name):
                self.assertNotIn(name, vars(InputTable))
                self.assertIs(vars(Table)[name], getattr(InputTable, name))


if __name__ == "__main__":
    unittest.main()
