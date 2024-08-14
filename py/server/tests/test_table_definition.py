#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest
from typing import Mapping
from deephaven import dtypes, new_table, DHError
from deephaven.table import TableDefinition
from deephaven.column import col_def, string_col, bool_col
from tests.testbase import BaseTestCase


class TableDefinitionTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_definition = TableDefinition(
            {
                "Bool": dtypes.bool_,
                "Char": dtypes.char,
                "Short": dtypes.short,
                "Int": dtypes.int32,
                "Long": dtypes.int64,
                "Float": dtypes.float32,
                "Double": dtypes.float64,
                "String": dtypes.string,
                "Instant": dtypes.Instant,
            }
        )

    def tearDown(self) -> None:
        self.test_definition = None
        super().tearDown()

    def test_is_mapping(self):
        self.assertTrue(isinstance(self.test_definition, Mapping))

    def test_length(self):
        self.assertEquals(9, len(self.test_definition))

    def test_contains(self):
        self.assertTrue("Bool" in self.test_definition)
        self.assertTrue("Char" in self.test_definition)
        self.assertTrue("Short" in self.test_definition)
        self.assertTrue("Int" in self.test_definition)
        self.assertTrue("Long" in self.test_definition)
        self.assertTrue("Float" in self.test_definition)
        self.assertTrue("Double" in self.test_definition)
        self.assertTrue("String" in self.test_definition)
        self.assertTrue("Instant" in self.test_definition)
        self.assertFalse("FooBarBaz" in self.test_definition)

    def test_getitem(self):
        self.assertEquals(col_def("Bool", dtypes.bool_), self.test_definition["Bool"])
        self.assertEquals(col_def("Char", dtypes.char), self.test_definition["Char"])
        self.assertEquals(col_def("Short", dtypes.short), self.test_definition["Short"])
        self.assertEquals(col_def("Int", dtypes.int32), self.test_definition["Int"])
        self.assertEquals(col_def("Long", dtypes.int64), self.test_definition["Long"])
        self.assertEquals(
            col_def("Float", dtypes.float32), self.test_definition["Float"]
        )
        self.assertEquals(
            col_def("Double", dtypes.float64),
            self.test_definition["Double"],
        )
        self.assertEquals(
            col_def("String", dtypes.string), self.test_definition["String"]
        )
        self.assertEquals(
            col_def("Instant", dtypes.Instant),
            self.test_definition["Instant"],
        )
        with self.assertRaises(KeyError):
            self.test_definition["FooBarBaz"]

    def test_get(self):
        self.assertEquals(
            col_def("Bool", dtypes.bool_), self.test_definition.get("Bool")
        )
        self.assertEquals(
            col_def("Char", dtypes.char), self.test_definition.get("Char")
        )
        self.assertEquals(
            col_def("Short", dtypes.short),
            self.test_definition.get("Short"),
        )
        self.assertEquals(col_def("Int", dtypes.int32), self.test_definition.get("Int"))
        self.assertEquals(
            col_def("Long", dtypes.int64), self.test_definition.get("Long")
        )
        self.assertEquals(
            col_def("Float", dtypes.float32),
            self.test_definition.get("Float"),
        )
        self.assertEquals(
            col_def("Double", dtypes.float64),
            self.test_definition.get("Double"),
        )
        self.assertEquals(
            col_def("String", dtypes.string),
            self.test_definition.get("String"),
        )
        self.assertEquals(
            col_def("Instant", dtypes.Instant),
            self.test_definition.get("Instant"),
        )
        self.assertEquals(None, self.test_definition.get("FooBarBaz"))

    def test_iter(self):
        self.assertEquals(
            [
                "Bool",
                "Char",
                "Short",
                "Int",
                "Long",
                "Float",
                "Double",
                "String",
                "Instant",
            ],
            list(iter(self.test_definition)),
        )

    def test_keys(self):
        self.assertEquals(
            [
                "Bool",
                "Char",
                "Short",
                "Int",
                "Long",
                "Float",
                "Double",
                "String",
                "Instant",
            ],
            list(self.test_definition.keys()),
        )

    def test_values(self):
        self.assertEquals(
            [
                col_def("Bool", dtypes.bool_),
                col_def("Char", dtypes.char),
                col_def("Short", dtypes.short),
                col_def("Int", dtypes.int32),
                col_def("Long", dtypes.int64),
                col_def("Float", dtypes.float32),
                col_def("Double", dtypes.float64),
                col_def("String", dtypes.string),
                col_def("Instant", dtypes.Instant),
            ],
            list(self.test_definition.values()),
        )

    def test_items(self):
        self.assertEquals(
            [
                ("Bool", col_def("Bool", dtypes.bool_)),
                ("Char", col_def("Char", dtypes.char)),
                ("Short", col_def("Short", dtypes.short)),
                ("Int", col_def("Int", dtypes.int32)),
                ("Long", col_def("Long", dtypes.int64)),
                ("Float", col_def("Float", dtypes.float32)),
                ("Double", col_def("Double", dtypes.float64)),
                ("String", col_def("String", dtypes.string)),
                ("Instant", col_def("Instant", dtypes.Instant)),
            ],
            list(self.test_definition.items()),
        )

    def test_equals_hash_and_from_columns(self):
        expected_hash = hash(self.test_definition)
        for actual in [
            # should be equal to the same exact object
            self.test_definition,
            # should be equal to a new python object, but same underlying java object
            TableDefinition(self.test_definition),
            # should be equal to a new python object and new underlying java object
            TableDefinition(self.test_definition.values()),
        ]:
            self.assertEquals(actual, self.test_definition)
            self.assertEquals(hash(actual), expected_hash)

    def test_meta_table(self):
        expected = new_table(
            [
                string_col(
                    "Name",
                    [
                        "Bool",
                        "Char",
                        "Short",
                        "Int",
                        "Long",
                        "Float",
                        "Double",
                        "String",
                        "Instant",
                    ],
                ),
                string_col(
                    "DataType",
                    [
                        "java.lang.Boolean",
                        "char",
                        "short",
                        "int",
                        "long",
                        "float",
                        "double",
                        "java.lang.String",
                        "java.time.Instant",
                    ],
                ),
                string_col("ColumnType", ["Normal"] * 9),
                bool_col("IsPartitioning", [False] * 9),
            ]
        )

        self.assert_table_equals(self.test_definition.table, expected)

    def test_from_TableDefinition(self):
        self.assertEquals(TableDefinition(self.test_definition), self.test_definition)

    def test_from_JpyJType(self):
        self.assertEquals(
            TableDefinition(self.test_definition.j_table_definition),
            self.test_definition,
        )

    def test_from_Mapping(self):
        # This case is already tested, it's how self.test_definition is created
        pass

    def test_from_Iterable(self):
        self.assertEquals(
            TableDefinition(self.test_definition.values()), self.test_definition
        )
        self.assertEquals(
            TableDefinition(list(self.test_definition.values())), self.test_definition
        )

    def test_from_unexpected_type(self):
        with self.assertRaises(DHError):
            TableDefinition(42)

    def test_bad_Mapping_key(self):
        with self.assertRaises(DHError):
            TableDefinition(
                {
                    "Foo": dtypes.int32,
                    42: dtypes.string,
                }
            )

    def test_bad_Mapping_value(self):
        with self.assertRaises(DHError):
            TableDefinition(
                {
                    "Foo": dtypes.int32,
                    "Bar": 42,
                }
            )

    def test_bad_Iterable(self):
        with self.assertRaises(DHError):
            TableDefinition([col_def("Foo", dtypes.int32), 42])


if __name__ == "__main__":
    unittest.main()
