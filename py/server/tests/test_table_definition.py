#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest
from typing import Mapping
from deephaven import dtypes, new_table
from deephaven.table import TableDefinition
from deephaven.column import Column, string_col, bool_col
from tests.testbase import BaseTestCase


class TableDefinitionTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_definition = TableDefinition.from_columns(
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

    def test_get(self):
        self.assertEquals(Column("Bool", dtypes.bool_), self.test_definition["Bool"])
        self.assertEquals(Column("Char", dtypes.char), self.test_definition["Char"])
        self.assertEquals(Column("Short", dtypes.short), self.test_definition["Short"])
        self.assertEquals(Column("Int", dtypes.int32), self.test_definition["Int"])
        self.assertEquals(Column("Long", dtypes.int64), self.test_definition["Long"])
        self.assertEquals(
            Column("Float", dtypes.float32), self.test_definition["Float"]
        )
        self.assertEquals(
            Column("Double", dtypes.float64), self.test_definition["Double"]
        )
        self.assertEquals(
            Column("String", dtypes.string), self.test_definition["String"]
        )
        self.assertEquals(
            Column("Instant", dtypes.Instant), self.test_definition["Instant"]
        )
        with self.assertRaises(KeyError):
            self.test_definition["FooBarBaz"]

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
                Column("Bool", dtypes.bool_),
                Column("Char", dtypes.char),
                Column("Short", dtypes.short),
                Column("Int", dtypes.int32),
                Column("Long", dtypes.int64),
                Column("Float", dtypes.float32),
                Column("Double", dtypes.float64),
                Column("String", dtypes.string),
                Column("Instant", dtypes.Instant),
            ],
            list(self.test_definition.values()),
        )

    def test_items(self):
        self.assertEquals(
            [
                ("Bool", Column("Bool", dtypes.bool_)),
                ("Char", Column("Char", dtypes.char)),
                ("Short", Column("Short", dtypes.short)),
                ("Int", Column("Int", dtypes.int32)),
                ("Long", Column("Long", dtypes.int64)),
                ("Float", Column("Float", dtypes.float32)),
                ("Double", Column("Double", dtypes.float64)),
                ("String", Column("String", dtypes.string)),
                ("Instant", Column("Instant", dtypes.Instant)),
            ],
            list(self.test_definition.items()),
        )

    def test_equals_hash_and_from_columns(self):
        expected_hash = hash(self.test_definition)
        for actual in [
            self.test_definition,
            TableDefinition.from_columns(
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
            ),
            TableDefinition.from_columns(self.test_definition.values()),
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

        self.assert_table_equals(expected, self.test_definition.table)


if __name__ == "__main__":
    unittest.main()
