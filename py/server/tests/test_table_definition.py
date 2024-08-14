#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest
from typing import Mapping
from deephaven import dtypes, new_table, DHError
from deephaven.table import TableDefinition
from deephaven.column import ColumnDefinition, string_col, bool_col
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

    def test_get(self):
        self.assertEquals(
            ColumnDefinition.of("Bool", dtypes.bool_), self.test_definition["Bool"]
        )
        self.assertEquals(
            ColumnDefinition.of("Char", dtypes.char), self.test_definition["Char"]
        )
        self.assertEquals(
            ColumnDefinition.of("Short", dtypes.short), self.test_definition["Short"]
        )
        self.assertEquals(
            ColumnDefinition.of("Int", dtypes.int32), self.test_definition["Int"]
        )
        self.assertEquals(
            ColumnDefinition.of("Long", dtypes.int64), self.test_definition["Long"]
        )
        self.assertEquals(
            ColumnDefinition.of("Float", dtypes.float32), self.test_definition["Float"]
        )
        self.assertEquals(
            ColumnDefinition.of("Double", dtypes.float64),
            self.test_definition["Double"],
        )
        self.assertEquals(
            ColumnDefinition.of("String", dtypes.string), self.test_definition["String"]
        )
        self.assertEquals(
            ColumnDefinition.of("Instant", dtypes.Instant),
            self.test_definition["Instant"],
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
                ColumnDefinition.of("Bool", dtypes.bool_),
                ColumnDefinition.of("Char", dtypes.char),
                ColumnDefinition.of("Short", dtypes.short),
                ColumnDefinition.of("Int", dtypes.int32),
                ColumnDefinition.of("Long", dtypes.int64),
                ColumnDefinition.of("Float", dtypes.float32),
                ColumnDefinition.of("Double", dtypes.float64),
                ColumnDefinition.of("String", dtypes.string),
                ColumnDefinition.of("Instant", dtypes.Instant),
            ],
            list(self.test_definition.values()),
        )

    def test_items(self):
        self.assertEquals(
            [
                ("Bool", ColumnDefinition.of("Bool", dtypes.bool_)),
                ("Char", ColumnDefinition.of("Char", dtypes.char)),
                ("Short", ColumnDefinition.of("Short", dtypes.short)),
                ("Int", ColumnDefinition.of("Int", dtypes.int32)),
                ("Long", ColumnDefinition.of("Long", dtypes.int64)),
                ("Float", ColumnDefinition.of("Float", dtypes.float32)),
                ("Double", ColumnDefinition.of("Double", dtypes.float64)),
                ("String", ColumnDefinition.of("String", dtypes.string)),
                ("Instant", ColumnDefinition.of("Instant", dtypes.Instant)),
            ],
            list(self.test_definition.items()),
        )

    def test_equals_hash_and_from_columns(self):
        expected_hash = hash(self.test_definition)
        for actual in [
            self.test_definition,
            TableDefinition(
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

        self.assert_table_equals(expected, self.test_definition.table)

    def test_unexpected_type(self):
        with self.assertRaises(DHError):
            TableDefinition(42)


if __name__ == "__main__":
    unittest.main()
