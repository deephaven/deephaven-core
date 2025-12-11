#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven_core.proto.table_pb2 import (
    CompareCondition,
    Condition,
    Value,
)
from pydeephaven import filters
from pydeephaven.filters import (
    ColumnName,
    CompareFilter,
    Filter,
    eq,
    ge,
    gt,
    le,
    lt,
    ne,
)
from tests.testbase import BaseTestCase


class FiltersTestCase(BaseTestCase):
    """Test cases for the filters module."""

    def test_column_name_creation(self):
        """Test ColumnName class creation and behavior."""
        col = ColumnName("test_column")
        self.assertIsInstance(col, str)
        self.assertEqual(col, "test_column")
        self.assertEqual(str(col), "test_column")

    def test_to_proto_value_column_name(self):
        """Test _to_proto_value with ColumnName."""
        col = ColumnName("my_column")
        proto_value = filters._to_proto_value(col)
        self.assertIsInstance(proto_value, Value)
        self.assertTrue(proto_value.HasField("reference"))
        self.assertEqual(proto_value.reference.column_name, "my_column")

    def test_to_proto_value_string(self):
        """Test _to_proto_value with string literal."""
        proto_value = filters._to_proto_value("test_string")
        self.assertIsInstance(proto_value, Value)
        self.assertTrue(proto_value.HasField("literal"))
        self.assertEqual(proto_value.literal.string_value, "test_string")

    def test_to_proto_value_bool(self):
        """Test _to_proto_value with boolean literal."""
        proto_value_true = filters._to_proto_value(True)
        self.assertIsInstance(proto_value_true, Value)
        self.assertTrue(proto_value_true.HasField("literal"))
        self.assertTrue(proto_value_true.literal.bool_value)

        proto_value_false = filters._to_proto_value(False)
        self.assertFalse(proto_value_false.literal.bool_value)

    def test_to_proto_value_int(self):
        """Test _to_proto_value with integer literal."""
        proto_value = filters._to_proto_value(42)
        self.assertIsInstance(proto_value, Value)
        self.assertTrue(proto_value.HasField("literal"))
        self.assertEqual(proto_value.literal.long_value, 42)

    def test_to_proto_value_float(self):
        """Test _to_proto_value with float literal."""
        proto_value = filters._to_proto_value(3.14)
        self.assertIsInstance(proto_value, Value)
        self.assertTrue(proto_value.HasField("literal"))
        self.assertAlmostEqual(proto_value.literal.double_value, 3.14)

    def test_to_proto_value_unsupported_type(self):
        """Test _to_proto_value with unsupported type raises TypeError."""
        with self.assertRaises(TypeError) as context:
            filters._to_proto_value([1, 2, 3])  # type: ignore
        self.assertIn("Unsupported type", str(context.exception))

    def test_compare_filter_equals(self):
        """Test CompareFilter with EQUALS operation."""
        filter_obj = CompareFilter(
            op=CompareCondition.CompareOperation.EQUALS,
            left=ColumnName("col_a"),
            right=10,
        )
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertIsInstance(condition, Condition)
        self.assertTrue(condition.HasField("compare"))
        self.assertEqual(
            condition.compare.operation, CompareCondition.CompareOperation.EQUALS
        )

    def test_eq_filter_column_to_literal(self):
        """Test eq() function with column name and literal value."""
        filter_obj = eq(ColumnName("price"), 100)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation, CompareCondition.CompareOperation.EQUALS
        )

    def test_eq_filter_column_to_column(self):
        """Test eq() function with two column names."""
        filter_obj = eq(ColumnName("col_a"), ColumnName("col_b"))
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation, CompareCondition.CompareOperation.EQUALS
        )
        self.assertTrue(condition.compare.lhs.HasField("reference"))
        self.assertTrue(condition.compare.rhs.HasField("reference"))

    def test_eq_filter_literal_to_literal(self):
        """Test eq() function with two literal values."""
        filter_obj = eq(5, 5)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation, CompareCondition.CompareOperation.EQUALS
        )

    def test_ne_filter(self):
        """Test ne() function creates NOT_EQUALS filter."""
        filter_obj = ne(ColumnName("status"), "inactive")
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation, CompareCondition.CompareOperation.NOT_EQUALS
        )

    def test_lt_filter(self):
        """Test lt() function creates LESS_THAN filter."""
        filter_obj = lt(ColumnName("age"), 65)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation, CompareCondition.CompareOperation.LESS_THAN
        )

    def test_le_filter(self):
        """Test le() function creates LESS_THAN_OR_EQUAL filter."""
        filter_obj = le(ColumnName("score"), 100.0)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation,
            CompareCondition.CompareOperation.LESS_THAN_OR_EQUAL,
        )

    def test_gt_filter(self):
        """Test gt() function creates GREATER_THAN filter."""
        filter_obj = gt(ColumnName("temperature"), 32.0)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation, CompareCondition.CompareOperation.GREATER_THAN
        )

    def test_ge_filter(self):
        """Test ge() function creates GREATER_THAN_OR_EQUAL filter."""
        filter_obj = ge(ColumnName("count"), 0)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation,
            CompareCondition.CompareOperation.GREATER_THAN_OR_EQUAL,
        )

    def test_filter_with_bool_values(self):
        """Test filter operations with boolean values."""
        filter_obj = eq(ColumnName("is_active"), True)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertTrue(condition.compare.rhs.literal.bool_value)

        filter_obj2 = ne(ColumnName("is_deleted"), False)
        condition2 = filter_obj2.make_grpc_message()
        self.assertFalse(condition2.compare.rhs.literal.bool_value)

    def test_filter_with_string_values(self):
        """Test filter operations with string values."""
        filter_obj = eq(ColumnName("city"), "New York")
        condition = filter_obj.make_grpc_message()
        self.assertEqual(condition.compare.rhs.literal.string_value, "New York")

        filter_obj2 = ne(ColumnName("country"), "USA")
        condition2 = filter_obj2.make_grpc_message()
        self.assertEqual(condition2.compare.rhs.literal.string_value, "USA")

    def test_filter_with_numeric_values(self):
        """Test filter operations with various numeric values."""
        # Integer
        filter_int = gt(ColumnName("count"), 100)
        condition_int = filter_int.make_grpc_message()
        self.assertEqual(condition_int.compare.rhs.literal.long_value, 100)

        # Float
        filter_float = lt(ColumnName("price"), 99.99)
        condition_float = filter_float.make_grpc_message()
        self.assertAlmostEqual(condition_float.compare.rhs.literal.double_value, 99.99)

        # Negative numbers
        filter_neg = ge(ColumnName("balance"), -50)
        condition_neg = filter_neg.make_grpc_message()
        self.assertEqual(condition_neg.compare.rhs.literal.long_value, -50)

    def test_all_comparison_operations(self):
        """Test that all comparison operations create proper filters."""
        operations = [
            (eq, CompareCondition.CompareOperation.EQUALS),
            (ne, CompareCondition.CompareOperation.NOT_EQUALS),
            (lt, CompareCondition.CompareOperation.LESS_THAN),
            (le, CompareCondition.CompareOperation.LESS_THAN_OR_EQUAL),
            (gt, CompareCondition.CompareOperation.GREATER_THAN),
            (ge, CompareCondition.CompareOperation.GREATER_THAN_OR_EQUAL),
        ]

        for filter_func, expected_op in operations:
            with self.subTest(filter_func=filter_func.__name__):
                filter_obj = filter_func(ColumnName("test_col"), 42)
                condition = filter_obj.make_grpc_message()
                self.assertEqual(condition.compare.operation, expected_op)

    def test_filter_abstract_base_class(self):
        """Test that Filter is an abstract base class."""
        # Cannot instantiate Filter directly
        with self.assertRaises(TypeError):
            Filter()  # type: ignore

    def test_compare_filter_stores_operation(self):
        """Test that CompareFilter properly stores the operation."""
        filter_obj = CompareFilter(
            op=CompareCondition.CompareOperation.GREATER_THAN,
            left=ColumnName("x"),
            right=5,
        )
        self.assertEqual(filter_obj._op, CompareCondition.CompareOperation.GREATER_THAN)


class FiltersIntegrationTestCase(BaseTestCase):
    """Integration tests for using Filter objects with Table operations."""

    def test_single_filter_with_where(self):
        """Test using a single Filter object with Table.where()."""
        # Create a test table
        test_table = self.session.empty_table(100).update(
            formulas=["id = i", "value = i * 10"]
        )

        # Apply filter using Filter object
        filter_obj = gt(ColumnName("value"), 500)
        filtered_table = test_table.where(filter_obj)

        # Verify filtering worked
        self.assertLess(filtered_table.size, test_table.size)
        self.assertGreater(filtered_table.size, 0)

        # Verify result matches string filter
        string_filtered = test_table.where("value > 500")
        self.assertEqual(filtered_table.size, string_filtered.size)

    def test_multiple_filters_with_where(self):
        """Test using multiple Filter objects with Table.where()."""
        # Create a test table
        test_table = self.session.empty_table(100).update(
            formulas=["id = i", "value = i * 10", "flag = i % 2 == 0"]
        )

        # Apply multiple filters using Filter objects
        filter1 = gt(ColumnName("value"), 200)
        filter2 = lt(ColumnName("value"), 800)
        filtered_table = test_table.where([filter1, filter2])

        # Verify filtering worked
        self.assertLess(filtered_table.size, test_table.size)
        self.assertGreater(filtered_table.size, 0)

        # Verify result matches string filters
        string_filtered = test_table.where(["value > 200", "value < 800"])
        self.assertEqual(filtered_table.size, string_filtered.size)

    def test_equality_filter_on_csv_data(self):
        """Test eq() filter on imported CSV data."""
        from pyarrow import csv

        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)

        # Apply equality filter
        filter_obj = eq(ColumnName("a"), 50)
        filtered_table = test_table.where(filter_obj)

        # Verify filtering worked
        self.assertLessEqual(filtered_table.size, pa_table.num_rows)

    def test_comparison_filters_on_numeric_data(self):
        """Test various comparison filters on numeric data."""
        test_table = self.session.empty_table(50).update(
            formulas=["score = i * 2", "grade = i % 5"]
        )

        # Test greater than
        gt_filtered = test_table.where(gt(ColumnName("score"), 60))
        self.assertLess(gt_filtered.size, test_table.size)

        # Test less than or equal
        le_filtered = test_table.where(le(ColumnName("grade"), 2))
        self.assertLess(le_filtered.size, test_table.size)

        # Test not equals
        ne_filtered = test_table.where(ne(ColumnName("grade"), 0))
        self.assertLess(ne_filtered.size, test_table.size)

        # Test greater than or equal
        ge_filtered = test_table.where(ge(ColumnName("score"), 0))
        self.assertEqual(ge_filtered.size, test_table.size)

    def test_filter_on_boolean_column(self):
        """Test filter on boolean column data."""
        test_table = self.session.empty_table(100).update(
            formulas=["id = i", "is_even = i % 2 == 0", "is_large = i > 50"]
        )

        # Filter by boolean true
        true_filtered = test_table.where(eq(ColumnName("is_even"), True))
        self.assertEqual(true_filtered.size, 50)

        # Filter by boolean false
        false_filtered = test_table.where(eq(ColumnName("is_large"), False))
        self.assertLessEqual(false_filtered.size, 51)

    def test_filter_chaining(self):
        """Test chaining multiple where() calls with Filter objects."""
        test_table = self.session.empty_table(100).update(
            formulas=["id = i", "value = i * 5", "category = i % 3"]
        )

        # Chain filters
        result = (test_table
                  .where(gt(ColumnName("value"), 100))
                  .where(lt(ColumnName("value"), 400))
                  .where(ne(ColumnName("category"), 0)))

        self.assertLess(result.size, test_table.size)
        self.assertGreater(result.size, 0)

    def test_filter_with_float_values(self):
        """Test filters with floating point values."""
        test_table = self.session.empty_table(50).update(
            formulas=["id = i", "price = i * 1.5 + 0.5"]
        )

        # Test with float comparison
        filter_obj = lt(ColumnName("price"), 50.5)
        filtered_table = test_table.where(filter_obj)

        self.assertLess(filtered_table.size, test_table.size)
        self.assertGreater(filtered_table.size, 0)

    def test_filter_comparing_two_columns(self):
        """Test filter that compares two columns."""
        test_table = self.session.empty_table(100).update(
            formulas=["id = i", "value1 = i * 2", "value2 = i * 3"]
        )

        # Filter where value2 > value1 (should be all rows except i=0 where both are 0)
        filter_obj = gt(ColumnName("value2"), ColumnName("value1"))
        filtered_table = test_table.where(filter_obj)

        self.assertEqual(filtered_table.size, test_table.size - 1)

        # Filter where value1 > value2 (should be no rows)
        filter_obj2 = gt(ColumnName("value1"), ColumnName("value2"))
        filtered_table2 = test_table.where(filter_obj2)

        self.assertEqual(filtered_table2.size, 0)

    def test_filter_edge_case_zero_results(self):
        """Test filter that returns zero results."""
        test_table = self.session.empty_table(50).update(
            formulas=["id = i", "value = i"]
        )

        # Filter that should return no results
        filter_obj = gt(ColumnName("value"), 100)
        filtered_table = test_table.where(filter_obj)

        self.assertEqual(filtered_table.size, 0)

    def test_filter_edge_case_all_results(self):
        """Test filter that returns all results."""
        test_table = self.session.empty_table(50).update(
            formulas=["id = i", "value = i"]
        )

        # Filter that should return all results
        filter_obj = ge(ColumnName("value"), 0)
        filtered_table = test_table.where(filter_obj)

        self.assertEqual(filtered_table.size, test_table.size)


if __name__ == "__main__":
    unittest.main()
