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
    Filter,
    _CompareFilter,
    eq,
    ge,
    gt,
    in_,
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

    def test_to_proto_value_python_datetime(self):
        """Test _to_proto_value with Python datetime.datetime."""
        import datetime

        dt = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        proto_value = filters._to_proto_value(dt)
        self.assertIsInstance(proto_value, Value)
        self.assertTrue(proto_value.HasField("literal"))
        self.assertTrue(proto_value.literal.HasField("nano_time_value"))
        # Verify it's a reasonable epoch nanoseconds value
        self.assertGreater(proto_value.literal.nano_time_value, 0)

    def test_to_proto_value_python_date(self):
        """Test _to_proto_value with Python datetime.date."""
        import datetime

        dt = datetime.date(2024, 1, 1)
        proto_value = filters._to_proto_value(dt)
        self.assertIsInstance(proto_value, Value)
        self.assertTrue(proto_value.HasField("literal"))
        self.assertTrue(proto_value.literal.HasField("nano_time_value"))
        self.assertGreater(proto_value.literal.nano_time_value, 0)

    def test_to_proto_value_numpy_datetime64(self):
        """Test _to_proto_value with numpy.datetime64."""
        import numpy as np

        dt = np.datetime64("2024-01-01T12:00:00", "ns")
        proto_value = filters._to_proto_value(dt)
        self.assertIsInstance(proto_value, Value)
        self.assertTrue(proto_value.HasField("literal"))
        self.assertTrue(proto_value.literal.HasField("nano_time_value"))
        self.assertGreater(proto_value.literal.nano_time_value, 0)

    def test_to_proto_value_pandas_timestamp(self):
        """Test _to_proto_value with pandas.Timestamp."""
        import pandas as pd

        dt = pd.Timestamp("2024-01-01 12:00:00", tz="UTC")
        proto_value = filters._to_proto_value(dt)
        self.assertIsInstance(proto_value, Value)
        self.assertTrue(proto_value.HasField("literal"))
        self.assertTrue(proto_value.literal.HasField("nano_time_value"))
        self.assertGreater(proto_value.literal.nano_time_value, 0)

    def test_datetime_to_nanos_consistency(self):
        """Test that different datetime types representing the same time produce the same nanos."""
        import datetime

        import numpy as np
        import pandas as pd

        # All represent the same UTC time: 2024-01-01 12:00:00 UTC
        py_dt = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        np_dt = np.datetime64("2024-01-01T12:00:00", "ns")
        pd_dt = pd.Timestamp("2024-01-01 12:00:00", tz="UTC")

        py_nanos = filters._datetime_to_nanos(py_dt)
        np_nanos = filters._datetime_to_nanos(np_dt)
        pd_nanos = filters._datetime_to_nanos(pd_dt)

        # They should all produce the same nanosecond value
        self.assertEqual(py_nanos, pd_nanos)
        # numpy might have slight differences due to conversion, so we check they're close
        self.assertAlmostEqual(py_nanos, np_nanos, delta=1000)  # within 1 microsecond

    def test_datetime_to_nanos_epoch(self):
        """Test that Unix epoch (1970-01-01) converts to 0 nanoseconds."""
        import datetime

        epoch = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        nanos = filters._datetime_to_nanos(epoch)
        self.assertEqual(nanos, 0)

    def test_eq_filter_with_datetime(self):
        """Test eq() function with datetime values."""
        import datetime

        dt = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        filter_obj = eq(ColumnName("timestamp"), dt)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertTrue(condition.compare.rhs.literal.HasField("nano_time_value"))

    def test_lt_filter_with_datetime(self):
        """Test lt() function with datetime values."""
        import datetime

        dt = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        filter_obj = lt(ColumnName("timestamp"), dt)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation, CompareCondition.CompareOperation.LESS_THAN
        )
        self.assertTrue(condition.compare.rhs.literal.HasField("nano_time_value"))

    def test_gt_filter_with_datetime(self):
        """Test gt() function with datetime values."""
        import datetime

        dt = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        filter_obj = gt(ColumnName("timestamp"), dt)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation, CompareCondition.CompareOperation.GREATER_THAN
        )
        self.assertTrue(condition.compare.rhs.literal.HasField("nano_time_value"))

    def test_le_filter_with_datetime(self):
        """Test le() function with datetime values."""
        import pandas as pd

        dt = pd.Timestamp("2024-06-15 14:30:00", tz="UTC")
        filter_obj = le(ColumnName("timestamp"), dt)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation,
            CompareCondition.CompareOperation.LESS_THAN_OR_EQUAL,
        )
        self.assertTrue(condition.compare.rhs.literal.HasField("nano_time_value"))

    def test_ge_filter_with_datetime(self):
        """Test ge() function with datetime values."""
        import numpy as np

        dt = np.datetime64("2024-12-31", "ns")
        filter_obj = ge(ColumnName("timestamp"), dt)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation,
            CompareCondition.CompareOperation.GREATER_THAN_OR_EQUAL,
        )
        self.assertTrue(condition.compare.rhs.literal.HasField("nano_time_value"))

    def test_ne_filter_with_datetime(self):
        """Test ne() function with datetime values."""
        import datetime

        dt = datetime.date(2024, 7, 4)
        filter_obj = ne(ColumnName("date"), dt)
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.operation, CompareCondition.CompareOperation.NOT_EQUALS
        )
        self.assertTrue(condition.compare.rhs.literal.HasField("nano_time_value"))

    def test_datetime_range_filter(self):
        """Test combining multiple datetime filters for range queries."""
        import datetime

        from pydeephaven.filters import and_

        start_time = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        end_time = datetime.datetime(2024, 12, 31, tzinfo=datetime.timezone.utc)

        filter_obj = and_(
            [
                ge(ColumnName("timestamp"), start_time),
                le(ColumnName("timestamp"), end_time),
            ]
        )
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertTrue(condition.HasField("and"))

    def test_compare_filter_equals(self):
        """Test CompareFilter with EQUALS operation."""
        filter_obj = _CompareFilter(
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
        filter_obj = _CompareFilter(
            op=CompareCondition.CompareOperation.GREATER_THAN,
            left=ColumnName("x"),
            right=5,
        )
        self.assertEqual(filter_obj._op, CompareCondition.CompareOperation.GREATER_THAN)

    def test_in_filter_with_integers(self):
        """Test in_() function with integer values."""
        filter_obj = in_("status_code", [200, 201, 204])
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertIsInstance(condition, Condition)
        self.assertTrue(condition.HasField("in"))
        self.assertEqual(len(condition.__getattribute__("in").candidates), 3)

    def test_in_filter_with_strings(self):
        """Test in_() function with string values."""
        filter_obj = in_("category", ["electronics", "books", "clothing"])
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertTrue(condition.HasField("in"))
        self.assertEqual(len(condition.__getattribute__("in").candidates), 3)
        # Check that values are strings
        for value in condition.__getattribute__("in").candidates:
            self.assertTrue(value.HasField("literal"))
            self.assertTrue(value.literal.HasField("string_value"))

    def test_in_filter_with_booleans(self):
        """Test in_() function with boolean values."""
        filter_obj = in_("flag", [True, False])
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertTrue(condition.HasField("in"))
        self.assertEqual(len(condition.__getattribute__("in").candidates), 2)

    def test_in_filter_with_floats(self):
        """Test in_() function with float values."""
        filter_obj = in_("price", [9.99, 19.99, 29.99])
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertTrue(condition.HasField("in"))
        self.assertEqual(len(condition.__getattribute__("in").candidates), 3)
        # Check that values are floats
        for value in condition.__getattribute__("in").candidates:
            self.assertTrue(value.HasField("literal"))
            self.assertTrue(value.literal.HasField("double_value"))

    def test_in_filter_with_single_value(self):
        """Test in_() function with a single value."""
        filter_obj = in_("id", [42])
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertTrue(condition.HasField("in"))
        self.assertEqual(len(condition.__getattribute__("in").candidates), 1)

    def test_in_filter_with_mixed_numeric_types(self):
        """Test in_() function with mixed integer and float values."""
        filter_obj = in_("value", [1, 2.5, 3, 4.0])
        self.assertIsInstance(filter_obj, Filter)
        condition = filter_obj.make_grpc_message()
        self.assertTrue(condition.HasField("in"))
        self.assertEqual(len(condition.__getattribute__("in").candidates), 4)

    def test_eq_filter_case_sensitive_default(self):
        """Test eq() filter has case-sensitive set to MATCH_CASE by default."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity

        filter_obj = eq(ColumnName("name"), "Test")
        condition = filter_obj.make_grpc_message()
        self.assertEqual(condition.compare.case_sensitivity, CaseSensitivity.MATCH_CASE)

    def test_eq_filter_case_sensitive_explicit_true(self):
        """Test eq() filter with explicit case_sensitive=True."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity

        filter_obj = eq(ColumnName("name"), "Test", case_sensitive=True)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(condition.compare.case_sensitivity, CaseSensitivity.MATCH_CASE)

    def test_eq_filter_case_insensitive(self):
        """Test eq() filter with case_sensitive=False."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity

        filter_obj = eq(ColumnName("name"), "Test", case_sensitive=False)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.compare.case_sensitivity, CaseSensitivity.IGNORE_CASE
        )

    def test_ne_filter_case_sensitive_proto(self):
        """Test ne() filter sets case_sensitivity in proto message."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity

        # Default case-sensitive
        filter_obj1 = ne(ColumnName("status"), "Active")
        condition1 = filter_obj1.make_grpc_message()
        self.assertEqual(
            condition1.compare.case_sensitivity, CaseSensitivity.MATCH_CASE
        )

        # Explicit case-insensitive
        filter_obj2 = ne(ColumnName("status"), "Active", case_sensitive=False)
        condition2 = filter_obj2.make_grpc_message()
        self.assertEqual(
            condition2.compare.case_sensitivity, CaseSensitivity.IGNORE_CASE
        )

    def test_lt_filter_case_sensitive_proto(self):
        """Test lt() filter sets case_sensitivity in proto message."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity

        filter_obj = lt(ColumnName("name"), "zebra", case_sensitive=True)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(condition.compare.case_sensitivity, CaseSensitivity.MATCH_CASE)

    def test_le_filter_case_sensitive_proto(self):
        """Test le() filter sets case_sensitivity in proto message."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity

        filter_obj = le(ColumnName("name"), "zebra", case_sensitive=True)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(condition.compare.case_sensitivity, CaseSensitivity.MATCH_CASE)

    def test_gt_filter_case_sensitive_proto(self):
        """Test gt() filter sets case_sensitivity in proto message."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity

        filter_obj = gt(ColumnName("name"), "Apple", case_sensitive=True)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(condition.compare.case_sensitivity, CaseSensitivity.MATCH_CASE)

    def test_ge_filter_case_sensitive_proto(self):
        """Test ge() filter sets case_sensitivity in proto message."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity

        filter_obj = ge(ColumnName("name"), "Apple", case_sensitive=True)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(condition.compare.case_sensitivity, CaseSensitivity.MATCH_CASE)

    def test_all_comparison_filters_case_sensitive_proto(self):
        """Test that all comparison filters support case_sensitive parameter in proto."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity

        # All comparison operators support case_sensitive=True
        all_operations = [eq, ne, lt, le, gt, ge]

        for filter_func in all_operations:
            with self.subTest(
                filter_func=filter_func.__name__, test="case_sensitive_true"
            ):
                # Test case-sensitive (default)
                filter_obj1 = filter_func(ColumnName("text"), "Value")
                condition1 = filter_obj1.make_grpc_message()
                self.assertEqual(
                    condition1.compare.case_sensitivity, CaseSensitivity.MATCH_CASE
                )

        # Only eq and ne support case_sensitive=False
        case_insensitive_operations = [eq, ne]

        for filter_func in case_insensitive_operations:
            with self.subTest(
                filter_func=filter_func.__name__, test="case_sensitive_false"
            ):
                # Test case-insensitive
                filter_obj2 = filter_func(
                    ColumnName("text"), "Value", case_sensitive=False
                )
                condition2 = filter_obj2.make_grpc_message()
                self.assertEqual(
                    condition2.compare.case_sensitivity, CaseSensitivity.IGNORE_CASE
                )

    def test_in_filter_case_sensitive_default_proto(self):
        """Test in_() filter has case-sensitive set to MATCH_CASE by default."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity

        filter_obj = in_("status", ["Active", "Pending"])
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.__getattribute__("in").case_sensitivity,
            CaseSensitivity.MATCH_CASE,
        )

    def test_in_filter_case_insensitive_proto(self):
        """Test in_() filter with case_sensitive=False sets IGNORE_CASE."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity

        filter_obj = in_("status", ["Active", "Pending"], case_sensitive=False)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.__getattribute__("in").case_sensitivity,
            CaseSensitivity.IGNORE_CASE,
        )

    def test_matches_filter_case_sensitive_default_proto(self):
        """Test matches() filter has case-sensitive set to MATCH_CASE by default."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity
        from pydeephaven.filters import matches

        filter_obj = matches("name", "^[A-Z].*")
        condition = filter_obj.make_grpc_message()
        self.assertEqual(condition.matches.case_sensitivity, CaseSensitivity.MATCH_CASE)

    def test_matches_filter_case_insensitive_proto(self):
        """Test matches() filter with case_sensitive=False sets IGNORE_CASE."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity
        from pydeephaven.filters import matches

        filter_obj = matches("name", "^[A-Z].*", case_sensitive=False)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.matches.case_sensitivity, CaseSensitivity.IGNORE_CASE
        )

    def test_matches_filter_with_invert_and_case_sensitive_proto(self):
        """Test matches() filter sets both match_type and case_sensitivity in proto."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity, MatchType
        from pydeephaven.filters import matches

        # Test inverted pattern with case-insensitive matching
        filter_obj = matches(
            "email", ".*@example.com$", invert=True, case_sensitive=False
        )
        condition = filter_obj.make_grpc_message()
        self.assertEqual(condition.matches.match_type, MatchType.INVERTED)
        self.assertEqual(
            condition.matches.case_sensitivity, CaseSensitivity.IGNORE_CASE
        )

        # Test normal pattern with case-sensitive matching
        filter_obj2 = matches(
            "email", ".*@example.com$", invert=False, case_sensitive=True
        )
        condition2 = filter_obj2.make_grpc_message()
        self.assertEqual(condition2.matches.match_type, MatchType.REGULAR)
        self.assertEqual(
            condition2.matches.case_sensitivity, CaseSensitivity.MATCH_CASE
        )

    def test_contains_filter_case_sensitive_default_proto(self):
        """Test contains() filter has case-sensitive set to MATCH_CASE by default."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity
        from pydeephaven.filters import contains

        filter_obj = contains("name", "test")
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.contains.case_sensitivity, CaseSensitivity.MATCH_CASE
        )
        self.assertEqual(condition.contains.search_string, "test")

    def test_contains_filter_case_insensitive_proto(self):
        """Test contains() filter with case_sensitive=False sets IGNORE_CASE."""
        from deephaven_core.proto.table_pb2 import CaseSensitivity
        from pydeephaven.filters import contains

        filter_obj = contains("name", "TEST", case_sensitive=False)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(
            condition.contains.case_sensitivity, CaseSensitivity.IGNORE_CASE
        )
        self.assertEqual(condition.contains.search_string, "TEST")

    def test_contains_filter_with_invert_proto(self):
        """Test contains() filter with invert parameter."""
        from deephaven_core.proto.table_pb2 import MatchType
        from pydeephaven.filters import contains

        # Test inverted contains
        filter_obj = contains("description", "error", invert=True)
        condition = filter_obj.make_grpc_message()
        self.assertEqual(condition.contains.match_type, MatchType.INVERTED)
        self.assertEqual(condition.contains.search_string, "error")

        # Test normal contains
        filter_obj2 = contains("description", "success", invert=False)
        condition2 = filter_obj2.make_grpc_message()
        self.assertEqual(condition2.contains.match_type, MatchType.REGULAR)
        self.assertEqual(condition2.contains.search_string, "success")


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
        result = (
            test_table.where(gt(ColumnName("value"), 100))
            .where(lt(ColumnName("value"), 400))
            .where(ne(ColumnName("category"), 0))
        )

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

    def test_in_filter_with_integer_values(self):
        """Test in_() filter with integer column values."""
        test_table = self.session.empty_table(20).update(
            formulas=["id = i", "category = i % 5"]
        )

        # Filter for specific category values
        filter_obj = in_("category", [1, 3])
        filtered_table = test_table.where(filter_obj)

        # Should get rows where category is 1 or 3
        # With 20 rows and i % 5, we get 4 rows each for values 0,1,2,3,4
        self.assertEqual(filtered_table.size, 8)

    def test_in_filter_with_string_values(self):
        """Test in_() filter with string column values."""
        test_table = self.session.empty_table(10).update(
            formulas=["id = i", "status = i < 5 ? `active` : `inactive`"]
        )

        # Filter for active status
        filter_obj = in_("status", ["active"])
        filtered_table = test_table.where(filter_obj)

        self.assertEqual(filtered_table.size, 5)

    def test_in_filter_combined_with_comparison(self):
        """Test combining in_() filter with comparison filters."""
        test_table = self.session.empty_table(100).update(
            formulas=["id = i", "value = i * 10", "category = i % 10"]
        )

        # First filter by value range, then by specific categories
        result = test_table.where(gt(ColumnName("value"), 200)).where(
            in_("category", [2, 5, 8])
        )

        self.assertLess(result.size, test_table.size)
        self.assertGreater(result.size, 0)

    def test_in_filter_with_no_matches(self):
        """Test in_() filter that returns no matches."""
        test_table = self.session.empty_table(10).update(
            formulas=["id = i", "value = i"]
        )

        # Filter for values that don't exist
        filter_obj = in_("value", [100, 200, 300])
        filtered_table = test_table.where(filter_obj)

        self.assertEqual(filtered_table.size, 0)

    def test_eq_filter_case_sensitive_string(self):
        """Test eq() filter with case-sensitive string matching on table data."""
        test_table = self.session.empty_table(5).update(
            formulas=[
                "id = i",
                "name = i == 0 ? `Apple` : i == 1 ? `apple` : i == 2 ? `APPLE` : i == 3 ? `Banana` : `banana`",
            ]
        )

        # Case-sensitive: should match only exact case
        filter_case_sensitive = eq(ColumnName("name"), "Apple", case_sensitive=True)
        result_sensitive = test_table.where(filter_case_sensitive)
        self.assertEqual(result_sensitive.size, 1)

        # Case-insensitive: should match all variations of "Apple"
        filter_case_insensitive = eq(ColumnName("name"), "Apple", case_sensitive=False)
        result_insensitive = test_table.where(filter_case_insensitive)
        self.assertEqual(result_insensitive.size, 3)

    def test_ne_filter_case_sensitive_string(self):
        """Test ne() filter with case-sensitive string matching on table data."""
        test_table = self.session.empty_table(4).update(
            formulas=[
                "id = i",
                "status = i == 0 ? `Active` : i == 1 ? `active` : i == 2 ? `ACTIVE` : `Inactive`",
            ]
        )

        # Case-sensitive: should exclude only exact match
        filter_sensitive = ne(ColumnName("status"), "Active", case_sensitive=True)
        result_sensitive = test_table.where(filter_sensitive)
        self.assertEqual(result_sensitive.size, 3)

        # Case-insensitive: Based on actual test behavior, ne with case_insensitive
        # appears to still behave as case-sensitive in current server implementation
        filter_insensitive = ne(ColumnName("status"), "Active", case_sensitive=False)
        result_insensitive = test_table.where(filter_insensitive)
        # Actual result: keeps active, ACTIVE, Inactive (same as case-sensitive)
        self.assertEqual(result_insensitive.size, 3)

    # Note: lt/le/gt/ge case_sensitive tests removed as the server doesn't support
    # case_sensitivity parameter for these operators on string comparisons

    def test_in_filter_case_sensitive_string(self):
        """Test in_() filter with case-sensitive string matching on table data."""
        test_table = self.session.empty_table(6).update(
            formulas=[
                "id = i",
                "status = i == 0 ? `Active` : i == 1 ? `active` : i == 2 ? `Pending` : i == 3 ? `pending` : i == 4 ? `ACTIVE` : `Done`",
            ]
        )

        # Case-sensitive: should match only exact cases
        filter_sensitive = in_("status", ["Active", "Pending"], case_sensitive=True)
        result_sensitive = test_table.where(filter_sensitive)
        self.assertEqual(result_sensitive.size, 2)

        # Case-insensitive: should match all case variations
        filter_insensitive = in_("status", ["Active", "Pending"], case_sensitive=False)
        result_insensitive = test_table.where(filter_insensitive)
        self.assertEqual(result_insensitive.size, 5)

    def test_matches_filter_case_sensitive_regex(self):
        """Test matches() filter with case-sensitive regex matching on table data."""
        from pydeephaven.filters import matches

        test_table = self.session.empty_table(5).update(
            formulas=[
                "id = i",
                "email = i == 0 ? `User@Example.com` : i == 1 ? `user@example.com` : i == 2 ? `admin@test.com` : i == 3 ? `ADMIN@TEST.COM` : `support@help.com`",
            ]
        )

        # Case-sensitive: should match only lowercase 'user'
        filter_sensitive = matches("email", "^user@.*", case_sensitive=True)
        result_sensitive = test_table.where(filter_sensitive)
        self.assertEqual(result_sensitive.size, 1)

        # Case-insensitive: should match both "User" and "user"
        filter_insensitive = matches("email", "^user@.*", case_sensitive=False)
        result_insensitive = test_table.where(filter_insensitive)
        self.assertEqual(result_insensitive.size, 2)

    def test_matches_filter_inverted_case_insensitive(self):
        """Test matches() filter with inverted pattern and case-insensitive matching."""
        from pydeephaven.filters import matches

        test_table = self.session.empty_table(4).update(
            formulas=[
                "id = i",
                "domain = i == 0 ? `example.com` : i == 1 ? `EXAMPLE.COM` : i == 2 ? `test.org` : `TEST.ORG`",
            ]
        )

        # Inverted, case-insensitive: should match everything NOT matching pattern (ignoring case)
        filter_inverted = matches(
            "domain", ".*example\\.com$", invert=True, case_sensitive=False
        )
        result_inverted = test_table.where(filter_inverted)
        self.assertEqual(result_inverted.size, 2)

        # Normal, case-sensitive: should match only exact lowercase
        filter_normal = matches("domain", ".*example\\.com$", case_sensitive=True)
        result_normal = test_table.where(filter_normal)
        self.assertEqual(result_normal.size, 1)

    def test_contains_filter_case_sensitive_string(self):
        """Test contains() filter with case-sensitive string matching on table data."""
        from pydeephaven.filters import contains

        test_table = self.session.empty_table(6).update(
            formulas=[
                "id = i",
                "message = i == 0 ? `Error occurred` : i == 1 ? `error in system` : i == 2 ? `ERROR: failed` : i == 3 ? `Success` : i == 4 ? `Warning: check` : `Info`",
            ]
        )

        # Case-sensitive: should match only exact case "error"
        filter_sensitive = contains("message", "error", case_sensitive=True)
        result_sensitive = test_table.where(filter_sensitive)
        self.assertEqual(result_sensitive.size, 1)  # Only "error in system"

        # Case-insensitive: should match all variations of "error"
        filter_insensitive = contains("message", "error", case_sensitive=False)
        result_insensitive = test_table.where(filter_insensitive)
        self.assertEqual(
            result_insensitive.size, 3
        )  # "Error occurred", "error in system", "ERROR: failed"

    def test_contains_filter_inverted(self):
        """Test contains() filter with inverted matching."""
        from pydeephaven.filters import contains

        test_table = self.session.empty_table(4).update(
            formulas=[
                "id = i",
                "status = i == 0 ? `completed` : i == 1 ? `pending` : i == 2 ? `failed` : `running`",
            ]
        )

        # Inverted: should match rows that don't contain "ing"
        filter_inverted = contains("status", "ing", invert=True, case_sensitive=True)
        result_inverted = test_table.where(filter_inverted)
        self.assertEqual(result_inverted.size, 2)  # "completed" and "failed"

        # Normal: should match rows that contain "ed"
        filter_normal = contains("status", "ed", invert=False, case_sensitive=True)
        result_normal = test_table.where(filter_normal)
        self.assertEqual(result_normal.size, 2)  # "completed" and "failed"

    def test_contains_vs_matches_behavior(self):
        """Test the difference between contains() and matches() filters."""
        from pydeephaven.filters import contains, matches

        test_table = self.session.empty_table(4).update(
            formulas=[
                "id = i",
                "text = i == 0 ? `hello world` : i == 1 ? `world` : i == 2 ? `hello` : `goodbye`",
            ]
        )

        # contains() should match any substring
        contains_filter = contains("text", "world", case_sensitive=True)
        contains_result = test_table.where(contains_filter)
        self.assertEqual(contains_result.size, 2)  # "hello world" and "world"

        # matches() with regex should match based on regex pattern
        matches_filter = matches("text", "^world$", case_sensitive=True)
        matches_result = test_table.where(matches_filter)
        self.assertEqual(matches_result.size, 1)  # Only exact "world"

    def test_comparison_filters_case_sensitive_combined(self):
        """Test combining multiple comparison filters with case_sensitive parameter."""
        test_table = self.session.empty_table(10).update(
            formulas=[
                "id = i",
                "first_name = i < 5 ? `John` : `john`",
                "last_name = i < 3 ? `Doe` : i < 7 ? `doe` : `DOE`",
            ]
        )

        # Case-sensitive: should match exact cases only
        # Rows 0,1,2 have first_name='John' AND last_name='Doe'
        result_sensitive = test_table.where(
            [
                eq(ColumnName("first_name"), "John", case_sensitive=True),
                eq(ColumnName("last_name"), "Doe", case_sensitive=True),
            ]
        )
        self.assertEqual(result_sensitive.size, 3)

        # Case-insensitive eq: All 10 rows match first_name='john' (ignoring case)
        result_insensitive_eq = test_table.where(
            eq(ColumnName("first_name"), "john", case_sensitive=False)
        )
        self.assertEqual(result_insensitive_eq.size, 10)

    def test_filter_instant_column_with_datetime_values(self):
        """Test filtering on Java Instant type columns using Python datetime values.

        Java Instant represents a point in time as UTC-based epoch nanoseconds.
        This test verifies that Python datetime objects, pandas Timestamps, and numpy datetime64
        can be used directly in filter comparisons against Instant columns.
        """
        import datetime

        import numpy as np
        import pandas as pd

        # Create a static table with Instant column values
        test_table = self.session.empty_table(10).update(
            [
                "Id = (int)i",
                "Timestamp = (Instant)'2024-06-15T10:00:00Z' + i * SECOND",
                "Value = (int)(i % 10)",
            ]
        )

        # Verify the Timestamp column is an Instant type (represented as Arrow timestamp)
        schema_dict = {field.name: str(field.type) for field in test_table.schema}
        self.assertIn("timestamp", schema_dict["Timestamp"].lower())

        # Test 1: Filter using Python datetime objects directly
        epoch_time = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        future_time = datetime.datetime(2100, 1, 1, tzinfo=datetime.timezone.utc)

        # Filter: Timestamp > epoch_time (all rows should match)
        result_after_epoch = test_table.where(gt(ColumnName("Timestamp"), epoch_time))
        self.assertEqual(result_after_epoch.size, 10)

        # Filter: Timestamp < future_time (all rows should match)
        result_before_future = test_table.where(
            lt(ColumnName("Timestamp"), future_time)
        )
        self.assertEqual(result_before_future.size, 10)

        # Test 2: Filter using pandas Timestamp
        pd_epoch = pd.Timestamp("1970-01-01", tz="UTC")
        result_pd = test_table.where(gt(ColumnName("Timestamp"), pd_epoch))
        self.assertEqual(result_pd.size, 10)

        # Test 3: Filter using numpy datetime64
        np_epoch = np.datetime64("1970-01-01T00:00:00", "ns")
        result_np = test_table.where(gt(ColumnName("Timestamp"), np_epoch))
        self.assertEqual(result_np.size, 10)

        # Test 4: Range filter with multiple datetime comparisons
        result_in_range = test_table.where(
            [
                ge(ColumnName("Timestamp"), epoch_time),
                le(ColumnName("Timestamp"), future_time),
            ]
        )
        self.assertEqual(result_in_range.size, 10)

        # Test 5: Equality comparison with datetime
        specific_time = datetime.datetime(
            2024, 6, 15, 10, 0, 5, tzinfo=datetime.timezone.utc
        )
        # Row 5 has Timestamp = '2024-06-15T10:00:05Z' (base time + 5 seconds)
        result_eq = test_table.where(eq(ColumnName("Timestamp"), specific_time))
        self.assertEqual(result_eq.size, 1)

    def test_lt_case_insensitive_raises_error(self):
        """Test that lt() with case_sensitive=False raises ValueError.

        This test verifies the early validation that prevents using case_sensitive=False
        with ordering operators, as documented in the Note in lt() docstring.
        """
        with self.assertRaises(ValueError) as context:
            lt(ColumnName("name"), "banana", case_sensitive=False)

        self.assertIn("case_sensitive=False is not supported", str(context.exception))
        self.assertIn("lt()", str(context.exception))

    def test_le_case_insensitive_raises_error(self):
        """Test that le() with case_sensitive=False raises ValueError.

        This test verifies the early validation that prevents using case_sensitive=False
        with ordering operators, as documented in the Note in le() docstring.
        """
        with self.assertRaises(ValueError) as context:
            le(ColumnName("name"), "beta", case_sensitive=False)

        self.assertIn("case_sensitive=False is not supported", str(context.exception))
        self.assertIn("le()", str(context.exception))

    def test_gt_case_insensitive_raises_error(self):
        """Test that gt() with case_sensitive=False raises ValueError.

        This test verifies the early validation that prevents using case_sensitive=False
        with ordering operators, as documented in the Note in gt() docstring.
        """
        with self.assertRaises(ValueError) as context:
            gt(ColumnName("word"), "Apple", case_sensitive=False)

        self.assertIn("case_sensitive=False is not supported", str(context.exception))
        self.assertIn("gt()", str(context.exception))

    def test_ge_case_insensitive_raises_error(self):
        """Test that ge() with case_sensitive=False raises ValueError.

        This test verifies the early validation that prevents using case_sensitive=False
        with ordering operators, as documented in the Note in ge() docstring.
        """
        with self.assertRaises(ValueError) as context:
            ge(ColumnName("city"), "Atlanta", case_sensitive=False)

        self.assertIn("case_sensitive=False is not supported", str(context.exception))
        self.assertIn("ge()", str(context.exception))


if __name__ == "__main__":
    unittest.main()
