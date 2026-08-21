#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import json
import unittest

import jpy

from deephaven import dtypes
from deephaven.jcompat import (
    AutoCloseable,
    j_array_list,
    j_collection_to_list,
    j_function,
    j_hashset,
    j_json_value,
    j_lambda,
)
from tests.testbase import BaseTestCase

_JSharedContext = jpy.get_type("io.deephaven.engine.table.SharedContext")
_JMap = jpy.get_type("java.util.Map")
_JList = jpy.get_type("java.util.List")
_JObjectMapper = jpy.get_type("com.fasterxml.jackson.databind.ObjectMapper")


class JCompatTestCase(BaseTestCase):
    def test_j_function(self):
        def int_to_str(v: int) -> str:
            return str(v)

        j_func = j_function(int_to_str, dtypes.string)

        r = j_func.apply(10)
        self.assertEqual(r, "10")

    def test_j_lambda(self):
        def int_to_str(v: int) -> str:
            return str(v)

        j_func = j_lambda(
            int_to_str, jpy.get_type("java.util.function.Function"), dtypes.string
        )

        r = j_func.apply(10)
        self.assertEqual(r, "10")

    def test_auto_closeable(self):
        auto_closeable = AutoCloseable(_JSharedContext.makeSharedContext())
        with auto_closeable:
            self.assertEqual(auto_closeable.closed, False)
        self.assertEqual(auto_closeable.closed, True)

    def test_j_collection_to_list(self):
        lst = [2, 1, 3]
        j_list = j_array_list(lst)
        self.assertEqual(lst, j_collection_to_list(j_list))

        s = set(lst)
        j_set = j_hashset(s)
        self.assertEqual(s, set(j_collection_to_list(j_set)))

    def assert_json_equals(self, j_object, expected) -> None:
        """Serializes a Java object with Jackson and asserts the resulting JSON matches expected."""
        actual = json.loads(_JObjectMapper().writeValueAsString(j_object))
        self.assertEqual(actual, expected)

    def assert_round_trip(self, value) -> None:
        """Asserts that value survives conversion to Java and JSON serialization unchanged."""
        self.assert_json_equals(j_json_value(value), value)

    def test_j_json_value_basic_values_pass_through(self):
        for value in [None, True, False, 0, 42, -7, 0.0, 1.5, -2.25, "", "a string"]:
            with self.subTest(value=value):
                self.assertIs(j_json_value(value), value)

    def test_j_json_value_dict(self):
        j_map = j_json_value({"key": "value", "count": 3})
        self.assertTrue(_JMap.jclass.isInstance(j_map))
        self.assertEqual(j_map.size(), 2)
        self.assertEqual(j_map.get("key"), "value")
        self.assertEqual(j_map.get("count"), 3)

    def test_j_json_value_dict_of_basic_values(self):
        self.assert_round_trip(
            {
                "none": None,
                "true": True,
                "false": False,
                "int": 42,
                "negative_int": -7,
                "float": 1.5,
                "str": "a string",
            }
        )

    def test_j_json_value_nested_dict(self):
        j_map = j_json_value({"outer": {"inner": {"deepest": "value"}}})
        j_outer = j_map.get("outer")
        self.assertTrue(_JMap.jclass.isInstance(j_outer))
        j_inner = j_outer.get("inner")
        self.assertTrue(_JMap.jclass.isInstance(j_inner))
        self.assertEqual(j_inner.get("deepest"), "value")

    def test_j_json_value_list(self):
        j_list = j_json_value(["a", "b", "c"])
        self.assertTrue(_JList.jclass.isInstance(j_list))
        self.assertEqual(j_list.size(), 3)
        self.assertEqual(j_list.get(0), "a")
        self.assertEqual(j_list.get(2), "c")

    def test_j_json_value_tuple(self):
        j_list = j_json_value(("a", "b"))
        self.assertTrue(_JList.jclass.isInstance(j_list))
        self.assert_json_equals(j_list, ["a", "b"])

    def test_j_json_value_set(self):
        j_list = j_json_value({"a", "b", "c"})
        self.assertTrue(_JList.jclass.isInstance(j_list))
        self.assertEqual(
            sorted(json.loads(_JObjectMapper().writeValueAsString(j_list))),
            ["a", "b", "c"],
        )

    def test_j_json_value_frozenset(self):
        j_list = j_json_value(frozenset(["a"]))
        self.assertTrue(_JList.jclass.isInstance(j_list))
        self.assert_json_equals(j_list, ["a"])

    def test_j_json_value_empty_containers(self):
        self.assert_round_trip({})
        self.assert_round_trip([])
        self.assert_round_trip({"empty_map": {}, "empty_list": []})

    def test_j_json_value_nested_containers(self):
        self.assert_round_trip(
            {
                "list_of_dicts": [{"a": 1}, {"b": [2, 3]}],
                "dict_of_lists": {"x": [[1, 2], []], "y": [{"z": None}]},
                "deep": {"a": {"b": {"c": {"d": ["e", {"f": True}]}}}},
            }
        )

    def test_j_json_value_list_of_lists(self):
        j_list = j_json_value([[1, 2], [3]])
        self.assertTrue(_JList.jclass.isInstance(j_list.get(0)))
        self.assert_json_equals(j_list, [[1, 2], [3]])

    def test_j_json_value_non_string_keys(self):
        # Jackson serializes non-string map keys as strings, which is the same behavior as json.dumps
        self.assert_json_equals(
            j_json_value({1: "a", 2.5: "b"}), {"1": "a", "2.5": "b"}
        )

    def test_j_json_value_unwraps_wrapped_objects(self):
        j_map = j_json_value(
            {"context": AutoCloseable(_JSharedContext.makeSharedContext())}
        )
        self.assertTrue(_JSharedContext.jclass.isInstance(j_map.get("context")))


if __name__ == "__main__":
    unittest.main()
