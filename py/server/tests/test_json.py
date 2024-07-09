#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from datetime import datetime
from tests.testbase import BaseTestCase

from deephaven import dtypes
from deephaven.json import *


def all_equals(items) -> bool:
    return len(set(items)) <= 1


class JsonTestCase(BaseTestCase):
    def all_same_json_internal(self, items):
        self.assertTrue(all_equals([json_val(x) for x in items]))

    def all_same_json(self, items):
        self.all_same_json_internal(items)
        # The following might be seen as redundant special cases, but worthwhile for a bit of extra coverage
        self.all_same_json_internal(
            [array_val(x) for x in items] + [[x] for x in items]
        )
        self.all_same_json_internal([object_entries_val(x) for x in items])
        self.all_same_json_internal([object_val({"Foo": x}) for x in items])
        self.all_same_json_internal(
            [tuple_val((x,)) for x in items] + [(x,) for x in items]
        )
        self.all_same_json_internal([tuple_val({"Bar": x}) for x in items])
        self.all_same_json_internal(
            [array_val(array_val(x)) for x in items] + [[[x]] for x in items]
        )
        self.all_same_json_internal([object_entries_val(array_val(x)) for x in items])

    def test_bool(self):
        self.all_same_json([bool_val(), dtypes.bool_, bool])
        with self.subTest("on_missing"):
            bool_val(on_missing=False)
        with self.subTest("on_null"):
            bool_val(on_null=False)

    def test_char(self):
        self.all_same_json([char_val(), dtypes.char])
        with self.subTest("on_missing"):
            char_val(on_missing="m")
        with self.subTest("on_null"):
            char_val(on_null="n")

    def test_byte(self):
        self.all_same_json([byte_val(), dtypes.byte])
        with self.subTest("on_missing"):
            byte_val(on_missing=-1)
        with self.subTest("on_null"):
            byte_val(on_null=-1)

    def test_short(self):
        self.all_same_json([short_val(), dtypes.short])
        with self.subTest("on_missing"):
            short_val(on_missing=-1)
        with self.subTest("on_null"):
            short_val(on_null=-1)

    def test_int(self):
        self.all_same_json([int_val(), dtypes.int32])
        with self.subTest("on_missing"):
            int_val(on_missing=-1)
        with self.subTest("on_null"):
            int_val(on_null=-1)

    def test_long(self):
        self.all_same_json([long_val(), dtypes.long, int])
        with self.subTest("on_missing"):
            long_val(on_missing=-1)
        with self.subTest("on_null"):
            long_val(on_null=-1)

    def test_float(self):
        self.all_same_json([float_val(), dtypes.float32])
        with self.subTest("on_missing"):
            float_val(on_missing=-1.0)
        with self.subTest("on_null"):
            float_val(on_null=-1.0)

    def test_double(self):
        self.all_same_json([double_val(), dtypes.double, float])
        with self.subTest("on_missing"):
            double_val(on_missing=-1.0)
        with self.subTest("on_null"):
            double_val(on_null=-1.0)

    def test_string(self):
        self.all_same_json([string_val(), dtypes.string, str])
        with self.subTest("on_missing"):
            string_val(on_missing="(missing)")
        with self.subTest("on_null"):
            string_val(on_null="(null)")

    def test_instant(self):
        self.all_same_json([instant_val(), dtypes.Instant, datetime])
        with self.subTest("on_missing"):
            instant_val(on_missing=datetime.fromtimestamp(0))
        with self.subTest("on_null"):
            instant_val(on_null=datetime.fromtimestamp(0))

    def test_any(self):
        self.all_same_json([any_val(), dtypes.JObject, object])

    def test_big_integer(self):
        self.all_same_json([big_integer_val(), dtypes.BigInteger])
        with self.subTest("on_missing"):
            big_integer_val(on_missing=123456789012345678901234567890)
        with self.subTest("on_null"):
            big_integer_val(on_null=123456789012345678901234567890)

    def test_big_decimal(self):
        self.all_same_json([big_decimal_val(), dtypes.BigDecimal])
        with self.subTest("on_missing"):
            big_decimal_val(on_missing="123456789012345678901234567890.999999999999")
        with self.subTest("on_null"):
            big_decimal_val(on_null="123456789012345678901234567890.999999999999")

    def test_object(self):
        e1 = [
            {"name": str, "age": int},
            {"name": string_val(), "age": long_val()},
            {"name": ObjectField(str), "age": ObjectField(int)},
            {"name": ObjectField(string_val()), "age": ObjectField(long_val())},
        ]
        e2 = [object_val(x) for x in e1]
        self.all_same_json(e1 + e2)

    def test_array(self):
        self.all_same_json(
            [
                array_val(int),
                array_val(long_val()),
                [int],
                [long_val()],
            ]
        )

    def test_tuple(self):
        e1 = [(str, int), (string_val(), long_val())]
        e2 = [tuple_val(x) for x in e1]
        self.all_same_json(e1 + e2)

        with self.subTest("named tuple"):
            self.all_same_json(
                [
                    tuple_val({"name": str, "age": int}),
                    tuple_val({"name": string_val(), "age": long_val()}),
                ]
            )

    def test_object_entries(self):
        self.all_same_json(
            [
                object_entries_val(int),
                object_entries_val(long_val()),
            ]
        )

    def test_skip(self):
        self.all_same_json([skip_val()])


if __name__ == "__main__":
    unittest.main()
