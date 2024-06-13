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
        self.assertTrue(all_equals([json(x) for x in items]))

    def all_same_json(self, items):
        self.all_same_json_internal(items)
        # The following might be seen as redundant special cases, but worthwhile for a bit of extra coverage
        self.all_same_json_internal([array_(x) for x in items] + [[x] for x in items])
        self.all_same_json_internal([object_kv_(value_type=x) for x in items])
        self.all_same_json_internal([object_({"Foo": x}) for x in items])
        self.all_same_json_internal(
            [tuple_((x,)) for x in items] + [(x,) for x in items]
        )
        self.all_same_json_internal([tuple_({"Bar": x}) for x in items])
        self.all_same_json_internal(
            [array_(array_(x)) for x in items] + [[[x]] for x in items]
        )
        self.all_same_json_internal([object_kv_(value_type=array_(x)) for x in items])

    def test_bool(self):
        self.all_same_json([bool_(), dtypes.bool_, bool])
        with self.subTest("on_missing"):
            bool_(on_missing=False)
        with self.subTest("on_null"):
            bool_(on_null=False)

    def test_char(self):
        self.all_same_json([char_(), dtypes.char])
        with self.subTest("on_missing"):
            char_(on_missing="m")
        with self.subTest("on_null"):
            char_(on_null="n")

    def test_byte(self):
        self.all_same_json([byte_(), dtypes.byte])
        with self.subTest("on_missing"):
            byte_(on_missing=-1)
        with self.subTest("on_null"):
            byte_(on_null=-1)

    def test_short(self):
        self.all_same_json([short_(), dtypes.short])
        with self.subTest("on_missing"):
            short_(on_missing=-1)
        with self.subTest("on_null"):
            short_(on_null=-1)

    def test_int(self):
        self.all_same_json([int_(), dtypes.int32])
        with self.subTest("on_missing"):
            int_(on_missing=-1)
        with self.subTest("on_null"):
            int_(on_null=-1)

    def test_long(self):
        self.all_same_json([long_(), dtypes.long, int])
        with self.subTest("on_missing"):
            long_(on_missing=-1)
        with self.subTest("on_null"):
            long_(on_null=-1)

    def test_float(self):
        self.all_same_json([float_(), dtypes.float32])
        with self.subTest("on_missing"):
            float_(on_missing=-1.0)
        with self.subTest("on_null"):
            float_(on_null=-1.0)

    def test_double(self):
        self.all_same_json([double_(), dtypes.double, float])
        with self.subTest("on_missing"):
            double_(on_missing=-1.0)
        with self.subTest("on_null"):
            double_(on_null=-1.0)

    def test_string(self):
        self.all_same_json([string_(), dtypes.string, str])
        with self.subTest("on_missing"):
            string_(on_missing="(missing)")
        with self.subTest("on_null"):
            string_(on_null="(null)")

    def test_instant(self):
        self.all_same_json([instant_(), dtypes.Instant, datetime])
        with self.subTest("on_missing"):
            instant_(on_missing=datetime.fromtimestamp(0))
        with self.subTest("on_null"):
            instant_(on_null=datetime.fromtimestamp(0))

    def test_any(self):
        self.all_same_json([any_(), dtypes.JObject, object])

    def test_big_integer(self):
        self.all_same_json([big_integer_(), dtypes.BigInteger])
        with self.subTest("on_missing"):
            big_integer_(on_missing=123456789012345678901234567890)
        with self.subTest("on_null"):
            big_integer_(on_null=123456789012345678901234567890)

    def test_big_decimal(self):
        self.all_same_json([big_decimal_(), dtypes.BigDecimal])
        with self.subTest("on_missing"):
            big_decimal_(on_missing="123456789012345678901234567890.999999999999")
        with self.subTest("on_null"):
            big_decimal_(on_null="123456789012345678901234567890.999999999999")

    def test_object(self):
        e1 = [
            {"name": str, "age": int},
            {"name": string_(), "age": long_()},
            {"name": FieldOptions(str), "age": FieldOptions(int)},
            {"name": FieldOptions(string_()), "age": FieldOptions(long_())},
        ]
        e2 = [object_(x) for x in e1]
        self.all_same_json(e1 + e2)

    def test_array(self):
        self.all_same_json(
            [
                array_(int),
                array_(long_()),
                [int],
                [long_()],
            ]
        )

    def test_tuple(self):
        e1 = [(str, int), (string_(), long_())]
        e2 = [tuple_(x) for x in e1]
        self.all_same_json(e1 + e2)

        with self.subTest("named tuple"):
            self.all_same_json(
                [
                    tuple_({"name": str, "age": int}),
                    tuple_({"name": string_(), "age": long_()}),
                ]
            )

    def test_object_kv(self):
        self.all_same_json(
            [
                object_kv_(value_type=int),
                object_kv_(value_type=long_()),
            ]
        )

    def test_skip(self):
        self.all_same_json([skip_()])


if __name__ == "__main__":
    unittest.main()
