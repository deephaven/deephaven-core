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

    def test_char(self):
        self.all_same_json([char_(), dtypes.char])

    def test_byte(self):
        self.all_same_json([byte_(), dtypes.byte])

    def test_short(self):
        self.all_same_json([short_(), dtypes.short])

    def test_int(self):
        self.all_same_json([int_(), dtypes.int32])

    def test_long(self):
        self.all_same_json([long_(), dtypes.long, int])

    def test_float(self):
        self.all_same_json([float_(), dtypes.float32])

    def test_double(self):
        self.all_same_json([double_(), dtypes.double, float])

    def test_string(self):
        self.all_same_json([string_(), dtypes.string, str])

    def test_instant(self):
        self.all_same_json([instant_(), dtypes.Instant, datetime])

    def test_any(self):
        self.all_same_json([any_(), dtypes.JObject, object])

    def test_big_integer(self):
        self.all_same_json([big_integer_(), dtypes.BigInteger])

    def test_big_decimal(self):
        self.all_same_json([big_decimal_(), dtypes.BigDecimal])

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
