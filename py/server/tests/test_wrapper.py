#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import unittest

import jpy

from deephaven._wrapper import JObjectWrapper
from tests.testbase import BaseTestCase


def alpha():
    return MyObject(0)


def beta():
    return MyObject(1)


def charlie():
    return MyObject(2)


def delta():
    return MyObject(3)


def other():
    return Other(0)


# BigInteger's hashCode() for 0 to 3 is the value itself, and its toString() is the value in decimal. jpy returns a
# java.lang.Integer as a Python int, so an Integer would not stay a Java object here.
class MyObject(JObjectWrapper):
    j_object_type = jpy.get_type("java.math.BigInteger")

    def __init__(self, value: int):
        self._j_my_object = MyObject.j_object_type.valueOf(value)

    @property
    def j_object(self) -> jpy.JType:
        return self._j_my_object


class Other(JObjectWrapper):
    j_object_type = jpy.get_type("java.math.BigInteger")

    def __init__(self, value: int):
        self._j_my_object = Other.j_object_type.valueOf(value)

    @property
    def j_object(self) -> jpy.JType:
        return self._j_my_object


class WrapperTestCase(BaseTestCase):
    def test_repr(self):
        regex = (
            r"^tests.test_wrapper.MyObject\(java.math.BigInteger\(objectRef=0x.+\)\)$"
        )
        self.assertRegex(repr(alpha()), regex)
        self.assertRegex(repr(beta()), regex)
        self.assertRegex(repr(charlie()), regex)
        self.assertRegex(repr(delta()), regex)

    def test_str(self):
        self.assertEqual(str(alpha()), "0")
        self.assertEqual(str(beta()), "1")
        self.assertEqual(str(charlie()), "2")
        self.assertEqual(str(delta()), "3")

    def test_hash(self):
        self.assertEqual(hash(alpha()), 0)
        self.assertEqual(hash(beta()), 1)
        self.assertEqual(hash(charlie()), 2)
        self.assertEqual(hash(delta()), 3)

    def test_eq(self):
        self.assertTrue(alpha() == alpha())
        self.assertTrue(beta() == beta())
        self.assertTrue(charlie() == charlie())
        self.assertTrue(delta() == delta())

    def test_ne(self):
        self.assertFalse(alpha() != alpha())
        self.assertTrue(alpha() != beta())
        self.assertTrue(alpha() != charlie())
        self.assertTrue(alpha() != delta())

    def test_lt(self):
        self.assertFalse(alpha() < alpha())
        self.assertTrue(alpha() < beta())
        self.assertTrue(beta() < charlie())
        self.assertTrue(charlie() < delta())

    def test_le(self):
        self.assertTrue(alpha() <= alpha())
        self.assertTrue(beta() <= beta())
        self.assertTrue(charlie() <= charlie())
        self.assertTrue(delta() <= delta())

        self.assertTrue(alpha() <= beta())
        self.assertTrue(beta() <= charlie())
        self.assertTrue(charlie() <= delta())

    def test_gt(self):
        self.assertFalse(alpha() > alpha())
        self.assertFalse(alpha() > beta())
        self.assertFalse(beta() > charlie())
        self.assertFalse(charlie() > delta())

    def test_ge(self):
        self.assertTrue(alpha() >= alpha())
        self.assertTrue(beta() >= beta())
        self.assertTrue(charlie() >= charlie())
        self.assertTrue(delta() >= delta())

        self.assertFalse(alpha() >= beta())
        self.assertFalse(beta() >= charlie())
        self.assertFalse(charlie() >= delta())

    def test_incompatible_types(self):
        self.assertFalse(alpha() == other())
        self.assertTrue(alpha() != other())
        with self.assertRaises(TypeError):
            _ = alpha() < other()
        with self.assertRaises(TypeError):
            _ = alpha() <= other()
        with self.assertRaises(TypeError):
            _ = alpha() > other()
        with self.assertRaises(TypeError):
            _ = alpha() >= other()


if __name__ == "__main__":
    unittest.main()
