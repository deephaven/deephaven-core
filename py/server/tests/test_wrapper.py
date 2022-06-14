#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

import jpy
from deephaven._wrapper import JObjectWrapper
from tests.testbase import BaseTestCase


def alpha():
    return MyObject(0, "ALPHA")

def beta():
    return MyObject(1, "BETA")

def charlie():
    return MyObject(2, "CHARLIE")

def delta():
    return MyObject(3, "DELTA")

def other():
    return Other(0, "ALPHA")


class MyObject(JObjectWrapper):
    j_object_type = jpy.get_type("io.deephaven.integrations.pyserver.wrapper.MyObject")

    def __init__(self, hash : int, s : str):
        self._j_my_object = MyObject.j_object_type(hash, s)

    @property
    def j_object(self) -> jpy.JType:
        return self._j_my_object


class Other(JObjectWrapper):
    j_object_type = jpy.get_type("io.deephaven.integrations.pyserver.wrapper.MyObject")

    def __init__(self, hash : int, s : str):
        self._j_my_object = Other.j_object_type(hash, s)

    @property
    def j_object(self) -> jpy.JType:
        return self._j_my_object


class WrapperTestCase(BaseTestCase):
    def test_repr(self):
        regex = r"^tests.test_wrapper.MyObject\(io.deephaven.integrations.pyserver.wrapper.MyObject\(objectRef=0x.+\)\)$"
        self.assertRegex(repr(alpha()), regex)
        self.assertRegex(repr(beta()), regex)
        self.assertRegex(repr(charlie()), regex)
        self.assertRegex(repr(delta()), regex)

    def test_str(self):
        self.assertEqual(str(alpha()), "ALPHA")
        self.assertEqual(str(beta()), "BETA")
        self.assertEqual(str(charlie()), "CHARLIE")
        self.assertEqual(str(delta()), "DELTA")

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
