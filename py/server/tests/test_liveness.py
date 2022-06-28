#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven.pandas import to_pandas

from deephaven import time_table

from deephaven.ugp import exclusive_lock
from deephaven.liveness_scope import liveness_scope
from tests.testbase import BaseTestCase


def create_table():
    with exclusive_lock():
        return time_table("00:00:00.001").update(["X=i%11"]).sort("X").tail(16)


class LivenessTestCase(BaseTestCase):
    def test_liveness(self):
        not_managed = create_table()
        with liveness_scope() as l_scope:
            to_discard = create_table()
            df = to_pandas(to_discard)
            must_keep = create_table()
            df = to_pandas(must_keep)
            l_scope.preserve(must_keep)

        self.assertTrue(not_managed.j_table.tryRetainReference())
        self.assertTrue(must_keep.j_table.tryRetainReference())
        self.assertFalse(to_discard.j_table.tryRetainReference())

    def test_liveness_nested(self):
        with liveness_scope() as l_scope:
            to_discard = create_table()
            df = to_pandas(to_discard)
            must_keep = create_table()
            df = to_pandas(must_keep)
            l_scope.preserve(must_keep)

            with liveness_scope() as nested_l_scope:
                nested_to_discard = create_table()
                df = to_pandas(nested_to_discard)
                nested_must_keep = create_table()
                df = to_pandas(nested_must_keep)
                nested_l_scope.preserve(nested_must_keep)
            self.assertTrue(nested_must_keep.j_table.tryRetainReference())
            # drop the extra reference obtained by the tryRetainReference() call in the above assert
            nested_must_keep.j_table.dropReference()
            self.assertFalse(nested_to_discard.j_table.tryRetainReference())

        self.assertTrue(must_keep.j_table.tryRetainReference())
        self.assertFalse(to_discard.j_table.tryRetainReference())
        self.assertFalse(nested_must_keep.j_table.tryRetainReference())
        self.assertFalse(nested_to_discard.j_table.tryRetainReference())


if __name__ == '__main__':
    unittest.main()
