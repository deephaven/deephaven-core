#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven.pandas import to_pandas

from deephaven import time_table, DHError

from deephaven.execution_context import get_exec_ctx
from deephaven.liveness_scope import liveness_scope
from deephaven.update_graph import exclusive_lock
from tests.testbase import BaseTestCase


class LivenessTestCase(BaseTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.test_update_graph = get_exec_ctx().update_graph

    def create_table(self):
        with exclusive_lock(self.test_update_graph):
            return time_table("PT00:00:00.001").update(["X=i%11"]).sort("X").tail(16)

    def test_liveness(self):
        not_managed = self.create_table()
        with liveness_scope() as l_scope:
            to_discard = self.create_table()
            df = to_pandas(to_discard)
            must_keep = self.create_table()
            df = to_pandas(must_keep)
            l_scope.preserve(must_keep)

        self.assertTrue(not_managed.j_table.tryRetainReference())
        self.assertTrue(must_keep.j_table.tryRetainReference())
        self.assertFalse(to_discard.j_table.tryRetainReference())

        with liveness_scope():
            to_discard = self.create_table()
            df = to_pandas(to_discard)
            must_keep = self.create_table()
            df = to_pandas(must_keep)

        with self.assertRaises(DHError):
            l_scope = liveness_scope()
            to_discard = self.create_table()
            df = to_pandas(to_discard)
            l_scope_2 = liveness_scope()
            must_keep = self.create_table()
            df = to_pandas(must_keep)
            l_scope.preserve(must_keep)
            l_scope.close()
            l_scope_2.close()
        l_scope_2.close()
        l_scope.close()

    def test_liveness_nested(self):
        with liveness_scope() as l_scope:
            to_discard = self.create_table()
            df = to_pandas(to_discard)
            must_keep = self.create_table()
            df = to_pandas(must_keep)
            l_scope.preserve(must_keep)

            with liveness_scope() as nested_l_scope:
                nested_to_discard = self.create_table()
                df = to_pandas(nested_to_discard)
                nested_must_keep = self.create_table()
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
