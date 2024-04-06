#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import time_table, DHError, merge
from deephaven import update_graph as ug
from deephaven.execution_context import make_user_exec_ctx, get_exec_ctx
from deephaven.table import Table
from tests.testbase import BaseTestCase


def transform_func(t: Table) -> Table:
    return t.update("f = X + Z")


def partitioned_transform_func(t: Table, ot: Table) -> Table:
    return t.natural_join(ot, on=["X", "Z"], joins=["f"])


class UpdateGraphTestCase(BaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        ug.auto_locking = False
        self.test_update_graph = get_exec_ctx().update_graph

    def tearDown(self):
        ug.auto_locking = True
        super().tearDown()

    def test_ug_context_manager(self):
        with self.assertRaises(DHError) as cm:
            test_table = time_table("PT00:00:00.001").update(["X=i%11"]).sort("X").tail(16)
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        with ug.exclusive_lock(self.test_update_graph):
            test_table = time_table("PT00:00:00.001").update(["TS=now()"])

        with ug.shared_lock(self.test_update_graph):
            test_table = time_table("PT00:00:00.001").update(["TS=now()"])

        # nested locking
        with ug.exclusive_lock(self.test_update_graph):
            with ug.shared_lock(self.test_update_graph):
                test_table = time_table("PT00:00:00.001").update(["TS=now()"])
            test_table = time_table("PT00:00:00.001").update(["TS=now()"])

        with self.assertRaises(DHError) as cm:
            with ug.shared_lock(self.test_update_graph):
                with ug.exclusive_lock(self.test_update_graph):
                    test_table = time_table("PT00:00:00.001").update(["TS=now()"])
        self.assertRegex(str(cm.exception), "Cannot upgrade a shared lock to an exclusive lock")

    def test_ug_decorator_exclusive(self):
        def ticking_table_op(tail_size: int, period: str = "PT00:00:01"):
            return time_table(period).update(["X=i%11"]).sort("X").tail(tail_size)

        with self.assertRaises(DHError) as cm:
            t = ticking_table_op(16)
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        @ug.exclusive_locked(self.test_update_graph)
        def ticking_table_op_decorated(tail_size: int, period: str = "PT00:00:01"):
            t = time_table(period).update(["X=i%11"]).sort("X").tail(tail_size)
            self.assertEqual(t.size, 0)
            return t

        with self.assertRaises(DHError):
            t = ticking_table_op_decorated()

        t = ticking_table_op_decorated(5)
        self.wait_ticking_table_update(t, row_count=5, timeout=10)

        t = ticking_table_op_decorated(10, "PT00:00:00.001")
        self.wait_ticking_table_update(t, row_count=8, timeout=5)

    def test_ug_decorator_shared(self):
        @ug.shared_locked(self.test_update_graph)
        def ticking_table_op_decorated(tail_size: int, period: str = "PT00:00:01"):
            t = time_table(period).update(["X=i%11"]).sort("X").tail(tail_size)
            self.assertEqual(t.size, 0)
            return t

        with self.assertRaises(DHError):
            t = ticking_table_op_decorated()

        t = ticking_table_op_decorated(5)
        self.wait_ticking_table_update(t, row_count=5, timeout=10)

        t = ticking_table_op_decorated(10, "PT00:00:00.001")
        self.wait_ticking_table_update(t, row_count=8, timeout=5)

    def test_auto_locking_release(self):
        ug.auto_locking = True
        test_table = time_table("PT00:00:00.001").update(["X=i%11"]).sort("X").tail(16)
        self.assertFalse(ug.has_shared_lock(self.test_update_graph))
        self.assertFalse(ug.has_exclusive_lock(self.test_update_graph))

    def test_auto_locking_update_select(self):
        test_table = time_table("PT00:00:00.001")
        ops = [
            Table.update,
            Table.lazy_update,
            Table.select,
        ]

        # auto_locking off
        for op in ops:
            with self.subTest(op=op):
                with self.assertRaises(DHError) as cm:
                    result_table = op(self=test_table, formulas="X = i % 11")
                self.assertRegex(str(cm.exception), r"IllegalStateException")

        # auto_locking on
        ug.auto_locking = True
        for op in ops:
            with self.subTest(op=op):
                result_table = op(test_table, "X = i % 11")

    def test_auto_locking_wherein(self):
        with ug.shared_lock(self.test_update_graph):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])
        unique_table = test_table.head(num_rows=50).select_distinct(formulas=["X", "Y"])

        with self.assertRaises(DHError) as cm:
            result_table = test_table.where_in(unique_table, cols=["Y"])
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        with self.assertRaises(DHError) as cm:
            result_table = test_table.where_not_in(unique_table, cols=["Y"])
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        ug.auto_locking = True
        result_table = test_table.where_in(unique_table, cols=["Y"])
        result_table = test_table.where_not_in(unique_table, cols=["Y"])

    def test_auto_locking_joins(self):
        with ug.shared_lock(self.test_update_graph):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])

        left_table = test_table.drop_columns(["Z", "Timestamp"])
        right_table = test_table.drop_columns(["Y", "Timestamp"])
        ops = [
            Table.natural_join,
            Table.exact_join,
            Table.join,
            Table.aj,
            Table.raj
        ]

        for op in ops:
            with self.subTest(op=op):
                self.skipTest("Waiting for #2596")
                with self.assertRaises(DHError) as cm:
                    result_table = left_table.aj(right_table, on="X")
                self.assertRegex(str(cm.exception), r"IllegalStateException")

        ug.auto_locking = True
        for op in ops:
            with self.subTest(op=op):
                result_table = left_table.aj(right_table, on="X")

    def test_auto_locking_ungroup(self):
        with ug.shared_lock(self.test_update_graph):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=i%13"])
        grouped_table = test_table.group_by(by=["Y"])
        with self.assertRaises(DHError) as cm:
            ungrouped_table = grouped_table.ungroup(cols=["X"])
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        ug.auto_locking = True
        ungrouped_table = grouped_table.ungroup(cols=["X"])

    def test_auto_locking_head_tail_by(self):
        ops = [Table.head_by, Table.tail_by]
        with ug.shared_lock(self.test_update_graph):
            test_table = time_table("PT00:00:00.001").update(["X=i%11"]).sort("X").tail(16)

        for op in ops:
            with self.subTest(op=op):
                with self.assertRaises(DHError) as cm:
                    result_table = op(test_table, num_rows=1, by=["X"])
                    self.assertLessEqual(result_table.size, test_table.size)
                self.assertRegex(str(cm.exception), r"IllegalStateException")

        ug.auto_locking = True
        for op in ops:
            with self.subTest(op=op):
                result_table = op(test_table, num_rows=1, by=["X"])
                self.assertLessEqual(result_table.size, test_table.size)

    def test_auto_locking_partitioned_table(self):
        with ug.shared_lock(self.test_update_graph):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])
        pt = test_table.partition_by(by="Y")

        with self.subTest("Merge"):
            ug.auto_locking = False
            with self.assertRaises(DHError) as cm:
                t = pt.merge()
            self.assertRegex(str(cm.exception), r"IllegalStateException")

            ug.auto_locking = True
            t = pt.merge()

        with self.subTest("Transform"):
            ug.auto_locking = False
            with make_user_exec_ctx(), self.assertRaises(DHError) as cm:
                pt1 = pt.transform(transform_func)
            self.assertRegex(str(cm.exception), r"IllegalStateException")

            ug.auto_locking = True
            with make_user_exec_ctx():
                pt1 = pt.transform(transform_func)

        with self.subTest("Partitioned Transform"):
            ug.auto_locking = False
            with make_user_exec_ctx(), self.assertRaises(DHError) as cm:
                pt2 = pt.partitioned_transform(pt1, partitioned_transform_func)
            self.assertRegex(str(cm.exception), r"IllegalStateException")

            ug.auto_locking = True
            with make_user_exec_ctx():
                pt2 = pt.partitioned_transform(pt1, partitioned_transform_func)

    def test_auto_locking_table_factory(self):
        with ug.shared_lock(self.test_update_graph):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])
            test_table1 = time_table("PT00:00:00.001").update(["X=i", "Y=i%23", "Z=X*Y"])

        with self.subTest("Merge"):
            ug.auto_locking = False
            with self.assertRaises(DHError) as cm:
                t = merge([test_table, test_table1])
            self.assertRegex(str(cm.exception), r"IllegalStateException")

            ug.auto_locking = True
            t = merge([test_table, test_table1])

        with self.subTest("Merge Sorted"):
            self.skipTest("mergeSorted does not yet support refreshing tables")

    def test_auto_locking_partitioned_table_proxy(self):
        with ug.shared_lock(self.test_update_graph):
            test_table = time_table("PT00:00:01").update(["X=i", "Y=i%13", "Z=X*Y"])
        proxy = test_table.drop_columns(["Timestamp", "Y"]).partition_by(by="X").proxy()
        proxy2 = test_table.drop_columns(["Timestamp", "Z"]).partition_by(by="X").proxy()

        with self.assertRaises(DHError) as cm:
            joined_pt_proxy = proxy.natural_join(proxy2, on="X")
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        ug.auto_locking = True
        joined_pt_proxy = proxy.natural_join(proxy2, on="X")
        del joined_pt_proxy


if __name__ == "__main__":
    unittest.main()
