#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import jpy
import unittest

from deephaven import time_table, DHError, merge, merge_sorted
from deephaven import ugp
from deephaven.table import Table
from tests.testbase import BaseTestCase


def transform_func(t: Table) -> Table:
    return t.update("f = X + Z")


def partitioned_transform_func(t: Table, ot: Table) -> Table:
    return t.natural_join(ot, on=["X", "Z"], joins=["f"])


class UgpTestCase(BaseTestCase):
    def setUp(self) -> None:
        ugp.auto_locking = False

    def tearDown(self):
        ugp.auto_locking = False

    def test_ugp_context_manager(self):
        with self.assertRaises(DHError) as cm:
            test_table = time_table("00:00:00.001").update(["X=i%11"]).sort("X").tail(16)
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        with ugp.exclusive_lock():
            test_table = time_table("00:00:00.001").update(["TS=currentTime()"])

        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["TS=currentTime()"])

        # nested locking
        with ugp.exclusive_lock():
            with ugp.shared_lock():
                test_table = time_table("00:00:00.001").update(["TS=currentTime()"])
            test_table = time_table("00:00:00.001").update(["TS=currentTime()"])

        with self.assertRaises(DHError) as cm:
            with ugp.shared_lock():
                with ugp.exclusive_lock():
                    test_table = time_table("00:00:00.001").update(["TS=currentTime()"])
        self.assertRegex(str(cm.exception), "Cannot upgrade a shared lock to an exclusive lock")

    def test_ugp_decorator_exclusive(self):
        def ticking_table_op(tail_size: int, period: str = "00:00:01"):
            return time_table(period).update(["X=i%11"]).sort("X").tail(tail_size)

        with self.assertRaises(DHError) as cm:
            t = ticking_table_op(16)
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        @ugp.exclusive_locked
        def ticking_table_op_decorated(tail_size: int, period: str = "00:00:01"):
            t = time_table(period).update(["X=i%11"]).sort("X").tail(tail_size)
            self.assertEqual(t.size, 0)
            return t

        with self.assertRaises(DHError):
            t = ticking_table_op_decorated()

        t = ticking_table_op_decorated(5)
        self.wait_ticking_table_update(t, row_count=5, timeout=10)

        t = ticking_table_op_decorated(10, "00:00:00.001")
        self.wait_ticking_table_update(t, row_count=8, timeout=5)

    def test_ugp_decorator_shared(self):
        @ugp.shared_locked
        def ticking_table_op_decorated(tail_size: int, period: str = "00:00:01"):
            t = time_table(period).update(["X=i%11"]).sort("X").tail(tail_size)
            self.assertEqual(t.size, 0)
            return t

        with self.assertRaises(DHError):
            t = ticking_table_op_decorated()

        t = ticking_table_op_decorated(5)
        self.wait_ticking_table_update(t, row_count=5, timeout=10)

        t = ticking_table_op_decorated(10, "00:00:00.001")
        self.wait_ticking_table_update(t, row_count=8, timeout=5)

    def test_auto_locking_release(self):
        ugp.auto_locking = True
        test_table = time_table("00:00:00.001").update(["X=i%11"]).sort("X").tail(16)
        self.assertFalse(ugp.has_shared_lock())
        self.assertFalse(ugp.has_exclusive_lock())

    def test_auto_locking_update_select(self):
        test_table = time_table("00:00:00.001")
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
        ugp.auto_locking = True
        for op in ops:
            with self.subTest(op=op):
                result_table = op(test_table, "X = i % 11")

    def test_auto_locking_wherein(self):
        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])
        unique_table = test_table.head(num_rows=50).select_distinct(formulas=["X", "Y"])

        with self.assertRaises(DHError) as cm:
            result_table = test_table.where_in(unique_table, cols=["Y"])
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        with self.assertRaises(DHError) as cm:
            result_table = test_table.where_not_in(unique_table, cols=["Y"])
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        ugp.auto_locking = True
        result_table = test_table.where_in(unique_table, cols=["Y"])
        result_table = test_table.where_not_in(unique_table, cols=["Y"])

    def test_auto_locking_joins(self):
        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])

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

        ugp.auto_locking = True
        for op in ops:
            with self.subTest(op=op):
                result_table = left_table.aj(right_table, on="X")

    def test_auto_locking_rename_columns(self):
        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])

        cols_to_rename = [
            f"{f.name + '_2'} = {f.name}" for f in test_table.columns[::2]
        ]

        with self.assertRaises(DHError) as cm:
            result_table = test_table.rename_columns(cols_to_rename)
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        ugp.auto_locking = True
        result_table = test_table.rename_columns(cols_to_rename)

    def test_auto_locking_ungroup(self):
        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["X=i", "Y=i%13"])
        grouped_table = test_table.group_by(by=["Y"])
        with self.assertRaises(DHError) as cm:
            ungrouped_table = grouped_table.ungroup(cols=["X"])
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        ugp.auto_locking = True
        ungrouped_table = grouped_table.ungroup(cols=["X"])

    def test_auto_locking_head_tail_by(self):
        ops = [Table.head_by, Table.tail_by]
        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["X=i%11"]).sort("X").tail(16)

        for op in ops:
            with self.subTest(op=op):
                with self.assertRaises(DHError) as cm:
                    result_table = op(test_table, num_rows=1, by=["X"])
                    self.assertLessEqual(result_table.size, test_table.size)
                self.assertRegex(str(cm.exception), r"IllegalStateException")

        ugp.auto_locking = True
        for op in ops:
            with self.subTest(op=op):
                result_table = op(test_table, num_rows=1, by=["X"])
                self.assertLessEqual(result_table.size, test_table.size)

    def test_auto_locking_partitioned_table(self):
        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])
        pt = test_table.partition_by(by="Y")

        _ExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
        _context = _ExecutionContext.newBuilder() \
                .captureQueryCompiler()           \
                .captureQueryLibrary()            \
                .emptyQueryScope()                \
                .build()                          \
                .open()

        with self.subTest("Merge"):
            ugp.auto_locking = False
            with self.assertRaises(DHError) as cm:
                t = pt.merge()
            self.assertRegex(str(cm.exception), r"IllegalStateException")

            ugp.auto_locking = True
            t = pt.merge()

        with self.subTest("Transform"):
            ugp.auto_locking = False
            with self.assertRaises(DHError) as cm:
                pt1 = pt.transform(transform_func)
            self.assertRegex(str(cm.exception), r"IllegalStateException")

            ugp.auto_locking = True
            pt1 = pt.transform(transform_func)

        with self.subTest("Partitioned Transform"):
            ugp.auto_locking = False
            with self.assertRaises(DHError) as cm:
                pt2 = pt.partitioned_transform(pt1, partitioned_transform_func)
            self.assertRegex(str(cm.exception), r"IllegalStateException")

            ugp.auto_locking = True
            pt2 = pt.partitioned_transform(pt1, partitioned_transform_func)

        _context.close()

    def test_auto_locking_table_factory(self):
        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])
            test_table1 = time_table("00:00:00.001").update(["X=i", "Y=i%23", "Z=X*Y"])

        with self.subTest("Merge"):
            ugp.auto_locking = False
            with self.assertRaises(DHError) as cm:
                t = merge([test_table, test_table1])
            self.assertRegex(str(cm.exception), r"IllegalStateException")

            ugp.auto_locking = True
            t = merge([test_table, test_table1])

        with self.subTest("Merge Sorted"):
            self.skipTest("mergeSorted does not yet support refreshing tables")

    def test_auto_locking_partitioned_table_proxy(self):
        with ugp.shared_lock():
            test_table = time_table("00:00:01").update(["X=i", "Y=i%13", "Z=X*Y"])
        proxy = test_table.drop_columns(["Timestamp", "Y"]).partition_by(by="X").proxy()
        proxy2 = test_table.drop_columns(["Timestamp", "Z"]).partition_by(by="X").proxy()

        with self.assertRaises(DHError) as cm:
            joined_pt_proxy = proxy.natural_join(proxy2, on="X")
        self.assertRegex(str(cm.exception), r"IllegalStateException")

        ugp.auto_locking = True
        joined_pt_proxy = proxy.natural_join(proxy2, on="X")
        del joined_pt_proxy


if __name__ == "__main__":
    unittest.main()
