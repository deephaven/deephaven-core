#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven import read_csv, empty_table
from deephaven.agg import sum_, avg, count_, count_where, first, last, max_, min_, std, abs_sum, \
    var
from deephaven.filters import Filter
from deephaven.table import NodeType
from tests.testbase import BaseTestCase


class RollupAndTreeTableTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")
        self.aggs_for_rollup = [
            avg(["aggAvg=var"]),
            count_("aggCount"),
            count_where("aggCountWhere", "var > 0"),
            first(["aggFirst=var"]),
            last(["aggLast=var"]),
            max_(["aggMax=var"]),
            min_(["aggMin=var"]),
            std(["aggStd=var"]),
            sum_(["aggSum=var"]),
            abs_sum(["aggAbsSum=var"]),
            var(["aggVar=var"]),
        ]

    def test_rollup_table(self):
        test_table = empty_table(100)
        test_table = test_table.update(
            ["grp_id=(int)(i/5)", "var=(int)i", "weights=(double)1.0/(i+1)"]
        )

        with self.subTest("with node operations"):
            rollup_table = test_table.rollup(aggs=self.aggs_for_rollup, by='grp_id')
            node_op = rollup_table.node_operation_recorder(NodeType.AGGREGATED).sort("aggVar").sort_descending(
                "aggMax").format_column("aggSum=`aliceblue`")
            rt = rollup_table.with_node_operations([node_op])
            self.assertIsNotNone(rt)

            rollup_table = test_table.rollup(aggs=self.aggs_for_rollup, by='grp_id', include_constituents=True)
            node_op_1 = rollup_table.node_operation_recorder(NodeType.CONSTITUENT).sort("weights").sort_descending(
                "var").format_column("grp_id=`aliceblue`")
            rt = rollup_table.with_node_operations([node_op, node_op_1])
            self.assertIsNotNone(rt)

        with self.subTest("with filter"):
            conditions = ["grp_id > 100", "grp_id > -100"]
            filters = Filter.from_(conditions)
            rt = rollup_table.with_filters(filters=conditions)
            rt1 = rollup_table.with_filters(filters=filters)
            self.assertTrue(all([rt, rt1]))

    def test_tree_table(self):
        tree_table = self.test_table.tail(10).tree(id_col='a', parent_col='c')
        with self.subTest("with node operations"):
            node_op = tree_table.node_operation_recorder().sort("b").where("e > 0").format_column("d=`aliceblue`")
            rt = tree_table.with_node_operations(node_op)
            self.assertIsNotNone(rt)

        with self.subTest("with filter"):
            conditions = ["a > 100", "b < 1000", "c < 0"]
            filters = Filter.from_(conditions)
            rt = tree_table.with_filters(filters=conditions)
            rt1 = tree_table.with_filters(filters=filters)
            self.assertTrue(all([rt, rt1]))


if __name__ == "__main__":
    unittest.main()
