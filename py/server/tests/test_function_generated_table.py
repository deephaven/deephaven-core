from datetime import datetime
from typing import Any

import deephaven.dtypes as dht
from deephaven import empty_table, input_table, new_table, update_graph, function_generated_table
from deephaven.column import string_col, int_col
from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table
from tests.testbase import BaseTestCase


class TableTestCase(BaseTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.test_update_graph = get_exec_ctx().update_graph

    def tearDown(self):
        super().tearDown()

    def test_generated_table_timed_refresh(self):
        def table_generator_function():
            return empty_table(1).update("Timestamp = io.deephaven.base.clock.Clock.system().currentTimeMillis()")

        with update_graph.exclusive_lock(self.test_update_graph):
            result_table = function_generated_table(table_generator_function, refresh_interval_ms=2000)
            self.assertEqual(result_table.size, 1)
            first_row_key = get_row_key(0, result_table)
            initial_time = result_table.j_table.getColumnSource("Timestamp").get(first_row_key)

            if not result_table.await_update(5_000):
                raise RuntimeError("Result table did not update within 5 seconds")

            first_row_key = get_row_key(0, result_table)
            later_time = result_table.j_table.getColumnSource("Timestamp").get(first_row_key)

        # Make sure it ticked at least once within 5 seconds. It should have ticked twice,
        # but leaving a wider margin to ensure the test passes -- as long as it ticks at all
        # we can be confident it's working.
        self.assertGreater(later_time, initial_time)

    def test_generated_table_1trigger(self):
        col_defs = {
            "MyStr": dht.string
        }
        append_only_input_table = input_table(col_defs=col_defs)

        def table_generator_function():
            print("Running table_generator_function() at time: " + str(datetime.now()))
            return append_only_input_table.last_by().update('ResultStr = MyStr')

        result_table = function_generated_table(table_generator_function, source_tables=append_only_input_table)

        self.assertEqual(result_table.size, 0)

        print("Adding row at time: " + str(datetime.now()))
        append_only_input_table.add(new_table([string_col(name='MyStr', data=['test string'])]))
        print("add() returned at time: " + str(datetime.now()))
        self.wait_ticking_table_update(result_table, row_count=1, timeout=30)
        print("result_table has 1 row at time: " + str(datetime.now()))

        first_row_key = get_row_key(0, result_table)
        result_str = result_table.j_table.getColumnSource("ResultStr").get(first_row_key)
        self.assertEqual(result_str, 'test string')

    def test_generated_table_2triggers(self):
        # NOTE: This tests that both trigger tables cause the refresh function to run.
        # It does not test updating two source tables in the same cycle (that is covered by
        # io.deephaven.engine.table.impl.util.TestFunctionGeneratedTableFactory.testMultipleSources).
        append_only_input_table1 = input_table(col_defs={"MyStr": dht.string})
        append_only_input_table2 = input_table(col_defs={"MyInt": dht.int32})

        def table_generator_function():
            t1 = append_only_input_table1.last_by()
            t2 = append_only_input_table2.last_by()

            my_str = t1.j_table.getColumnSource('MyStr').get(get_row_key(0, t1))
            my_int = t2.j_table.getColumnSource('MyInt').getInt(get_row_key(0, t2))

            return new_table([
                string_col('ResultStr', [my_str]),
                int_col('ResultInt', [my_int]),
            ])

        result_table = function_generated_table(table_generator_function,
                                                source_tables=[append_only_input_table1, append_only_input_table2])

        self.assertEqual(result_table.size, 1)
        first_row_key = get_row_key(0, result_table)
        result_str = result_table.j_table.getColumnSource("ResultStr").get(first_row_key)
        result_int = result_table.j_table.getColumnSource("ResultInt").get(first_row_key)
        self.assertEqual(result_str, None)
        self.assertEqual(result_int, None)

        with update_graph.exclusive_lock(self.test_update_graph):
            append_only_input_table1.add(new_table([string_col(name='MyStr', data=['test string'])]))

            self.assertEqual(result_table.size, 1)
            first_row_key = get_row_key(0, result_table)
            result_str = result_table.j_table.getColumnSource("ResultStr").get(first_row_key)
            result_int = result_table.j_table.getColumnSource("ResultInt").get(first_row_key)
            self.assertEqual(result_str, 'test string')
            self.assertEqual(result_int, None)

            append_only_input_table2.add(new_table([int_col(name='MyInt', data=[12345])]))

            self.assertEqual(result_table.size, 1)

            first_row_key = get_row_key(0, result_table)
            result_str = result_table.j_table.getColumnSource("ResultStr").get(first_row_key)
            result_int = result_table.j_table.getColumnSource("ResultInt").get(first_row_key)
            self.assertEqual(result_str, 'test string')
            self.assertEqual(result_int, 12345)


def get_row_key(row_position: int, t: Table) -> Any:
    return t.j_table.getRowSet().get(row_position)
