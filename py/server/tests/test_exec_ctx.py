#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import concurrent.futures
import threading
import unittest

from deephaven import DHError
from deephaven import empty_table
from deephaven.execution_context import get_exec_ctx, make_user_exec_ctx
from tests.testbase import BaseTestCase


class ExecCtxTestCase(BaseTestCase):
    def test_exec_ctx_threads(self):

        def assert_threads_ok(thread_func):
            threads = []
            thread_results = {}

            for i in range(4):
                threads.append(threading.Thread(target=thread_func, args=(i, thread_results)))

            for th in threads:
                th.start()
                th.join()

            self.assertTrue(thread_results)
            for i, rlt in thread_results.items():
                self.assertIn(str(i * i), rlt)

        main_exec_ctx = get_exec_ctx()

        def thread_update1(p: int, thread_results):
            with main_exec_ctx:
                t = empty_table(1)
                t2 = t.update("X = p*p")
            thread_results[p] = t2.to_string()

            with self.assertRaises(DHError):
                t3 = t.update("Y = p * p * p")

        assert_threads_ok(thread_update1)

        @main_exec_ctx
        def thread_update2(p: int, thread_results):
            t = empty_table(1)
            t2 = t.update("X = p*p")
            thread_results[p] = t2.to_string()

            t3 = t.update("Y = p * p * p")

        assert_threads_ok(thread_update2)

        def thread_update3(p: int, thread_results):
            t = empty_table(1)
            t2 = t.update("X = p*p")
            thread_results[p] = t2.to_string()

            t3 = t.update("Y = p * p * p")

        assert_threads_ok(main_exec_ctx(thread_update3))


    def test_thread_pool_executor(self):

        def assert_executor_ok(thread_func):
            thread_results = {}

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                future_table_updates = {executor.submit(thread_func, i): i for i in range(4)}
                for future in concurrent.futures.as_completed(future_table_updates.keys()):
                    i = future_table_updates[future]
                    thread_results[i] = future.result()

            self.assertTrue(thread_results)
            for i, rlt in thread_results.items():
                self.assertIn(str(i * i), rlt)

        main_exec_ctx = get_exec_ctx()

        def thread_update1(p: int):
            with main_exec_ctx:
                t = empty_table(1)
                t2 = t.update("X = p*p")
            return t2.to_string()

        assert_executor_ok(thread_update1)

        @main_exec_ctx
        def thread_update2(p: int):
            t = empty_table(1)
            t2 = t.update("X = p*p")
            return t2.to_string()

        assert_executor_ok(thread_update2)

        def thread_update3(p: int):
            t = empty_table(1)
            t2 = t.update("X = p*p")
            return t2.to_string()

        assert_executor_ok(main_exec_ctx(thread_update3))


    def test_freeze_vars(self):
        to_keep = "Foo"
        to_drop = 2

        def inner_func():
            nonlocal to_keep
            nonlocal to_drop
            user_exec_ctx = make_user_exec_ctx(freeze_vars=['to_keep'])
            with self.assertRaises(DHError):
                with user_exec_ctx:
                    t = empty_table(1).update("X = to_drop")

            with user_exec_ctx:
                t = empty_table(1).update("X = to_keep")
                self.assertIn("Foo", t.to_string())

                to_keep = "Bar"
                t1 = empty_table(1).update("X = to_keep")
                self.assertNotIn("Bar", t1.to_string())

            return 0

        with make_user_exec_ctx():
            t = empty_table(1).update("X = inner_func()")


if __name__ == '__main__':
    unittest.main()
