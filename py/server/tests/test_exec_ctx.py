#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import concurrent.futures
import threading
import unittest

from deephaven import DHError
from deephaven import empty_table
from deephaven.execution_context import get_exec_ctx, set_exec_ctx


class ExecCtxTestCase(unittest.TestCase):
    def test_exec_ctx_threads(self):

        thread_results = {}

        main_exec_ctx = get_exec_ctx()

        def thread_update(p: int):
            t = empty_table(1)
            with main_exec_ctx:
                t2 = t.update("X = p*p")
            thread_results[p] = t2.to_string()

            with self.assertRaises(DHError):
                t3 = t.update("Y = p * p * p")

            set_exec_ctx(main_exec_ctx)
            t3 = t.update("Y = p * p * p")

        threads = []
        for i in range(4):
            threads.append(threading.Thread(target=thread_update, args=(i,)))

        for th in threads:
            th.start()
            th.join()

        self.assertTrue(thread_results)
        for i, rlt in thread_results.items():
            self.assertIn(str(i * i), rlt)

    def test_thread_pool_executor(self):
        thread_results = {}

        main_exec_ctx = get_exec_ctx()

        def thread_update(p: int):
            t = empty_table(1)
            with main_exec_ctx:
                t2 = t.update("X = p*p")
            return t2.to_string()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_table_updates = {executor.submit(thread_update, i): i for i in range(4)}
            for future in concurrent.futures.as_completed(future_table_updates.keys()):
                i = future_table_updates[future]
                thread_results[i] = future.result()

        self.assertTrue(thread_results)
        for i, rlt in thread_results.items():
            self.assertIn(str(i * i), rlt)


if __name__ == '__main__':
    unittest.main()
