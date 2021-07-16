import unittest
import warnings
import time
from deephaven import Session


class QueryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        self.session = Session()

    def tearDown(self) -> None:
        self.session.close()

    def test_tail_update_static(self):
        tbl = self.session.empty_table(10)
        query = self.session.query(tbl).update(column_specs=["Col1=i + 1"]) \
            .tail(5).update(column_specs=["Col2=i*i"])
        rlt = query.exec()
        self.assertEqual(10, rlt.size)
        time.sleep(1)
        rlt2 = query.exec()
        self.assertEqual(rlt.size, rlt2.size)

    def test_tail_update_ticking(self):
        tbl = self.session.time_table(period=100000)
        query = self.session.query(tbl).update(column_specs=["Col1=i + 1"]) \
            .tail(5).update(column_specs=["Col2=i*i"])
        rlt = query.exec()
        time.sleep(1)
        rlt2 = query.exec()
        # rlt.snapshot().to_pandas()
        self.assertLess(rlt.size, rlt2.size)


if __name__ == '__main__':
    unittest.main()
