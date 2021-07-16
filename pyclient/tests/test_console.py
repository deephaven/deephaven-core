import unittest
import warnings

from pyarrow import csv

from deephaven import Session, DHError
from tests.gen_test_data import make_random_csv


class ConsoleTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def setUp(self) -> None:
        self.session = Session()
        make_random_csv(num_cols=5, num_rows=3000, output_file='data/test_console_csv')

    def tearDown(self) -> None:
        self.session.close()

    def test_bind_table(self):
        pa_table = csv.read_csv('data/test_console_csv')
        user_table = self.session.import_table(pa_table)
        variable_name = "t"
        self.session.bind_table(user_table, variable_name)
        server_script = '''t2 = t.update("col1 = i*i")'''
        self.session.run_script(server_script)
        self.assertIn('t2', self.session.console_tables)

    def test_open_table(self):
        server_script = '''
import time
import math
from deephaven import TableTools
from numba import vectorize, int32, float64

##########################
@vectorize([float64(int32, int32)])
def vectorized_func(x, y):
    return math.sin((x % 3) + y)

table_size = 1000
start_time = time.time()
demo_table = TableTools.emptyTable(table_size) \
    .view("I=(int)i", "J=(int)(i * 2)") \
    .view("K = vectorized_func(I, J)")
        '''
        self.session.run_script(server_script)
        for t_name, t_type in self.session.console_tables.items():
            if t_type == 'Table':
                pa_table = self.session.open_table(t_name).snapshot()
                df = pa_table.to_pandas()
                self.assertEquals(1000, len(df.index))


if __name__ == '__main__':
    unittest.main()
