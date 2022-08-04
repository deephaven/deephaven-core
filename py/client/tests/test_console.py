#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from pyarrow import csv

from pydeephaven import DHError
from tests.testbase import BaseTestCase


class ConsoleTestCase(BaseTestCase):

    def test_bind_table(self):
        pa_table = csv.read_csv(self.csv_file)
        user_table = self.session.import_table(pa_table)
        variable_name = "t"
        self.session.bind_table(variable_name, user_table)
        server_script = '''t2 = t.update("col1 = i*i")'''
        self.session.run_script(server_script)
        self.assertIn('t2', self.session.tables)

        # verify that Python variable 't' refers to an instance of the wrapper class: deephaven.table.Table
        server_script = '''t3 = t.meta_table'''
        self.session.run_script(server_script)
        self.assertIn('t3', self.session.tables)

    def test_run_script_and_open_table(self):
        server_script = '''
import time
import math
from deephaven import empty_table
from numba import vectorize, int32, float64

##########################
@vectorize([float64(int32, int32)])
def vectorized_func(x, y):
    return math.sin((x % 3) + y)

table_size = 1000
start_time = time.time()
demo_table = (empty_table(table_size) 
    .view(["I=(int)i", "J=(int)(i * 2)"]) 
    .view("K = vectorized_func(I, J)"))
        '''
        self.session.run_script(server_script)
        self.assertIn('demo_table', self.session.tables)
        pa_table = self.session.open_table('demo_table').snapshot()
        df = pa_table.to_pandas()
        self.assertEquals(1000, len(df.index))
    
    def test_open_invalid_table(self):
        with self.assertRaises(DHError):
            self.session.open_table('thistablereallyreallydoesnotexist')
