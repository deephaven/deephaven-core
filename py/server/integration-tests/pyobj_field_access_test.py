#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from test_helper import start_jvm
start_jvm()

from deephaven import empty_table
from deephaven.html import to_html


class EmptyCls:
    ...


pyobj = EmptyCls()
pyobj.name = "GOOG"
pyobj.price = 1000


class TestClass(unittest.TestCase):

    def test_pyobj_field_access(self):
        t = empty_table(10)
        t2 = t.update(formulas=["SYM = `AAPL-` + (String)pyobj.name", "PRICE = i * 1000"]).where("PRICE > (int)pyobj.price + 100")
        html_output = to_html(t2)
        self.assertIn("AAPL-GOOG", html_output)
        self.assertIn("2000", html_output)


if __name__ == "__main__":
    unittest.main(verbosity=2)
