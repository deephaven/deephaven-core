#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest
from deephaven import TableTools
import bootstrap


class EmptyCls:
    ...


pyobj = EmptyCls()
pyobj.name = "GOOG"
pyobj.price = 1000


class TestClass(unittest.TestCase):

    def test_pyobj_field_access(self):
        t = TableTools.emptyTable(10)
        t2 = t.update("SYM = `AAPL-` + (String)pyobj.name", "PRICE = i * 1000").where("PRICE > (int)pyobj.price + 100")
        html_output = TableTools.html(t2)
        self.assertIn("AAPL-GOOG", html_output)
        self.assertIn("2000", html_output)


if __name__ == "__main__":
    bootstrap.build_py_session()

    unittest.main(verbosity=2)
