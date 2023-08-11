#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
import unittest

from tests.testbase import BaseTestCase


class PluginClientTestCase(BaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        server_script = '''
from deephaven.plot.figure import Figure
from deephaven import empty_table

source = empty_table(20).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = 0.1 * i", "Y = randomDouble(0.0, 5.0)"])

plot = Figure().plot_xy(series_name="Random numbers", t=source, x="X", y="Y").show()

source2 = empty_table(20).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = 0.1 * i", "Y = randomDouble(0.0, 5.0)"])

plot2 = Figure().plot_xy(series_name="Random numbers", t=source2, x="X", y="Y", by=["Letter"]).show()
    '''
        self.session.run_script(server_script)

    def test_create(self):
        plugin_client = self.session.plugin_client(self.session.exportable_objects["plot"])
        self.assertIsNotNone(plugin_client)
        payload, refs = next(plugin_client.resp_stream)
        self.assertGreater(len(payload), 0)
        self.assertGreater(len(refs), 0)
        ref = refs[0]
        self.assertEqual(ref.type_, "Table")


if __name__ == "__main__":
    unittest.main()
