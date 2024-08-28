#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest

from pydeephaven.session import Session
from pydeephaven.ticket import SharedTicket
from pydeephaven.server_object import ServerObject
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

plot3 = Figure().plot_xy(series_name="Random numbers", t=empty_table(30).update(["Letter = (i % 2 == 0) ? `A` : `B`", "X = 0.1 * i", "Y = randomDouble(0.0, 5.0)"]), x="X", y="Y").show()

    '''
        self.session.run_script(server_script)

    def test_create(self):
        plugin_client = self.session.plugin_client(self.session.exportable_objects["plot"])
        self.assertIsNotNone(plugin_client)
        payload, refs = next(plugin_client.resp_stream)
        self.assertGreater(len(payload), 0)
        self.assertGreater(len(refs), 0)
        ref = refs[0]
        self.assertEqual(ref.type, "Table")

    def test_publish_fetch(self):
        plugin_client = self.session.plugin_client(self.session.exportable_objects["plot3"])
        self.assertIsNotNone(plugin_client)

        with self.subTest("Plugin object"):
            # First fetch the Plugin object, then publish it
            export_plugin_client = self.session.fetch(plugin_client)
            shared_ticket = SharedTicket.random_ticket()
            self.session.publish(export_plugin_client, shared_ticket)

            # Another session to use the shared Plugin object
            sub_session = Session()
            server_obj = ServerObject(type="Figure", ticket=shared_ticket)
            sub_plugin_client = sub_session.plugin_client(server_obj)
            payload, refs = next(sub_plugin_client.resp_stream)
            self.assertGreater(len(payload), 0)
            self.assertGreater(len(refs), 0)
            ref = refs[0]
            self.assertEqual(ref.type, "Table")
            sub_plugin_client.close()
            sub_session.close()

        with self.subTest("Fetchable in the Plugin object"):
            payload, refs = next(plugin_client.resp_stream)
            self.assertGreater(len(payload), 0)
            self.assertGreater(len(refs), 0)
            ref = refs[0]
            self.assertEqual(ref.type, "Table")
            fetched = ref.fetch()
            self.assertIsNotNone(fetched)
            self.assertEqual(fetched.size, 30)

            # Publish the fetchable
            shared_ticket = SharedTicket.random_ticket()
            self.session.publish(ref, shared_ticket)

            # Another session to use the shared fetchable
            sub_session = Session()
            sub_table = sub_session.fetch_table(shared_ticket)
            self.assertIsNotNone(sub_table)
            self.assertEqual(sub_table.size, 30)
            sub_session.close()

        with self.subTest("released Plugin object"):
            sub_session = Session()
            server_obj = ServerObject(type="Figure", ticket=shared_ticket)
            sub_plugin_client = sub_session.plugin_client(server_obj)
            self.session.release(export_plugin_client)
            with self.assertRaises(Exception):
                payload, refs = next(sub_plugin_client.resp_stream)
            sub_session.close()

        plugin_client.close()


if __name__ == "__main__":
    unittest.main()
