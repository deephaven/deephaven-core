#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven.remote import Session

from tests.testbase import BaseTestCase


class RemoteTestCase(BaseTestCase):
    def test_session(self):
        session = Session(host="core-server-2-1", port=10000, auth_type="Anonymous")


if __name__ == "__main__":
    unittest.main()
