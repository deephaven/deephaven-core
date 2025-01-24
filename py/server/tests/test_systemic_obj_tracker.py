#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest
from unittest.mock import patch
from deephaven import DHError, empty_table
import deephaven.systemic_obj_tracker as tracker


class TestSystemicObjectTracker(unittest.TestCase):

    def test_is_systemic_object_marking_enabled(self):
        self.assertTrue(tracker.is_systemic_object_marking_enabled())

    def test_is_systemic(self):
        # user thread is not systemic by default
        self.assertFalse(tracker.is_systemic())

    def test_set_systemic(self):
        tracker.set_systemic(True)
        self.assertTrue(tracker.is_systemic())
        t = empty_table(10)
        self.assertTrue(t.j_object.isSystemicObject())

        tracker.set_systemic(False)
        self.assertFalse(tracker.is_systemic())
        t = empty_table(10)
        self.assertFalse(t.j_object.isSystemicObject())

        with patch("deephaven.systemic_obj_tracker._JSystemicObjectTracker") as mock_tracker:
            mock_tracker.isSystemicObjectMarkingEnabled.return_value = False
            with self.assertRaises(DHError) as cm:
                tracker.set_systemic(True)
            self.assertIn("Systemic object marking is not enabled", str(cm.exception))

    def test_systemic_object_marking(self):
        with tracker.systemic_object_marking():
            self.assertTrue(tracker.is_systemic())
            t = empty_table(10)
        self.assertTrue(t.j_object.isSystemicObject())

    def test_systemic_object_marking_error(self):
        with patch("deephaven.systemic_obj_tracker._JSystemicObjectTracker") as mock_tracker:
            mock_tracker.isSystemicObjectMarkingEnabled.return_value = False
            with self.assertRaises(DHError) as cm:
                with tracker.systemic_object_marking():
                    pass
            self.assertIn("Systemic object marking is not enabled", str(cm.exception))

    def test_no_systemic_object_marking(self):
        with tracker.no_systemic_object_marking():
            self.assertFalse(tracker.is_systemic())
            t = empty_table(10)
        self.assertFalse(t.j_object.isSystemicObject())


if __name__ == '__main__':
    unittest.main()
