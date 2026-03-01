#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import DHError
from deephaven.configuration import Configuration, get_configuration
from tests.testbase import BaseTestCase


class ConfigurationTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.config = get_configuration()

    def test_get_configuration(self):
        """Test that we can get the default configuration instance."""
        config = get_configuration()
        self.assertIsNotNone(config)
        self.assertIsInstance(config, Configuration)

    def test_j_object(self):
        """Test that the j_object property returns a Java Configuration instance."""
        j_config = self.config.j_object
        self.assertIsNotNone(j_config)

    def test_has_property(self):
        """Test the has_property method."""
        # Test with a property that should exist in dh-defaults.prop
        self.assertTrue(self.config.has_property("QueryCompiler.logEnabledDefault"))
        self.assertFalse(self.config.has_property("test.property.does.not.exist"))

    def test_get_string_with_default(self):
        """Test getting a string property with a default value."""
        # Test with a property that exists in dh-defaults.prop
        value = self.config.get_string(
            "UpdatePerformanceTracker.reportingMode", "default"
        )
        self.assertEqual(value, "LISTENER_ONLY")

        # Test with non-existent property
        value = self.config.get_string("test.nonexistent.property", "default")
        self.assertEqual(value, "default")

    def test_get_string_without_default(self):
        """Test getting a string property without a default value."""
        # Test with a property that exists in dh-defaults.prop
        value = self.config.get_string("Calendar.default")
        self.assertEqual(value, "USNYSE_EXAMPLE")

        # Test with non-existent property - should raise exception
        with self.assertRaises(DHError):
            self.config.get_string("test.nonexistent.property2")

    def test_get_int_with_default(self):
        """Test getting an integer property with a default value."""
        # Test with a property that exists in dh-defaults.prop
        value = self.config.get_int("NIO.driver.initialThreadCount", 0)
        self.assertEqual(value, 4)

        # Test with non-existent property
        value = self.config.get_int("test.nonexistent.int.property", 100)
        self.assertEqual(value, 100)

    def test_get_int_without_default(self):
        """Test getting an integer property without a default value."""
        # Test with a property that exists in dh-defaults.prop
        value = self.config.get_int("NIO.driver.maxThreadCount")
        self.assertEqual(value, 16)

        # Test with non-existent property - should raise exception
        with self.assertRaises(DHError):
            self.config.get_int("test.nonexistent.int.property2")

    def test_get_float_with_default(self):
        """Test getting a float property with a default value."""
        # Test with a property that exists in dh-defaults.prop
        value = self.config.get_float(
            "UpdatePerformanceTracker.reportIntervalMillis", 0.0
        )
        self.assertAlmostEqual(value, 60000.0, places=1)

        # Test with non-existent property
        value = self.config.get_float("test.nonexistent.float.property", 2.71828)
        self.assertAlmostEqual(value, 2.71828, places=5)

    def test_get_float_without_default(self):
        """Test getting a float property without a default value."""
        # Test with a property that exists in dh-defaults.prop
        value = self.config.get_float("NIO.driver.workTimeout")
        self.assertAlmostEqual(value, 100.0, places=1)

        # Test with non-existent property - should raise exception
        with self.assertRaises(DHError):
            self.config.get_float("test.nonexistent.float.property2")

    def test_get_bool_with_default(self):
        """Test getting a boolean property with a default value."""
        # Test with a property that exists in dh-defaults.prop
        value = self.config.get_bool("QueryCompiler.logEnabledDefault", True)
        self.assertFalse(value)

        value = self.config.get_bool("statsdriver.enabled", False)
        self.assertTrue(value)

        # Test with non-existent property
        value = self.config.get_bool("test.nonexistent.bool.property", True)
        self.assertTrue(value)

    def test_get_bool_without_default(self):
        """Test getting a boolean property without a default value."""
        # Test with a property that exists in dh-defaults.prop
        value = self.config.get_bool("allocation.stats.enabled")
        self.assertFalse(value)

        # Test with non-existent property - should raise exception
        with self.assertRaises(DHError):
            self.config.get_bool("test.nonexistent.bool.property2")

    def test_get_long_with_default(self):
        """Test getting a long property with a default value."""
        # Test with a property that exists in dh-defaults.prop
        value = self.config.get_long("http.session.durationMs", 0)
        self.assertEqual(value, 300000)

        # Test with non-existent property
        value = self.config.get_long("test.nonexistent.long.property", 12345)
        self.assertEqual(value, 12345)

    def test_get_long_without_default(self):
        """Test getting a long property without a default value."""
        # Test with a property that exists in dh-defaults.prop
        value = self.config.get_long("UpdatePerformanceTracker.reportIntervalMillis")
        self.assertEqual(value, 60000)

        # Test with non-existent property - should raise exception
        with self.assertRaises(DHError):
            self.config.get_long("test.nonexistent.long.property2")

    def test_get_property_alias(self):
        """Test that get_property is an alias for get_string."""
        # Use a property that exists in dh-defaults.prop
        value1 = self.config.get_property("Calendar.default", "default")
        value2 = self.config.get_string("Calendar.default", "default")
        self.assertEqual(value1, value2)
        self.assertEqual(value1, "USNYSE_EXAMPLE")

    def test_mapping_protocol_getitem(self):
        """Test using square bracket notation to get properties."""
        # Test with a property that exists in dh-defaults.prop
        value = self.config["Calendar.default"]
        self.assertEqual(value, "USNYSE_EXAMPLE")

        value = self.config["UpdatePerformanceTracker.reportingMode"]
        self.assertEqual(value, "LISTENER_ONLY")

        # Test with non-existent property - should raise KeyError
        with self.assertRaises(KeyError):
            _ = self.config["test.nonexistent.property"]

    def test_mapping_protocol_contains(self):
        """Test using 'in' operator to check if properties exist."""
        # Test with properties that exist in dh-defaults.prop
        self.assertTrue("Calendar.default" in self.config)
        self.assertTrue("QueryCompiler.logEnabledDefault" in self.config)
        self.assertTrue("NIO.driver.initialThreadCount" in self.config)

        # Test with non-existent property
        self.assertFalse("test.nonexistent.property" in self.config)
        self.assertFalse("nonexistent.key" in self.config)

    def test_mapping_protocol_len(self):
        """Test using len() to get the number of properties."""
        # Get the length
        length = len(self.config)

        # Should be a positive integer (there are many properties in dh-defaults.prop)
        self.assertIsInstance(length, int)
        self.assertGreater(length, 0)

        # Verify it's consistent
        self.assertEqual(len(list(self.config)), length)

    def test_mapping_protocol_iter(self):
        """Test iteration over configuration property names."""
        # Get all keys by iteration
        keys = list(self.config)

        # Should have at least some properties
        self.assertGreater(len(keys), 0)

        # All keys should be strings
        for key in keys:
            self.assertIsInstance(key, str)

        # Some known properties should be in the list
        self.assertIn("Calendar.default", keys)
        self.assertIn("QueryCompiler.logEnabledDefault", keys)

        # Can iterate multiple times
        keys2 = list(self.config)
        self.assertEqual(keys, keys2)

        # Can use in for loop
        count = 0
        for key in self.config:
            self.assertIsInstance(key, str)
            count += 1
        self.assertEqual(count, len(self.config))

    def test_mapping_protocol_keys_values_items(self):
        """Test that we can use dict-like patterns with the configuration."""
        # Can access values via iteration
        for key in self.config:
            # Should be able to access the value
            value = self.config[key]
            self.assertIsInstance(value, str)

        # Can create a dict from the config
        config_dict = {key: self.config[key] for key in self.config}
        self.assertIsInstance(config_dict, dict)
        self.assertEqual(len(config_dict), len(self.config))

        # Verify some known properties are in the dict
        self.assertIn("Calendar.default", config_dict)
        self.assertEqual(config_dict["Calendar.default"], "USNYSE_EXAMPLE")


if __name__ == "__main__":
    unittest.main()
