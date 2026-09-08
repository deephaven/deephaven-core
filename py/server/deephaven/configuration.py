#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""This module provides access to the Deephaven Configuration system, allowing users to retrieve configuration
properties as strings, integers, floats, or booleans with optional default values.
"""

from collections.abc import ItemsView, KeysView, ValuesView
from typing import Optional

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.jcompat import j_collection_to_list

_JConfiguration = jpy.get_type("io.deephaven.configuration.Configuration")


class Configuration(JObjectWrapper):
    """A wrapper for the Java Configuration class that provides convenient access to configuration properties.

    This class wraps the singleton Configuration.getInstance() by default, but can also wrap a passed in
    Configuration instance.
    """

    j_object_type = _JConfiguration

    def __init__(self, j_configuration: Optional[jpy.JType] = None):
        """Initialize a Configuration wrapper.

        Args:
            j_configuration (Optional[jpy.JType]): The Java Configuration instance to wrap. If None, uses the
                default Configuration.getInstance().
        """
        self._j_configuration = (
            j_configuration
            if j_configuration is not None
            else _JConfiguration.getInstance()
        )

    @property
    def j_object(self) -> jpy.JType:
        """The wrapped Java Configuration instance."""
        return self._j_configuration

    def get_property(
        self, property_name: str, default: Optional[str] = None
    ) -> Optional[str]:
        """Get a configuration property as a string.

        Args:
            property_name (str): The name of the property to retrieve.
            default (Optional[str]): The default value to return if the property is not set. If None and the
                property is not set, raises DHError.

        Returns:
            Optional[str]: The property value as a string, or the default value if provided and the property is not set.

        Raises:
            DHError: If the property is not set and no default value is provided.
        """
        try:
            if default is None:
                return self._j_configuration.getProperty(property_name)
            else:
                return self._j_configuration.getStringWithDefault(
                    property_name, default
                )
        except Exception as e:
            raise DHError(message=f"Failed to get property '{property_name}'") from e

    def get_string(
        self, property_name: str, default: Optional[str] = None
    ) -> Optional[str]:
        """Get a configuration property as a string. Alias for get_property.

        Args:
            property_name (str): The name of the property to retrieve.
            default (Optional[str]): The default value to return if the property is not set. If None and the
                property is not set, raises DHError.

        Returns:
            Optional[str]: The property value as a string, or the default value if provided and the property is not set.

        Raises:
            DHError: If the property is not set and no default value is provided.
        """
        return self.get_property(property_name, default)

    def get_int(
        self, property_name: str, default: Optional[int] = None
    ) -> Optional[int]:
        """Get a configuration property as an integer.

        Args:
            property_name (str): The name of the property to retrieve.
            default (Optional[int]): The default value to return if the property is not set. If None and the
                property is not set, raises DHError.

        Returns:
            Optional[int]: The property value as an integer, or the default value if provided and the property is not set.

        Raises:
            DHError: If the property is not set and no default value is provided, or if the property value
                cannot be parsed as an integer.
        """
        try:
            if default is None:
                return self._j_configuration.getInteger(property_name)
            else:
                return self._j_configuration.getIntegerWithDefault(
                    property_name, default
                )
        except Exception as e:
            raise DHError(
                message=f"Failed to get integer property '{property_name}'"
            ) from e

    def get_float(
        self, property_name: str, default: Optional[float] = None
    ) -> Optional[float]:
        """Get a configuration property as a float (double).

        Args:
            property_name (str): The name of the property to retrieve.
            default (Optional[float]): The default value to return if the property is not set. If None and the
                property is not set, raises DHError.

        Returns:
            Optional[float]: The property value as a float, or the default value if provided and the property is not set.

        Raises:
            DHError: If the property is not set and no default value is provided, or if the property value
                cannot be parsed as a float.
        """
        try:
            if default is None:
                return self._j_configuration.getDouble(property_name)
            else:
                return self._j_configuration.getDoubleWithDefault(
                    property_name, default
                )
        except Exception as e:
            raise DHError(
                message=f"Failed to get float property '{property_name}'"
            ) from e

    def get_bool(
        self, property_name: str, default: Optional[bool] = None
    ) -> Optional[bool]:
        """Get a configuration property as a boolean.

        Args:
            property_name (str): The name of the property to retrieve.
            default (Optional[bool]): The default value to return if the property is not set. If None and the
                property is not set, raises DHError.

        Returns:
            Optional[bool]: The property value as a boolean, or the default value if provided and the property is not set.

        Raises:
            DHError: If the property is not set and no default value is provided, or if the property value
                cannot be parsed as a boolean.
        """
        try:
            if default is None:
                return self._j_configuration.getBoolean(property_name)
            else:
                return self._j_configuration.getBooleanWithDefault(
                    property_name, default
                )
        except Exception as e:
            raise DHError(
                message=f"Failed to get boolean property '{property_name}'"
            ) from e

    def get_long(
        self, property_name: str, default: Optional[int] = None
    ) -> Optional[int]:
        """Get a configuration property as a long integer.

        Args:
            property_name (str): The name of the property to retrieve.
            default (Optional[int]): The default value to return if the property is not set. If None and the
                property is not set, raises DHError.

        Returns:
            Optional[int]: The property value as a long integer, or the default value if provided and the property is not set.

        Raises:
            DHError: If the property is not set and no default value is provided, or if the property value
                cannot be parsed as a long.
        """
        try:
            if default is None:
                return self._j_configuration.getLong(property_name)
            else:
                return self._j_configuration.getLongWithDefault(property_name, default)
        except Exception as e:
            raise DHError(
                message=f"Failed to get long property '{property_name}'"
            ) from e

    def has_property(self, property_name: str) -> bool:
        """Check if a configuration property is defined.

        Args:
            property_name (str): The name of the property to check.

        Returns:
            bool: True if the property is defined, False otherwise.
        """
        try:
            return self._j_configuration.hasProperty(property_name)
        except Exception as e:
            raise DHError(message=f"Failed to check property '{property_name}'") from e

    def __getitem__(self, property_name: str) -> str:
        """Get a configuration property as a string using square bracket notation.

        This implements the mapping protocol to allow `config[property_name]` syntax.

        Args:
            property_name (str): The name of the property to retrieve.

        Returns:
            str: The property value as a string.

        Raises:
            KeyError: If the property is not set.
        """
        try:
            return self._j_configuration.getProperty(property_name)
        except Exception as e:
            raise KeyError(f"Property '{property_name}' not found") from e

    def __contains__(self, property_name: str) -> bool:
        """Check if a configuration property is defined using the 'in' operator.

        This implements the mapping protocol to allow `property_name in config` syntax.

        Args:
            property_name (str): The name of the property to check.

        Returns:
            bool: True if the property is defined, False otherwise.
        """
        return self.has_property(property_name)

    def __len__(self) -> int:
        """Return the number of configuration properties.

        This implements the mapping protocol to allow `len(config)` syntax.

        Returns:
            int: The number of properties in the configuration.
        """
        try:
            return self._j_configuration.getProperties().size()
        except Exception as e:
            raise DHError(message="Failed to get configuration size") from e

    def __iter__(self):
        """Return an iterator over the property names.

        This implements the mapping protocol to allow iteration over property names.

        Returns:
            iterator: An iterator over the property names (keys).
        """
        try:
            # Get the Properties object and convert its keySet to a Python list
            properties = self._j_configuration.getProperties()
            key_set = properties.keySet()
            # Convert Java Set to Python list
            return iter(j_collection_to_list(key_set))
        except Exception as e:
            raise DHError(message="Failed to iterate over configuration keys") from e

    def keys(self):
        """Return a dynamic view of the mapping's keys."""
        return KeysView(self)

    def values(self):
        """Return a dynamic view of the mapping's values."""
        return ValuesView(self)

    def items(self):
        """Return a dynamic view of the mapping's (key, value) pairs."""
        return ItemsView(self)


def get_configuration() -> Configuration:
    """Get the default Configuration instance.

    Returns:
        Configuration: The default Configuration instance wrapping Configuration.getInstance().
    """
    return Configuration()
