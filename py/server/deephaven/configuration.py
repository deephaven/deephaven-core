#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""This module provides access to the Deephaven Configuration system, allowing users to retrieve configuration
properties as strings, integers, floats, or booleans with optional default values.
"""

from typing import Optional

import jpy

from deephaven import DHError

_JConfiguration = jpy.get_type("io.deephaven.configuration.Configuration")


class Configuration:
    """A wrapper for the Java Configuration class that provides convenient access to configuration properties.

    This class wraps the singleton Configuration.getInstance() by default, but can also wrap named or custom
    Configuration instances.
    """

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


def get_configuration() -> Configuration:
    """Get the default Configuration instance.

    Returns:
        Configuration: The default Configuration instance wrapping Configuration.getInstance().
    """
    return Configuration()
