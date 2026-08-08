
"""A module with classes for testing the documentation generator."""

from typing import TypeVar, Generic

T = TypeVar('T')
""" A type variable for use in generic classes. """

class GenericClass(Generic[T]):
    """A generic class that takes a type parameter."""
    def __init__(self, value: T):
        self.value = value

    def get_value(self) -> T:
        """Return the value stored in the class."""
        return self.value

    def to_str(self, s: str) -> str:
        """Return a string representation of the value stored in the class."""

        return str(self.value) + s

class ChildClass(GenericClass[str]):
    """A child class of GenericClass that uses a string type parameter."""

    def print_value(self):
        """Print the value stored in the class."""
        print(self.get_value())

class Table(GenericClass[str]):
    """A child class of GenericClass that uses a string type parameter."""

    def print_value(self):
        """Print the value stored in the class."""
        print(self.get_value())