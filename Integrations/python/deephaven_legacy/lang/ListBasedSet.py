
"""
Alternate set implementation favoring space over speed and not requiring the set elements to be hashable.

**The contents of this module are intended only for internal Deephaven use and may change at any time.**
"""

# See: https://docs.python.org/3/library/collections.abc.html?highlight=orderedset#collections.abc.Set

from sys import version_info
is_py2 = version_info[0] < 3

if is_py2:
    from collections import MutableSet
else:
    from collections.abc import MutableSet


class ListBasedSet(MutableSet):
    ''' Alternate set implementation favoring space over speed
        and not requiring the set elements to be hashable. '''

    def add(self, x):
        if x not in self.elements:
            self.elements.append(x)

    def discard(self, x):
        raise TypeError('Not Supported')

    def __init__(self, iterable=[]):
        """
        Creates a new set.

        :param iterable: initial set elements
        """
        self.elements = lst = []
        for value in iterable:
            if value not in lst:
                lst.append(value)

    def __iter__(self):
        return iter(self.elements)

    def __contains__(self, value):
        return value in self.elements

    def __len__(self):
        return len(self.elements)
