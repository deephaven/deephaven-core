# C4 Formulas in Python

Formulas are used to filter tables or to assign data to columns. The following language features can be used to construct a formula:

- operators:  `+`, `-`, `*`, `/`, `%`, `_`, `.`, `[]`, `()`
- functions 
- objects
- columns
- variables
- special variables



### Filter tables with booleans formulas

To filter data, such as with `where`, `whereIn`, etc. we use a formula that returns a boolean value. 

Formulas designed to return a boolean value are useful to narrow data sets to only desired values. In this example, operators are used with functions to limit values in result tables.

```python
from deephaven import new_table
from deephaven.column import string_col, int_col

def f(a, b):
    return a * b

source = new_table([
        string_col("X", ["A", "B", "C", "D", "E", "F", "G"]),
        int_col("Y", [1, 2, 3, 4, 5, 6, 7]),
        int_col("Z", [2, 3, 1, 2, 3, 1, 2])
])

result = source.where(filters = ["(int)f(Y, Z) > 9"])
```

Boolean formulas in filter methods are also known as conditional filters.

### Create data with assignment formulas

To assign values, such as with `update`, `view`, etc. we use formulas to create new values based on other values. 

Formulas designed to return a value are useful in creating new data values. In this example, operators are used with objects to create new columns of values.

```python
from deephaven import empty_table

class MyObj:
    def __init__(self, a, b, c):
        self.a = a
        self.b = b
        self.c = c

    def compute(self, value1):
        return self.a + value1

obj = MyObj(1,2,3)

result = empty_table(10).update(formulas = [
        "A = i",
        "B = A * A",
        "C = A / 2",
        "D = A % 3",
        "E = (int)(byte) C",
        "F = A_[i-2]",
        "G = obj.a",
        "H = obj.compute(A)",
        "I = sqrt(A)"
])
```

## Columns 

In Deephaven we can use columns as values to create the new data. 

In the following example, operators are used to create new columns of values based on prior data.

```python
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table([
        string_col("X", ["A", "B", "C", "D", "E", "F", "G"]),
        int_col("Y", [1, 2, 3, 4, 5, 6, 7]),
        int_col("Z", [2, 3, 1, 2, 3, 1, 2])
])

result = source.update(formulas = ["Sum = Y + Z"])
```

Deephaven has a rich query language that is ready for your expansion.  