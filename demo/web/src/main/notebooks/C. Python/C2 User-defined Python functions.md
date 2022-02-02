# User-defined Python functions

This notebook will demonstrate how to write a Python function that can be used in the Deephaven Query Language. Functions are useful because they allow repeatability of code by just calling one line rather than duplicating efforts.

In this example, a custom, user-defined function is used inside a query string to compute new column values using [`update`](https://deephaven.io/core/docs/reference/table-operations/select/update/).

In Python, you need to import all the tools your query will require. In this example, we are going to make a new table with integer columns by using [`newTable`](https://deephaven.io/core/docs/reference/table-operations/create/newTable/) and [`intCol`](https://deephaven.io/core/docs/reference/table-operations/create/intCol/).

```python
from deephaven.TableTools import newTable, intCol

numbers = newTable(
    intCol("X", 2, 4, 6),
    intCol("Y", 8, 10, 12)
)
```

In Python, a function is defined using the `def` keyword. Information can be passed into functions as arguments. Arguments are comma-separated parameters specified after the function name, inside the parentheses. Values returned by the function are specified using the `return` keyword.

Below we define a function called `f`, which has two arguments (`a` and `b`). When `f` is called, it returns the value `a + b`. For example, `f(1,2)` returns 3.

```python
def f(a, b):
    return a + b
```

We now call the function inside the query string and assign the results to a new column, `Sum`. Here, `f` is called using values in the `X` and `Y` columns.

```python
resultNumbers = numbers.update("Sum = f(X, Y)")
```

The complete code block is shown below. We define a function `f` and use it to create a new table. The new table contains the `X` and `Y` columns from `numbers`, plus a new `Sum` column, which is the summation of columns `X` and `Y`.

```python
from deephaven.TableTools import newTable, intCol

numbers = newTable(
    intCol("X", 2, 4, 6),
    intCol("Y", 8, 10, 12)
)

def f(a, b):
    return a + b

resultNumbers = numbers.update("Sum = f(X, Y)")
```

Once a Python function is created, it can be reused. For example, `f` can be used, without redefinition, to add columns from a new `words` table. Here, we make this table with string columns using [`newTable`](https://deephaven.io/core/docs/reference/table-operations/create/newTable/) and [`stringCol`](https://deephaven.io/core/docs/reference/table-operations/create/stringCol/).

```python
from deephaven.TableTools import stringCol

words = newTable(
    stringCol("Welcome", "Hello ", "Hola ", "Bonjour "),
    stringCol("Day", "Monday", "Tuesday", "Wednesday")
)

resultWords = words.view("Greeting = f(Welcome, Day)")
```

Now you are ready to use your own Python functions in Deephaven!

If you're ready to level up, the [Deephaven documentation](https://deephaven.io/core/docs/) has many more examples.

```python
print("Go to https://deephaven.io/core/docs/tutorials/quickstart/ to download pre-built Docker images.")
```
