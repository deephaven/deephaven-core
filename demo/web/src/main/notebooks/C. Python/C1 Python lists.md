# Python lists

This notebook will demonstrate how to use Python lists within Deephaven.

Lists are native Python data structures that can be declared using the bracket `[ ]` syntax.

```python
my_list = [1, 2, 3]
```

Lists can be used just like other objects within a query. When using a list within a query, Deephaven will automatically assign its column to the `org.jpy.PyListWrapper` type.

```python
from deephaven import empty_table

list_table = empty_table(size = 1).update(formulas = ["List = my_list"])
```

When using a custom function to generate a list, you need to cast the function call to `org.jpy.PyListWrapper` in order for Deephaven to recognize the return value as a list and assign the column type properly. Otherwise, it will be defined as a generic object and won't be useable as expected.

```python
def list_creator():
   return [1, 2, 3]

list_table_from_function = empty_table(size = 1).update(formulas = ["List = (org.jpy.PyListWrapper)list_creator()"])
```

Given a table containing a column of lists, you can extract the values into individual columns by accessing the [column's array value](https://deephaven.io/core/docs/how-to-guides/work-with-arrays/) by appending `_` to its name, indexing the array, and then using the Python bracket syntax for lists to access the individual element.

```python
list_table_extracted = list_table.update(formulas = ["Value0 = List_[i][0]", "Value1 = List_[i][1]", "Value2 = List_[i][2]"])
```

To break this down: 

- `List_` is the array representation of the `List` column
- `List_[i]` accesses the _list_ at index `i` (reminder that `i` is a special character in Deephaven)
- `List_[i][0]`, `List_[i][1]`, and `List_[i][2]` access the individual elements of the list

If you hover your mouse over the columns in the `list_table_extracted` table, you'll notice they're of type `org.jpy.PyObject`.  These columns clearly contain integer values, so let's cast them appropriately.

```python
list_table_extracted = list_table.update(formulas = ["Value0 = (int)List_[i][0]", "Value1 = (int)List_[i][1]", "Value2 = (int)List_[i][2]"])
```

Now the data is stored in integer format.