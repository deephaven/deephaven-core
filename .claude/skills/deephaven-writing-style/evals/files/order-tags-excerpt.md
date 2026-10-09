## Example tables

The examples in this section use the following two tables:

```python test-set=1 order=employees,departments
from deephaven import new_table
from deephaven.column import string_col, int_col

employees = new_table(
    [
        string_col("LastName", ["Rafferty", "Jones", "Steinberg", "Robins"]),
        int_col("DeptID", [31, 33, 33, 34]),
    ]
)
departments = new_table(
    [
        int_col("DeptID", [31, 33, 34]),
        string_col("DeptName", ["Sales", "Engineering", "Clerical"]),
    ]
)
```

### `natural_join`

```python test-set=1 order=result,employees,departments
result = employees.natural_join(table=departments, on=["DeptID"])
```

### `exact_join`

```python test-set=1 order=result,departments,employees
result = employees.exact_join(table=departments, on=["DeptID"])
```
