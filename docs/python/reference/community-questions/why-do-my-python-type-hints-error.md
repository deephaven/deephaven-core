---
title: Why does my function with type hints throw an error on null values?
---

<em>My query that uses type hints now fails to calculate and throws an error. How do I fix it?</em>

<p></p>

How a null value reaches a Python function depends on the column's type and the parameter's type hint:

- **String and Boolean columns with a built-in `str` or `bool` type hint** — A null arrives as `None`, with or without `Optional`. The function must handle `None` if the column can contain nulls.
- **String and Boolean columns with a NumPy `np.str_` or `np.bool_` type hint** — Deephaven converts every argument to the NumPy type, so a null arrives as the string `"None"` or as `False`, with or without `Optional`. An `is None` check never catches these nulls, so use the built-in `str` or `bool` type hint when the column can contain nulls.
- **Numeric columns with a numeric type hint (`int`, `float`, NumPy scalars)** — A null arrives as the Deephaven null sentinel for the column's type, such as `NULL_INT` for an `int` column or `NULL_LONG` for a `long` column. The sentinel is an ordinary number, so a function that doesn't check for it silently computes with it. Wrapping the type hint in `Optional[...]` converts the sentinel to `None`.
- **Numeric columns with an `object` type hint** — A null arrives as the null sentinel for the column's type, even with `Optional`.

The following example shows the difference between a plain numeric type hint and an `Optional` one:

```python
from typing import Optional
from deephaven import empty_table


def f_str(x: str) -> str:
    return f"got {x}"


def f_int(x: int) -> int:
    return x + 1


def f_optional_int(x: Optional[int]) -> Optional[int]:
    if x is None:
        return None
    return x + 1


source = empty_table(6).update(
    ["X = i % 2 == 0 ? null : `hello`", "Y = i % 2 == 0 ? NULL_INT : i"]
)

# A null string arrives as None, so f_str returns "got None"
t_str = source.update("S = f_str(X)")

# NULL_INT arrives as an ordinary int, so f_int returns NULL_INT + 1 instead of null
t_int = source.update("Z = f_int(Y)")

# Optional converts NULL_INT to None, so f_optional_int returns null
t_optional = source.update("Z = f_optional_int(Y)")
```

To handle nulls in numeric columns, either:

1. **Use `Optional[...]`** to convert nulls to `None`, then handle `None` in the function body.
2. **Filter out nulls** before applying the function, for example with `where("Y != null")`.

## Related documentation

- [Python functions: Type hints](../../how-to-guides/python-functions.md#type-hints)
- [Python functions: Optional return values](../../how-to-guides/python-functions.md#optional-return-values)
- [How do I troubleshoot type hints?](./how-do-i-troubleshoot-type-hints.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
