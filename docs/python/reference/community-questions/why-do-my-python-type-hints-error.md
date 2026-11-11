---
title: Why does my function with type hints throw an error on null values?
---

<em>My query that uses type hints now fails to calculate and throws an error. How do I fix it?</em>

<p></p>

How Deephaven handles null values in function arguments depends on the type hint:

- **`str` and `object` parameters** — Null values pass through as `None` with no conversion and no error. The function must handle `None` if the column can contain nulls.
- **Primitive parameters (`int`, `float`, `bool`, NumPy scalars)** — Null values pass through as the Deephaven null sentinel value (for example, `NULL_INT` for `int`). This can produce unexpected results if the function doesn't check for the sentinel.
- **`Optional[...]` parameters** — Wrapping a primitive type hint in `Optional` enables null-to-`None` conversion. For example, `Optional[int]` converts null values to `None` instead of passing the null sentinel. For `str` and `object`, `Optional` has no effect because nulls already pass through as `None`.

The following example demonstrates these behaviors:

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


source = empty_table(5).update(
    ["X = i % 2 == 0 ? null : `hello`", "Y = i % 2 == 0 ? null : i"]
)

# str with nulls: None passes through, f_str returns "got None"
t1 = source.update("S = f_str(X)")

# Optional[int] with nulls: null becomes None, handled explicitly
t2 = source.update("Z = f_optional_int(Y)")
```

To fix errors caused by null values in columns with primitive type hints, either:

1. **Use `Optional[...]`** to convert nulls to `None`, then handle `None` in the function body.
2. **Filter out nulls** before applying the function, for example with `where("Y != null")`.

## Related documentation

- [Python functions: Type hints](../../how-to-guides/python-functions.md#type-hints)
- [Python functions: Optional return values](../../how-to-guides/python-functions.md#optional-return-values)
- [How do I troubleshoot type hints?](./how-do-i-troubleshoot-type-hints.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
