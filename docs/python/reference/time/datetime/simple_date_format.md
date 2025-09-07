---
title: simple_date_format
---

The `simple_date_format` method creates a Java [`SimpleDateFormat`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/text/SimpleDateFormat.html) from a date-time string.

> [!NOTE]
> This method is intended for use in Python code when a [`SimpleDateFormat`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/text/SimpleDateFormat.html) is needed. It should not be used directly in query strings.
>
> The function is commonly used to create a [`SimpleDateFormat`](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/text/SimpleDateFormat.html) in Python, which is then used in query strings.

## Syntax

```python syntax
simple_date_format(pattern: str) -> SimpleDateFormat
```

## Returns

A `SimpleDateFormat`.

## Example

```python order=:log
from deephaven.time import simple_date_format

date_str = "2021/12/10 14:21:17"

sdf = simple_date_format(date_str)

print(type(sdf))
```

## Related Documentation

- [Pydoc](/core/pydoc/code/deephaven.time.html#deephaven.time.simple_date_format)
