---
title: Why
sidebar_label: Why are my table operations producing incorrect results?
---

<em>My query that uses type hints now fails to calculate and throws an error. How do I fix it?</em>

<p></p>

The following code will run in earlier Deephaven versions, but due to stricter type hint enforcing, fails in version 0.32 and up:

```python should-fail
from typing import Optional
from deephaven import empty_table


def f1(x: str) -> str:
    return x


def f2(x: Optional[str]) -> Optional[str]:
    return x


t = empty_table(10).update(["X = i%2 == 0 ? null : `A`"])

t2 = t.update("Y = f1(X)")
t3 = t.update("Z = f2(X)")
```

It throws this error:

```text
RuntimeError: java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'TypeError'>
Value: Argument None is not compatible with annotation {<class 'str'>}
Line: 255
Namespace: _convert_arg
```

The calculation of `t2` fails because the type signature of `f1` says that it handles `str` arguments, but the column being passed to it contains nulls. An error results because the data received does not match the type hint in the function.
In most cases, a user wouldn't really _want_ this to work - their type hint doesn't indicate how nulls should be handled, and the error that is thrown makes sure the issue is clearly visible to the user.
