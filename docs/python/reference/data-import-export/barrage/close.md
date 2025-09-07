---
title: close
---

The `close` method closes the [`BarrageSession`](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession).

> [!NOTE]
> If the [`BarrageSession`](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession) is initialized with a managed channel, `close` will cut down the channel as well.

## Syntax

```python syntax
close()
```

## Parameters

This method does not take any parameters.

## Returns

Closes the [`BarrageSession`](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession).

## Examples

```python skip-test
from deephaven.barrage import barrage_session

barrage_sesh = barrage_session("localhost", 10000)

barrage_sesh.close()
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession.close)
