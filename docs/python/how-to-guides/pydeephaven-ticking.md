---
title: Subscribe to ticking tables from a Python client
---

This guide shows how to receive live updates from a Deephaven table in an external Python application with the `pydeephaven-ticking` package.

A _ticking_ table is one whose contents can change while the server runs — the server can add, remove, or modify rows at any time. The base [`pydeephaven`](../getting-started/pyclient-quickstart.md) package can only fetch a point-in-time snapshot of such a table. `pydeephaven-ticking` adds a subscription: your code receives callbacks that contain just the rows that changed.

Callbacks don't arrive for every individual change. The server collects changes and sends them at most once per update interval, which is one second by default, so a single callback can cover many changes. If nothing changed during an interval, no callback arrives.

> [!NOTE]
> This guide covers the _client_ package, which runs outside the Deephaven server. To react to table changes in code that runs on the server, see [Listen to ticking tables](./table-listeners-python.md).

## Install the package

`pydeephaven-ticking` is published to PyPI as prebuilt packages for **Linux on x86_64 only**. It wraps Deephaven's C++ client with Cython, and no macOS, Windows, or Linux ARM packages are available. See the [version matrix](../reference/version-matrix.md) for details.

```sh
pip install pydeephaven-ticking
```

This also installs `pydeephaven`. Install the version that matches your server, for example `pip install pydeephaven-ticking==42.5` for a 42.5 server.

On other platforms, run your client code in a Linux container. For example, this starts a Python shell with the package installed:

```sh
docker run --rm -it python:3.12-slim sh -c 'pip install pydeephaven-ticking && python'
```

Inside a container, `localhost` refers to the container itself, not to your computer. Point the `Session` at the server in one of these ways:

- **Server on your computer, Docker Desktop on macOS or Windows:** connect with `Session(host="host.docker.internal", port=10000)`.
- **Server on your computer, Linux:** add `--network host` to `docker run`, then connect to `localhost`.
- **Server in another container:** add `--network container:<server-container-name>` to `docker run`, then connect to `localhost`.

You don't import `pydeephaven_ticking` directly. When it's installed, `pydeephaven` exposes four extra names: `listen`, `TableListener`, `TableUpdate`, and `TableListenerHandle`.

```python skip-test
from pydeephaven import Session, TableListener, TableUpdate, listen
```

If that import fails, run `import pydeephaven_ticking` to see why. `pydeephaven` only adds the four names when it can import `pydeephaven_ticking`, and it hides the reason when it can't. The direct import shows the real error — for example, the package isn't installed in the active environment, or its compiled library can't load on your platform.

## Subscribe with a function

The simplest listener is a function that takes one argument — a `TableUpdate`:

```python skip-test
import time
from pydeephaven import Session, TableUpdate, listen

session = Session(host="localhost", port=10000)
table = session.time_table("PT1s").update(["X = ii"])


def on_update(update: TableUpdate) -> None:
    added = update.added()
    if added:
        print(added["X"].to_pylist())


handle = listen(table, on_update)
handle.start()  # Subscribe and start receiving updates on a background thread.

time.sleep(10)  # The main thread is free to do other work.

handle.stop()  # Unsubscribe and wait for the background thread to exit.
session.close()
```

`listen` takes a [`Table`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table) reference and a listener, and returns a `TableListenerHandle`. Nothing happens until you call `start`. The table can be any table reference — one you create with the session, or an existing server table opened with [`open_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.open_table).

## Subscribe with a listener class

For listeners that keep state or need custom error handling, subclass `TableListener`. You must implement `on_update`; `on_error` is optional.

```python skip-test
from pydeephaven import TableListener, TableUpdate, listen


class PrintingListener(TableListener):
    def on_update(self, update: TableUpdate) -> None:
        for kind, cols in [
            ("removed", update.removed()),
            ("added", update.added()),
            ("modified", update.modified()),
        ]:
            if cols:
                print(kind, {name: arr.to_pylist() for name, arr in cols.items()})

    def on_error(self, error: Exception) -> None:
        print(f"Subscription failed: {error}")


handle = listen(table, PrintingListener())
handle.start()
```

## Read the changes in a table update

Each call to `on_update` receives a `TableUpdate` that describes every change since the previous call. Every accessor returns a `dict` that maps column names to [PyArrow arrays](https://arrow.apache.org/docs/python/generated/pyarrow.Array.html). The arrays in one `dict` all have the same length, and position `i` in each array belongs to the same row. If a category has no rows in this update, the accessor returns an empty `dict`.

| Method          | Returns                                                                            |
| --------------- | ---------------------------------------------------------------------------------- |
| `added`         | Rows added in this update.                                                         |
| `removed`       | Rows removed in this update, with the values they had before removal.              |
| `modified`      | Rows modified in this update, with their new values.                               |
| `modified_prev` | The same rows as `modified`, in the same order, with the values before the change. |

Keep these points in mind:

- **The first update is the initial snapshot.** When the subscription starts, every row currently in the table arrives as an addition. If the table is empty at that moment, the first update has no rows.
- **Modified rows include every requested column.** A row is reported as modified if any of its columns changed, and `modified` returns all the columns you ask for, not just the ones that changed. This means you can always read a key column alongside the changed values.
- **`modified` and `modified_prev` line up by position.** Use them together to see how each row changed — for example, to compute a price move.
- **One update can combine several changes to the same row.** The server merges all the changes from an update interval, so each row appears at most once per category:
  - A row modified several times appears once in `modified`, with its latest values. `modified_prev` holds its values from before the first of those changes.
  - A row added and then modified appears only in `added`, with its latest values. It isn't also in `modified`.
  - A row added and then removed doesn't appear at all.
  - A row removed and then added again appears in both `removed` and `added`. Apply removals before additions so that the row ends up present.

### Request only the columns you need

Every accessor takes an optional `cols` argument — a single column name or a list of names. By default, it returns all columns.

```python skip-test
def on_update(update: TableUpdate) -> None:
    prices = update.modified(["Sym", "Price"])
    if prices:
        print(dict(zip(prices["Sym"].to_pylist(), prices["Price"].to_pylist())))
```

### Process large updates in chunks

Each accessor has a `_chunks` variant — `added_chunks`, `removed_chunks`, `modified_chunks`, and `modified_prev_chunks` — that returns a generator instead of one `dict`. Each chunk holds at most `chunk_size` rows. Use these to limit memory use when an update, such as the initial snapshot of a large table, contains many rows.

```python skip-test
def on_update(update: TableUpdate) -> None:
    for chunk in update.added_chunks(10_000, ["Sym", "Price"]):
        # Each chunk maps column names to PyArrow arrays of up to 10,000 rows.
        process(chunk)
```

### Data types

Deephaven column types map to these PyArrow types. Null values arrive as PyArrow nulls, which become `None` when you call `to_pylist`.

| Deephaven type            | PyArrow type                      |
| ------------------------- | --------------------------------- |
| `byte`                    | `int8`                            |
| `short`                   | `int16`                           |
| `int`                     | `int32`                           |
| `long`                    | `int64`                           |
| `float`                   | `float32`                         |
| `double`                  | `float64`                         |
| `char`                    | `uint16` (a number, not a string) |
| `boolean`                 | `bool`                            |
| `String`                  | `string`                          |
| `Instant`                 | `timestamp("ns", "UTC")`          |
| `LocalDate`               | `date64`                          |
| `LocalTime`               | `time64("ns")`                    |
| Arrays of the types above | `list` of the PyArrow type        |

The package doesn't support other column types, such as `BigDecimal`. Drop or convert those columns on the server — for example with [`view`](../reference/table-operations/select/view.md) — before you subscribe.

## Subscribe to less data

A subscription always covers every row and every column of the table you pass to `listen`. Shape the table on the server first, so that only the data you need crosses the network:

- [`where`](../reference/table-operations/filter/where.md) to keep only relevant rows
- [`view`](../reference/table-operations/select/view.md) to keep only relevant columns
- [`tail`](../reference/table-operations/filter/tail.md) to keep only the most recent rows
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) to keep only the latest row per key

```python skip-test
trades = session.open_table("trades")
latest = trades.where("Exchange = `NYSE`").view(["Sym", "Price", "Size"]).last_by("Sym")
handle = listen(latest, on_update)
```

## Threading

Each `TableListenerHandle` runs its own background thread. Your listener's methods run on that thread, one update at a time, in order. Keep the following in mind:

- **Don't block in `on_update`.** Updates for this subscription wait while your callback runs. Hand slow work, such as database writes, to another thread or a queue.
- **Protect shared state.** If the main thread reads data that `on_update` writes, guard it with a [`threading.Lock`](https://docs.python.org/3/library/threading.html#lock-objects).
- **You can call `stop` from inside `on_update`.** This ends the subscription after the current callback returns, which is useful for stopping when a condition is met.

## Handle errors

If the connection fails, or `on_update` raises an exception, the subscription **ends** and the listener's `on_error` method runs with the exception. No more updates arrive after that. The default `on_error` prints the error.

When your listener is a function, pass an error callback as the third argument to `listen`:

```python skip-test
def on_error(error: Exception) -> None:
    print(f"Subscription failed: {error}")


handle = listen(table, on_update, on_error)
```

To recover, create a new handle and call `start` again. If you'd rather keep the subscription running after a bad update, catch exceptions inside `on_update`.

`listen` itself raises `ValueError` if a function listener or error callback doesn't take exactly one argument, or if you pass `on_error` together with a `TableListener` object. Use the object's `on_error` method instead.

## Stop the subscription

Call `stop` when you're done. It cancels the subscription and waits for the background thread to exit, so no callbacks run after it returns. Close the session after stopping its handles. Use `try`/`finally` so that cleanup happens even when your program fails or is interrupted:

```python skip-test
handle = listen(table, on_update)
handle.start()
try:
    run_application()
finally:
    handle.stop()
    session.close()
```

## Example: keep a live local copy of a table

This example keeps a local `dict` in sync with a ticking table keyed by symbol and prints alerts for large price moves. It applies removals first, then additions, then modifications.

```python skip-test
import threading
import time

from pydeephaven import Session, TableListener, TableUpdate, listen

COLS = ["Sym", "Price", "Size"]


def rows(cols: dict) -> list[tuple]:
    """Turns a dict of column name to PyArrow array into a list of row tuples."""
    return list(zip(*(cols[c].to_pylist() for c in COLS))) if cols else []


class QuoteBook(TableListener):
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.quotes: dict[str, tuple[float, int]] = {}

    def on_update(self, update: TableUpdate) -> None:
        removed = update.removed("Sym")
        added = update.added(COLS)
        prev, curr = update.modified_prev(COLS), update.modified(COLS)

        with self.lock:
            for sym in removed.get("Sym", []):
                self.quotes.pop(sym.as_py(), None)
            for sym, price, size in rows(added):
                self.quotes[sym] = (price, size)
            for (sym, old_price, _), (_, price, size) in zip(rows(prev), rows(curr)):
                self.quotes[sym] = (price, size)
                if None not in (old_price, price) and abs(price - old_price) >= 4.0:
                    print(f"ALERT {sym}: {old_price:.2f} -> {price:.2f}")

    def snapshot(self) -> dict[str, tuple[float, int]]:
        with self.lock:
            return dict(self.quotes)


session = Session(host="localhost", port=10000)
quotes = (
    session.time_table("PT0.25s")
    .update(
        [
            "Sym = `SYM` + randomInt(0, 5)",
            "Price = 100.0 + randomGaussian(0.0, 5.0)",
            "Size = randomInt(1, 1000)",
        ]
    )
    .last_by("Sym")
)

book = QuoteBook()
handle = listen(quotes, book)
handle.start()
try:
    for _ in range(10):
        time.sleep(3)
        print(book.snapshot())
finally:
    handle.stop()
    session.close()
```

The server table uses [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md), so each new symbol arrives as an added row and later trades for that symbol arrive as modified rows. If a symbol's first trade and later trades fall in the same update interval, only an added row arrives, with the latest trade. The listener handles that case without special code because it stores every added row. The listener reads `Sym` with every modification even though only `Price` and `Size` change, which is what lets it find the right entry in the `dict`.

## Related documentation

- [Python client quickstart](../getting-started/pyclient-quickstart.md)
- [Listen to ticking tables](./table-listeners-python.md)
- [Send data to Deephaven from a Python client](./client-input-tables.md)
- [What is Barrage?](../conceptual/what-is-barrage.md)
- [Version matrix](../reference/version-matrix.md)
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [Python client API reference](/core/client-api/python/index.html)
