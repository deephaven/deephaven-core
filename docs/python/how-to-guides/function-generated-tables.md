---
title: Generate tables with Python functions
---

This guide covers [function-generated tables](../reference/table-operations/create/function_generated_table.md), which create a table from a Python function. With a trigger table or a refresh interval, the result is a ticking table, one whose rows update over time. The function runs once when the table is created, and again whenever:

- One or more trigger tables tick.
- A refresh interval is reached.

Use a function-generated table to ingest data from external sources into a ticking table. A refresh that produces a new table replaces the whole result, so when the input is already a Deephaven table, use regular table operations, many of which update incrementally.

## Usage pattern

[Function-generated tables](../reference/table-operations/create/function_generated_table.md) follow this basic usage pattern:

- Define a Python function that returns a table.
- Create a function-generated table by calling [`function_generated_table`](../reference/table-operations/create/function_generated_table.md) with that function and at most one kind of trigger:
  - One or more trigger tables, passed as `source_tables`. Every trigger table must be a refreshing (ticking) table.
  - A refresh interval, passed as `refresh_interval_ms`.

If you supply neither trigger, or a `refresh_interval_ms` that is zero or negative, the function runs once and the result is a static table.

### Define the generator and call `function_generated_table`

Define the table generator function as you would any other Python function. It must return one of the following:

- A table with the same column names and types as the result. The result takes its columns from the first generated table, or from the [`table_definition`](#specify-the-table-definition) when you supply one.
- `None`, to [keep the previous result](#retain-the-previous-result).

The function can read data from its source tables, but don't run further table operations on them inside the function:

- Deephaven can return a cached (memoized) result for such an operation.
- The function-generated table doesn't track that cached table as a dependency.
- The function can therefore run before the cached table finishes updating in the same [update cycle](../conceptual/table-update-model.md), and read inconsistent data.

List every ticking table the function depends on in `source_tables`.

The following code block defines `make_table`, which returns a five-row table with a column of random integers and a column of random doubles. It uses `make_table` as the table generator function and calls [`function_generated_table`](../reference/table-operations/create/function_generated_table.md) twice:

- Once with a trigger table.
- Once with a refresh interval.

```python ticking-table order=null reset
from deephaven import time_table, empty_table
from deephaven import function_generated_table


def make_table():
    return empty_table(5).update(
        ["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"]
    )


tt = time_table("PT1S")

result_from_table = function_generated_table(
    table_generator=make_table, source_tables=tt
)

result_from_refresh_interval = function_generated_table(
    table_generator=make_table, refresh_interval_ms=2000
)
```

### Execution context

[Function-generated tables](../reference/table-operations/create/function_generated_table.md) require an [execution context](../conceptual/execution-context.md) to run in. Pass a context with the `exec_ctx` parameter. If you omit it, [`function_generated_table`](../reference/table-operations/create/function_generated_table.md) captures the execution context that is current when you call it, and uses that context for every invocation of the `table_generator`. In a console script, that is the [systemic execution context](../conceptual/execution-context.md#systemic-vs-separate-executioncontext). The examples on this page do not pass `exec_ctx`, so they use the systemic execution context.

## Example: pull data from a web API

The following example pulls the hourly forecast for Denver, Colorado, from the National Weather Service's free-to-use [Weather API](https://www.weather.gov/documentation/services-web-api). Each row is one forecast hour, in time order, and `PctChanceRain` holds the forecast probability of any precipitation. The function re-runs once per minute (`refresh_interval_ms=60_000`).

```python ticking-table order=null
from deephaven import function_generated_table
from deephaven import column as dhcol
from deephaven import new_table

from urllib.request import Request, urlopen
import json


def pull_denver_weather_data():
    req = Request("https://api.weather.gov/gridpoints/BOU/63,62/forecast/hourly")
    # Identify your own application and contact address, as the API requires.
    req.add_header("User-Agent", "(deephaven.io, social@deephaven.io)")
    content = json.loads(urlopen(req).read())
    weather = content["properties"]["periods"]
    n_weather = len(weather)
    temps = [0] * n_weather
    chances_of_rain = [0] * n_weather
    dewpoints = [0] * n_weather
    humidities = [0] * n_weather
    windspeeds = [0] * n_weather
    winddirs = [""] * n_weather
    forecasts = [""] * n_weather
    for idx in range(n_weather):
        temps[idx] = weather[idx]["temperature"]
        chances_of_rain[idx] = weather[idx]["probabilityOfPrecipitation"]["value"]
        dewpoints[idx] = weather[idx]["dewpoint"]["value"]
        humidities[idx] = weather[idx]["relativeHumidity"]["value"]
        windspeeds[idx] = int(weather[idx]["windSpeed"].split()[0])
        winddirs[idx] = weather[idx]["windDirection"]
        forecasts[idx] = weather[idx]["shortForecast"]
    return new_table(
        [
            dhcol.int_col("TempF", temps),
            dhcol.int_col("PctChanceRain", chances_of_rain),
            dhcol.double_col("DewPointC", dewpoints),
            dhcol.int_col("RelativeHumidity", humidities),
            dhcol.int_col("WindSpeedMPH", windspeeds),
            dhcol.string_col("WindDirection", winddirs),
            dhcol.string_col("ShortForecast", forecasts),
        ]
    )


denver_weather = function_generated_table(
    table_generator=pull_denver_weather_data,
    refresh_interval_ms=60_000,
)
```

![The above `denver_weather` table](../assets/how-to/denver-weather.png)

## How the result updates

Every refresh in which the `table_generator` produces a table replaces the result in full. Each [table update](../conceptual/table-update-model.md):

- Removes all of the previous rows.
- Adds all of the newly generated rows.
- Contains no modified rows and no shifts, even when the generated data is identical to the previous cycle's.

Downstream operations therefore reprocess the entire result on every refresh that produces a table, which is why this guide recommends regular table operations when the input is already a Deephaven table.

A refresh in which the `table_generator` returns `None` produces no update, unless the result is a [blink table](../conceptual/table-types.md#specialization-3-blink). See [Retain the previous result](#retain-the-previous-result).

## Additional options

Beyond the trigger, the `table_generator` can decline to produce a new table. [`function_generated_table`](../reference/table-operations/create/function_generated_table.md) also accepts several optional parameters that control how the result is produced and shaped. Two of them refine [how the result updates](#how-the-result-updates), independently of each other:

- [`copy_data`](#copy-data-or-delegate-to-the-generated-table) controls where the result's data lives and what its row keys look like.
- [`blink_table`](#present-the-result-as-a-blink-table) controls how downstream operations interpret each update.

### Retain the previous result

When new data is not always available, the `table_generator` function can return `None` to decline producing a new table on a given update cycle. The result then keeps the previous cycle's table. A [blink table](#present-the-result-as-a-blink-table) result is cleared instead.

If the first invocation returns `None`, supply a [`table_definition`](#specify-the-table-definition) so the result's columns are known before the first table exists.

```python ticking-table order=null
from deephaven import function_generated_table, time_table, new_table
from deephaven.column import int_col
import deephaven.dtypes as dht

tt = time_table("PT1S")


def make_table():
    # Only produce a table once the trigger has rows; otherwise retain the previous result.
    if tt.size == 0:
        return None
    return new_table([int_col("Count", [tt.size])])


result = function_generated_table(
    table_generator=make_table,
    source_tables=tt,
    table_definition={"Count": dht.int32},
)
```

### Specify the table definition

You can supply a `table_definition`, such as a dictionary that maps column names to `deephaven.dtypes` types. The definition is authoritative. It defines the result's columns and their order. Every table the `table_generator` produces must be compatible with it. A definition is required when the first invocation returns `None`. See [Retain the previous result](#retain-the-previous-result).

### Copy data or delegate to the generated table

By default (`copy_data=True`), the generated rows are copied into the result's own [column sources](../conceptual/table-update-model.md#describing-table-updates), and the result uses a flat, contiguous [row set](../conceptual/table-update-model.md#describing-table-updates) with row keys `0` through `size - 1`. The generated table itself is not retained.

With `copy_data=False`, the result skips the copy and delegates directly to the generated table. It:

- Uses the generated table's column sources.
- Adopts the generated table's row set as-is.
- Reports the generated table's row set as each update's added rows and the previous cycle's row set as its removed rows.

Because the result holds the generated column sources across cycles, a refreshing generated table must expose immutable column sources. A ticking table keeps each column's previous-cycle values so downstream operations can see what changed. A generated table that changes values in place would corrupt those values, so [`function_generated_table`](../reference/table-operations/create/function_generated_table.md) rejects a refreshing generated table if any of its column sources is not immutable. A static table produced fresh on each refresh, such as one produced by [`snapshot`](../reference/table-operations/snapshot/snapshot.md), always meets this requirement.

### Present the result as a blink table

Set `blink_table=True` to present the result as a [blink table](../conceptual/table-types.md#specialization-3-blink), so downstream operations see only the rows generated during the current cycle. Each update is still a full replacement, as described in [How the result updates](#how-the-result-updates). The blink setting changes only how downstream operations interpret that update, not how the rows are copied or delegated.

A blink result behaves as follows:

- It requires a refresh trigger.
- Rows generated in one update cycle are removed on the next cycle, whether or not the `table_generator` runs again.
- With a refresh interval longer than one cycle, the result is empty between refreshes.

### Pass arguments to the table generator

Use `args` (a tuple) and `kwargs` (a dictionary) to pass positional and keyword arguments to the `table_generator`. [`function_generated_table`](../reference/table-operations/create/function_generated_table.md) passes the same arguments on every invocation.

## Related documentation

- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`function_generated_table`](../reference/table-operations/create/function_generated_table.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [Table types](../conceptual/table-types.md)
- [Execution Context](../conceptual/execution-context.md)
