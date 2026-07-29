---
title: Generate tables with Python functions
---

This guide covers [function-generated tables](../reference/table-operations/create/function_generated_table.md), which enable the creation of ticking tables via a Python function. The function is run when either:

- One or more source tables tick.
- A refresh interval is reached.

## Usage pattern

[Function-generated tables](../reference/table-operations/create/function_generated_table.md) follow this basic usage pattern:

- Define a Python function that returns a table.
- Define one or more trigger tables or a refresh interval.
- Create a function-generated table by calling `function_generated_table`.
  - A function-generated table can use one or both of the following to trigger the function call:
    - A trigger table.
    - A refresh interval.

A [function-generated table](../reference/table-operations/create/function_generated_table.md) is designed to ingest data from external sources into ticking tables. The only requirement is that the Python function that ingests this data returns a table.

### Table generator function

You can define your function in the normal Pythonic way. The only requirement is that the function must return a table.

Here's an example:

```python order=null
from deephaven import empty_table


def make_table():
    return empty_table(5).update(
        ["X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)"]
    )
```

### Call `function_generated_table`

The following code block uses `make_table` as the table generator function. [`function_generated_table`](../reference/table-operations/create/function_generated_table.md) is called twice:

- Once with a trigger table.
- Once with a refresh interval.

```python test-set=1 order=result_from_table,result_from_refresh_interval reset
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

## Weather data

The following example pulls weather from NOAA's free-to-use [Weather API](https://www.weather.gov/documentation/services-web-api) for the city of Denver, Colorado. The trigger table ticks once per minute.

```python ticking-table order=null
from deephaven import function_generated_table
from deephaven import column as dhcol
from deephaven import time_table
from deephaven import new_table

from urllib.request import Request, urlopen
import json


def pull_denver_weather_data():
    req = Request("https://api.weather.gov/gridpoints/BOU/63,62/forecast/hourly")
    req.add_header("deephaven.io", "social@deephaven.io")
    content = json.loads(urlopen(req).read())
    weather = content["properties"]["periods"]
    n_weather = len(weather)
    times = [0] * n_weather
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

### Execution context

[Function-generated tables](../reference/table-operations/create/function_generated_table.md) require an [execution context](../conceptual/execution-context.md) to run in. If you don't specify an execution context, the method will use the systemic [execution context](../conceptual/execution-context.md). The example above does not specify an execution context, so the systemic execution context is used.

## Additional options

Beyond the trigger, [`function_generated_table`](../reference/table-operations/create/function_generated_table.md) accepts several optional parameters that control how the result is produced and shaped.

### Retain the previous result

The `table_generator` function can return `None` to decline producing a new table on a given cycle. When it does, the previous cycle's result is retained instead of being regenerated. This is useful when new data is not always available. When the first invocation returns `None`, supply a `table_definition` so the result's columns are known before the first table exists.

```python order=null
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

### Copy data or delegate to the generated table

By default (`copy_data=True`), so the generated rows are copied into the result's own columns, and each refresh replaces the result entirely. With `copy_data=False`, the result delegates directly to the generated table's column sources instead of copying, which avoids the copy and adopts the generated table's row set. Because the result holds the generated column sources across cycles, a refreshing generated table must expose immutable column sources; a static table produced fresh on each refresh — for example, via [`snapshot`](../reference/table-operations/snapshot/snapshot.md) — always satisfies this requirement.

### Present the result as a blink table

Set `blink_table=True` to present the result as a [blink table](../conceptual/table-types.md#specialization-3-blink), so downstream operations see only the rows generated during the current cycle. A blink table requires a refresh trigger. On a cycle where the `table_generator` returns `None`, the blink result is cleared.

### Specify the table definition

When you supply a `table_definition`, it is authoritative: it defines the result's columns and their order, and every table the `table_generator` produces must be compatible with it. A definition is required when the first invocation returns `None`, since the columns must be known before the first table exists.

## Related documentation

- [Install Python packages](./install-and-use-python-packages.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`function_generated_table`](../reference/table-operations/create/function_generated_table.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [Table types](../conceptual/table-types.md)
- [Execution Context](../conceptual/execution-context.md)
