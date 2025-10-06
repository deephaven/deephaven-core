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


tt = time_table("PT60s")

denver_weather = function_generated_table(
    table_generator=pull_denver_weather_data, source_tables=tt
)
```

![The above `denver_weather` table](../assets/how-to/denver-weather.png)

### Execution context

[Function-generated tables](../reference/table-operations/create/function_generated_table.md) require an [execution context](../conceptual/execution-context.md) to run in. If you don't specify an execution context, the method will use the systemic [execution context](../conceptual/execution-context.md). The example above does not specify an execution context, so the systemic execution context is used.

## Related documentation

- [Install Python packages](./install-and-use-python-packages.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`function_generated_table`](../reference/table-operations/create/function_generated_table.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [Execution Context](../conceptual/execution-context.md)
