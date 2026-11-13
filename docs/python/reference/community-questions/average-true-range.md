---
title: Does Deephaven have a built-in ATR (average true range)?
---

No, Deephaven does not have a built-in [ATR (average true range)](https://www.investopedia.com/terms/a/atr.asp) operation. However, you can accomplish this using [aggregations](../table-operations/group-and-aggregate/aggBy.md) and [`update_by`](../../reference/table-operations/update-by-operations/updateBy.md), such as in the code below.

The first code block sets up a table of open, high, low, and close (OHLC) prices per day as a preliminary step.

<details>
<summary>Get some sample data</summary>

```python test-set=1 order=trades,data
from deephaven import empty_table, agg, updateby
from deephaven.calendar import calendar

cal = calendar("USNYSE_EXAMPLE")

trades = (
    empty_table(500)
    .update(
        [
            "Timestamp = '2025-05-01T09:30:00 ET' + ii * HOUR",
            "Date = formatDate(Timestamp, 'ET')",
            "Sym = ii % 2 == 0 ? `COS` : `SIN`",
            "Price = ii % 2 == 0 ? 50 + 10 * cos(0.1 * ii) : 50 + 10 * sin(0.1 * ii)",
        ]
    )
    .where("cal.isBusinessDay(Date)")
)

data = trades.agg_by(
    [
        agg.first("Open=Price"),
        agg.last("Close=Price"),
        agg.max_("High=Price"),
        agg.min_("Low=Price"),
    ],
    by=["Date", "Sym"],
)
```

</details>

Now, we calculate the ATR for each symbol using a 14-day rolling average. The true range (TR) for each day is the maximum of the high minus the low, the absolute value of the high minus the prior close, and the absolute value of the low minus the prior close. The ATR is the simple rolling average of TR over the last 14 business days.

> [!NOTE]
> Wilder's original ATR uses an exponentially smoothed average rather than a simple moving average. The example below uses a simple rolling average for simplicity.

```python test-set=1 order=atr
atr = (
    data.update("PriorBusDay = cal.plusBusinessDays(Date, -1)")
    .natural_join(data, on=["PriorBusDay=Date", "Sym"], joins="ClosePrior=Close")
    .where("!isNull(ClosePrior)")
    .update("TR = max(High-Low, abs(High-ClosePrior), abs(Low-ClosePrior))")
    .update_by([updateby.rolling_avg_tick("ATR=TR", rev_ticks=14)], by=["Sym"])
)
```
