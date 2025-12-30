---
title: Does Deephaven have a built-in ATR (average true range)?
sidebar_label: Does Deephaven have a built-in ATR?
---

No, Deephaven does not have a built-in [ATR (average true range)](https://www.investopedia.com/terms/a/atr.asp) operation. However, you can accomplish this using aggregations and [`update_by`](../../reference/table-operations/update-by-operations/updateBy.md), such as in the code below.

The first code block sets up a table of OHLC trades data as a preliminary step.

<details>
<summary>Get some sample data</summary>

```python test-set=1 order=trades,data
from deephaven import time_table, agg, updateby
from deephaven.calendar import calendar

cal = nyse_cal = calendar("USNYSE_EXAMPLE")

trades = (
    time_table("PT00:00:01", "2024-09-01T00:00 ET")
    .update(
        [
            "Date = formatDate(Timestamp, 'ET')",
            "Sym = ii%2 == 0 ? `COS` : `SIN`",
            "Price = ii%2 == 0 ? cos(0.000001*ii) : sin(0.0000001*ii)",
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

Now, we calculate the ATR for each symbol using a 14-day rolling average. The ATR is calculated as the maximum of the high minus the low, the absolute value of the high minus the prior close, and the absolute value of the low minus the prior close.

```python test-set=1 order=atr
atr = (
    data.update("PriorBusDay = cal.plusBusinessDays(Date, -1)")
    .natural_join(data, on=["PriorBusDay=Date", "Sym"], joins="ClosePrior=Close")
    .where("!isNull(ClosePrior)")
    .update("TR = max(High-Low, abs(High-ClosePrior), abs(Low-ClosePrior))")
    .update_by([updateby.rolling_avg_tick("ATR=TR", rev_ticks=14)], by=["Sym"])
)
```
