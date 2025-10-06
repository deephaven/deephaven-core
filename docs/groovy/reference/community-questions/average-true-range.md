---
title: Does Deephaven have a built-in ATR (average true range)?
sidebar_label: Does Deephaven have a built-in ATR?
---

No, Deephaven does not have a built-in [ATR (average true range)](https://www.investopedia.com/terms/a/atr.asp) operation. However, you can accomplish this using aggregations and [`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md), such as in the code below.

The first code block sets up a table of OHLC trades data as a preliminary step.

<details>
<summary>Get some sample data</summary>

```groovy test-set=1 order=trades,data
import io.deephaven.time.calendar.Calendars

cal = Calendars.calendar()

trades = timeTable("2025-05-01T00:00 ET", "PT00:00:01").update(
    "Date = formatDate(Timestamp, 'ET')",
    "Sym = ii % 2 == 0 ? \"COS\" : \"SIN\"",
    "Price = ii%2 == 0 ? cos(0.000001*ii) : sin(0.0000001*ii)",
).where("cal.isBusinessDay(Date)")

data = trades.aggBy([
    AggFirst("Open=Price"),
    AggLast("Close=Price"),
    AggMax("High=Price"),
    AggMin("Low=Price")],
    "Date", "Sym")
```

</details>

Now, we calculate the ATR for each symbol using a 14-day rolling average. The ATR is calculated as the maximum of the high minus the low, the absolute value of the high minus the prior close, and the absolute value of the low minus the prior close.

```groovy test-set=1 order=atr
atr = data.update("PriorBusDay = cal.plusBusinessDays(Date, -1)")
    .naturalJoin(data, "PriorBusDay=Date, Sym", "ClosePrior=Close")
    .where("!isNull(ClosePrior)")
    .update("TR = max(High-Low, abs(High-ClosePrior), abs(Low-ClosePrior))")
    .updateBy([RollingAvg(14, "ATR=TR")], "Sym")
```
