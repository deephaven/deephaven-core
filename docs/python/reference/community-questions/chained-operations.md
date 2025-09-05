---
title: Does it make any difference if I separate table operations or chain them together?
sidebar_label: Does it make any difference if I separate table operations or chain them together?
---

<em>I have a query in which I create a series of tables via various table operations. I only really need a couple of the resultant tables as output. Does creating tables I don't need along the way affect performance?</em>

<p></p>

The short answer is **most of the time, it does _not_**. We actually encourage users to create intermediate tables during query writing, as they make debugging significantly easier. The following hypothetical queries will be similarly performant:

```python skip-test
t1 = some_table
t2 = some_other_table
a = t1.where("X")
b = a.agg_by("Y")
t_final = b.natural_join(t2)
```

```python skip-test
t1 = some_table
t2 = some_other_table
t_final = t1.where("X").agg_by("Y").natural_join(t2)
```

Chaining table operations like in the latter hypothetical example is mostly beneficial for those who prefer it stylistically. A notable exception to this rule of thumb is [`update_by`](../table-operations/update-by-operations/updateBy.md), which optimizes multiple operations that are chained together:

```python order=:log,result6,result5,result4,result3,result2,result1,source
from deephaven import updateby as ub
from deephaven.time import to_j_instant
from deephaven import empty_table
from random import choice
from time import time


def get_id() -> str:
    return choice(["ABC", "DEF", "GHI", "JKL", "MNO", "PQR", "STU", "VWX"])


start_time = to_j_instant("2024-03-01T00:00:00 ET")

source = empty_table(1_000_000).update(
    [
        "Timestamp = start_time + (long)(ii * SECOND / 10)",
        "ID = get_id()",
        "Volume = randomDouble(0.0, 500.0)",
        "Price = randomDouble(10.0, 100.0)",
    ]
)

cumsum_op = ub.cum_sum("TotalPrice = Total")
cummax_op = ub.cum_max(cols="MaxPrice = Price")
rollingmin_op = ub.rolling_min_time(
    ts_col="Timestamp", cols="RecentMinPrice = Price", rev_time="PT60s"
)
rollingavg_op = ub.rolling_avg_time(
    ts_col="Timestamp", cols="RecentAvgPrice = Price", rev_time="PT60s"
)
rollingmax_op = ub.rolling_max_time(
    ts_col="Timestamp", cols="RecentMaxPrice = Price", rev_time="PT60s"
)

start = time()
result1 = source.update(["Total = Volume * Price"])
result2 = result1.update_by(ops=cumsum_op, by="ID")
result3 = result2.update_by(ops=cummax_op, by="ID")
result4 = result3.update_by(ops=rollingmin_op, by="ID")
result5 = result4.update_by(ops=rollingavg_op, by="ID")
result6 = result5.update_by(ops=rollingmax_op, by="ID")
end = time()

print(f"Total time elapsed: {(end - start):.4f} seconds.")
```

```python order=:log,result,source
from deephaven import updateby as ub
from deephaven.time import to_j_instant
from deephaven import empty_table
from random import choice
from time import time


def get_id() -> str:
    return choice(["ABC", "DEF", "GHI", "JKL", "MNO", "PQR", "STU", "VWX"])


start_time = to_j_instant("2024-03-01T00:00:00 ET")

source = empty_table(1_000_000).update(
    [
        "Timestamp = start_time + (long)(ii * SECOND / 10)",
        "ID = get_id()",
        "Volume = randomDouble(0.0, 500.0)",
        "Price = randomDouble(10.0, 100.0)",
    ]
)

cumsum_op = ub.cum_sum("TotalPrice = Total")
cummax_op = ub.cum_max(cols="MaxPrice = Price")
rollingmin_op = ub.rolling_min_time(
    ts_col="Timestamp", cols="RecentMinPrice = Price", rev_time="PT60s"
)
rollingavg_op = ub.rolling_avg_time(
    ts_col="Timestamp", cols="RecentAvgPrice = Price", rev_time="PT60s"
)
rollingmax_op = ub.rolling_max_time(
    ts_col="Timestamp", cols="RecentMaxPrice = Price", rev_time="PT60s"
)

updateby_ops = [cumsum_op, cummax_op, rollingmin_op, rollingavg_op, rollingmax_op]

start = time()
result = source.update("Total = Volume * Price").update_by(ops=updateby_ops, by="ID")
end = time()

print(f"Total time elapsed: {(end - start):.4f} seconds.")
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
