---
title: Does it make any difference if I separate table operations or chain them together?
sidebar_label: Does it make any difference if I separate table operations or chain them together?
---

<em>I have a query in which I create a series of tables via various table operations. I only really need a couple of the resultant tables as output. Does creating tables I don't need along the way affect performance?</em>

<p></p>

The short answer is **most of the time, it does _not_**. We actually encourage users to create intermediate tables during query writing, as they make debugging significantly easier. The following hypothetical queries will be similarly performant:

```groovy skip-test
t1 = someTable
t2 = someOtherTable
a = t1.where("X")
b = a.aggBy("Y")
tFinal = b.naturalJoin(t2)
```

```groovy skip-test
t1 = someTable
t2 = someOtherTable
tFinal = t1.where("X").aggBy("Y").naturalJoin(t2)
```

Chaining table operations like in the latter hypothetical example is mostly beneficial for those who prefer it stylistically. A notable exception to this rule of thumb is [`updateBy`](../table-operations/update-by-operations/updateBy.md), which optimizes multiple operations that are chained together:

```groovy order=:log,result6,result5,result4,result3,result2,result1,source
import java.time.*

import static java.util.concurrent.ThreadLocalRandom.current

getId = {
    def options = ["ABC", "DEF", "GHI", "JKL", "MNO", "PQR", "STU", "VWX"]
    return options[current().nextInt(options.size())]
}

startTime = Instant.parse("2024-01-02T09:20:00Z")

source = emptyTable(1000000).update(
    "Timestamp = startTime + (long)(ii * SECOND / 10)",
    "ID = getId()",
    "Volume = randomDouble(0.0, 500.0)",
    "Price = randomDouble(10.0, 100.0)"
)

cumSumOp = CumSum("TotalPrice = Total")
cumMaxOp = CumMax("MaxPrice = Price")
rollingMinOp = RollingMin("Timestamp", 60 * SECOND, "RecentMinPrice = Price")
rollingAvgOp = RollingAvg("Timestamp", 60 * SECOND, "RecentAvgPrice = Price")
rollingMaxOp = RollingMax("Timestamp", 60 * SECOND, "RecentMaxPrice = Price")

start = now()
result1 = source.update("Total = Volume * Price")
result2 = result1.updateBy(cumSumOp, "ID")
result3 = result2.updateBy(cumMaxOp, "ID")
result4 = result3.updateBy(rollingMinOp, "ID")
result5 = result4.updateBy(rollingAvgOp, "ID")
result6 = result5.updateBy(rollingMaxOp, "ID")
end = now()

elapsed = Duration.between(start, end).toMillis() / 1000.0
println String.format("Total time elapsed: %.4f seconds.", elapsed)
```

```groovy order=:log,result,source
import java.time.*

import static java.util.concurrent.ThreadLocalRandom.current

getId = {
    def options = ["ABC", "DEF", "GHI", "JKL", "MNO", "PQR", "STU", "VWX"]
    return options[current().nextInt(options.size())]
}

startTime = Instant.parse("2024-03-01T09:20:00Z")

source = emptyTable(1000000).update(
    "Timestamp = startTime + (long)(ii * SECOND / 10)",
    "ID = getId()",
    "Volume = randomDouble(0.0, 500.0)",
    "Price = randomDouble(10.0, 100.0)"
)

cumSumOp = CumSum("TotalPrice = Total")
cumMaxOp = CumMax("MaxPrice = Price")
rollingMinOp = RollingMin("Timestamp", 60 * SECOND, "RecentMinPrice = Price")
rollingAvgOp = RollingAvg("Timestamp", 60 * SECOND, "RecentAvgPrice = Price")
rollingMaxOp = RollingMax("Timestamp", 60 * SECOND, "RecentMaxPrice = Price")

updateByOps = [cumSumOp, cumMaxOp, rollingMinOp, rollingAvgOp, rollingMaxOp]

start = now()
result = source.update("Total = Volume * Price").updateBy(updateByOps, "ID")
end = now()

elapsed = Duration.between(start, end).toMillis() / 1000.0
println String.format("Total time elapsed: %.4f seconds.", elapsed)
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
