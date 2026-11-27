---
title: Does it make any difference if I separate table operations or chain them together?
---

<em>I have a query in which I create a series of tables via various table operations. I only really need a couple of the resultant tables as output. Does creating tables I don't need along the way affect performance?</em>

The short answer is **most of the time, it does _not_**. We actually encourage users to create intermediate tables during query writing, as they make debugging significantly easier. The following hypothetical queries will be similarly performant:

```groovy skip-test
t1 = someTable
t2 = someOtherTable
a = t1.where("X > 10")
b = a.sumBy("Y")
tFinal = b.naturalJoin(t2, "Y")
```

```groovy skip-test
t1 = someTable
t2 = someOtherTable
tFinal = t1.where("X > 10").sumBy("Y").naturalJoin(t2, "Y")
```

Chaining operations, as in the second example, is mostly a matter of style. A notable exception is [`updateBy`](../table-operations/update-by-operations/updateBy.md): passing several operations to a single `updateBy` call is faster than applying them one at a time in separate calls, whether those calls are chained or not:

```groovy order=:log,result6,result5,result4,result3,result2,result1,source
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

cumSumOp = CumSum("CumTotal = Total")
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

cumSumOp = CumSum("CumTotal = Total")
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

The single `updateBy` call finishes faster because it groups the rows by `ID` once and computes all five operations together.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
