---
title: How can I periodically write a ticking table to disk?
---

It's common to have queries on ticking tables where it's prudent to write that data to disk periodically. There are a few ways to do this.

## Java `ScheduledExecutorService`

There are several ways to periodically call functions in Groovy. The following example uses Java's [`ScheduledExecutorService`](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ScheduledExecutorService.html) to write a ticking table to disk every 30 seconds.

```groovy skip-test
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

// Create a ticking table
myData = timeTable("PT0.2s").update("X = i", "Y = randomDouble(0, 100)")

// Define the task to write to CSV
def writeToDisk = {
    try {
        writeCsv(myData, "/data/my_data.csv")
    } catch (Exception e) {
        e.printStackTrace()
    }
}

// Set up a scheduled executor to run every 30 seconds
def scheduler = Executors.newSingleThreadScheduledExecutor()
scheduler.scheduleAtFixedRate(writeToDisk as Runnable, 30, 30, TimeUnit.SECONDS)
```

## Time table

You can also just use a time table to trigger writes. For instance, if you want to write every 30 seconds, have a time table tick every 30 seconds and call a Groovy function that writes the ticking table of interest to disk.

```groovy skip-test
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

// Create the ticking source table
myData = timeTable("PT0.2s").update(
    "X = randomDouble(0, 100)",
    "Y = randomInt(-100, 100)",
    "Z = randomBool()"
)

// Create a ring table to avoid writing unbounded data
myDataRing = RingTableTools.of(myData, 1000) // adjust capacity as needed

// Define a function to write the ring table to disk
writeToDisk = { ->
    try {
        writeCsv(myDataRing, "/data/my_data.csv")
    } catch (Exception e) {
        return false
    }
    return true
}

// Create a table that triggers the write_to_disk function every 30 seconds
writeTrigger = timeTable("PT30s").update(
    "WriteSuccessful = writeToDisk()"
)
```
