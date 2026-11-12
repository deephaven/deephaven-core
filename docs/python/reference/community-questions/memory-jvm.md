---
title: How does memory work in the JVM?
---

- "Why does my Java application's memory usage keep going up and down like a sawtooth pattern?"

The sawtooth memory pattern is normal JVM behavior - memory usage increases as objects are created, then drops when garbage collection runs. The JVM keeps freed memory available for future allocations rather than immediately returning it to the OS for performance reasons.

- "If garbage collection is working, why doesn't memory go back to the OS?"

Java's garbage collector is like a small child with a chest full of toys. If you put them in a small room, they'll scatter toys around but only clear small areas when they need space for new toys - the rest stays cluttered. Give them an entire gymnasium, and they'll keep using new, uncluttered areas until the entire space is full before cleaning up.

The JVM behaves similarly. It collects garbage regularly, but it tends to grow its heap toward the maximum you allocate rather than collect aggressively. So the same Deephaven application can show higher memory use with 8GB of heap than with 4GB. This is why you might see high memory usage even when your application could theoretically use less.

For a comprehensive understanding of JVM memory management, garbage collection, and memory allocation strategies, see Oracle's [HotSpot Virtual Machine Garbage Collection Tuning Guide](https://docs.oracle.com/en/java/javase/21/gctuning/).

For Deephaven-specific memory configuration and performance guidance, see:

- [Garbage collection guide](../../how-to-guides/performance/garbage-collection.md)
- [Performance tables for monitoring](../../how-to-guides/performance/performance-tables.md)
- [Table memory usage estimation](./find-how-much-memory-a-table-is-using.md)
- [Minimum memory allocation recommendation](./whats-minimum-amount-memory-to-allocate.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not answered in our documentation, [join our Community](/slack) and we'll be happy to help.
