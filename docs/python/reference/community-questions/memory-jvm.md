---
title: How does memory work in the JVM?
---

## Why does memory usage rise and fall in a sawtooth pattern?

The sawtooth memory pattern is normal JVM behavior — memory usage increases as objects are created, then drops when garbage collection runs.

## Why doesn't freed memory go back to the OS?

The JVM keeps freed memory available for future allocations rather than immediately returning it to the OS for performance reasons. Think of it like reserving a room: if you reserve a small room, you fill it quickly and have to tidy often, but you only hold a small room. If you reserve a gymnasium, you spread out into all the available space even though you don't need it — the JVM claims the room but doesn't give it back.

The _heap_ is the memory the JVM uses to store objects, and its maximum size is set when the JVM starts (see [How to set the heap size](../../how-to-guides/heap-size.md)). The JVM grows its heap toward the maximum you allocate and keeps that memory reserved. Deephaven uses the G1 collector, which frees short-lived objects often, but it tends to let the heap grow rather than shrink it. So the same Deephaven application can show higher memory use with an 8 GB heap than with 4 GB, even though it doesn't need the extra memory. This is why you might see high memory usage even when your application could theoretically use less.

For a comprehensive understanding of JVM memory management, garbage collection, and memory allocation strategies, see Oracle's [HotSpot Virtual Machine Garbage Collection Tuning Guide](https://docs.oracle.com/en/java/javase/21/gctuning/).

For Deephaven-specific memory configuration and performance guidance, see:

- [Configure and adjust heap size](../../how-to-guides/heap-size.md)
- [Run a full garbage collection](../../how-to-guides/performance/garbage-collection.md)
- [Performance tables for monitoring](../../how-to-guides/performance/performance-tables.md)
- [Table memory usage estimation](./find-how-much-memory-a-table-is-using.md)
- [Minimum memory allocation recommendation](./whats-minimum-amount-memory-to-allocate.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not answered in our documentation, [join our Community](/slack) and we'll be happy to help.
