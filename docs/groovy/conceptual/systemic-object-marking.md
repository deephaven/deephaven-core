---
title: Systemic object marking
---

In Deephaven, a **systemic object** is an object whose failure causes Deephaven to terminate the entire worker process. When systemic object marking is disabled (the default), all objects are treated as systemic and any object failure is fatal. When marking is enabled, threads are non-systemic by default—only objects created on explicitly marked threads or within a scoped execution are systemic. This guide explains how to control this behavior using the [`SystemicObjectTracker`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html) class.

## Enable systemic object marking

Systemic object marking is not user-configurable at runtime by default. To enable this feature, start the server with the JVM option:

```
-DSystemicObjectTracker.enabled=true
```

Check if systemic object marking is enabled:

```groovy test-set=1 order=null
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker

println SystemicObjectTracker.isSystemicObjectMarkingEnabled()
```

## Manage thread systemic status

Systemic object creation is controlled per thread. When systemic object marking is enabled, you can programmatically set a thread as systemic or non-systemic at runtime. While a thread is systemic, any object it creates is also systemic; when non-systemic, objects are not marked as systemic.

Check and toggle the systemic status of the current thread:

```groovy test-set=1 order=null
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker

if (SystemicObjectTracker.isSystemicObjectMarkingEnabled()) {
    if (SystemicObjectTracker.isSystemicThread()) {
        SystemicObjectTracker.markThreadNotSystemic()
    } else {
        SystemicObjectTracker.markThreadSystemic()
    }
} else {
    println "Systemic object marking is not currently enabled."
}
```

## Scoped execution

The [`SystemicObjectTracker`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html) class provides an `executeSystemically` method to enable or disable systemic object creation within a closure.

```groovy skip-test
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker

// Create a systemic table
systemicT = SystemicObjectTracker.executeSystemically(true, {
    emptyTable(1).update("Label=`I am systemic`")
})

// Create a non-systemic table
notSystemicT = SystemicObjectTracker.executeSystemically(false, {
    emptyTable(1).update("Label=`I am not systemic`")
})
```

## Related documentation

- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/util/systemicmarking/SystemicObjectTracker.html)
