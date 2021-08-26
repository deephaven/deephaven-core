/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.Function;
import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogEntryPool;

public class DynamicDelayedLogEntryUnsafePoolImpl extends ThreadSafeLenientFixedSizePool<LogEntry>
        implements LogEntryPool {

    public DynamicDelayedLogEntryUnsafePoolImpl(String name, int entryCount) {
        super(name, entryCount,
                new Function.Nullary<LogEntry>() {
                    public LogEntry call() {
                        return new DelayedLogEntryUnsafeImpl();
                    }
                }, null);
    }

    @Override
    public void shutdown() {
        LogEntry entry;
        while ((entry = takeMaybeNull()) != null) {
            ((DelayedLogEntryUnsafeImpl) entry).free();
        }
    }

    @Override
    public void give(LogEntry item) {
        final boolean gived = giveInternal(item);
        if (!gived && item != null) {
            ((DelayedLogEntryUnsafeImpl) item).free();
        }
    }
}
