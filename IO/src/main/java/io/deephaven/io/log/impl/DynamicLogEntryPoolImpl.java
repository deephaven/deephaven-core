/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;
import io.deephaven.io.log.LogEntryPool;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogBufferPool;
import io.deephaven.base.Function;

public class DynamicLogEntryPoolImpl extends ThreadSafeLenientFixedSizePool<LogEntry>
    implements LogEntryPool {
    public DynamicLogEntryPoolImpl(String name, int entryCount, final LogBufferPool bufferPool) {
        super(name, entryCount,
            new Function.Nullary<LogEntry>() {
                public LogEntry call() {
                    return new LogEntryImpl(bufferPool);
                }
            }, null);
    }

    @Override
    public void shutdown() {

    }
}
