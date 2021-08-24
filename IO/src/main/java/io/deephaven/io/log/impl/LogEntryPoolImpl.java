/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.Function;
import io.deephaven.base.pool.ThreadSafeFixedSizePool;
import io.deephaven.io.log.LogBufferPool;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogEntryPool;

public class LogEntryPoolImpl extends ThreadSafeFixedSizePool<LogEntry> implements LogEntryPool {

    public LogEntryPoolImpl(int entryCount, final LogBufferPool bufferPool) {
        super(entryCount, new Function.Nullary<LogEntry>() {
            public LogEntry call() {
                return new LogEntryImpl(bufferPool);
            }
        }, null);
    }

    @Override
    public void shutdown() {

    }
}
