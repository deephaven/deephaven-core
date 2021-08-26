/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.Function;
import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogEntryPool;
import io.deephaven.io.logger.LoggerTimeSource;

public class DynamicDelayedLogEntryPoolImpl extends ThreadSafeLenientFixedSizePool<LogEntry> implements LogEntryPool {
    private final LoggerTimeSource timeSource;

    public DynamicDelayedLogEntryPoolImpl(String name, int entryCount, final LoggerTimeSource timeSource) {
        super(name, entryCount,
                new Function.Nullary<LogEntry>() {
                    public LogEntry call() {
                        return new DelayedLogEntryImpl(timeSource);
                    }
                }, null);
        this.timeSource = timeSource;
    }

    @Override
    public void shutdown() {

    }
}
