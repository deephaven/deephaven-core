//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.log.impl;

import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogEntryPool;
import io.deephaven.io.logger.LoggerTimeSource;

public class DynamicDelayedLogEntryPoolImpl extends ThreadSafeLenientFixedSizePool<LogEntry> implements LogEntryPool {

    public DynamicDelayedLogEntryPoolImpl(String name, int entryCount, final LoggerTimeSource timeSource) {
        super(name, entryCount, () -> new DelayedLogEntryImpl(timeSource), null);
    }

    @Override
    public void shutdown() {

    }
}
