//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.log.impl;

import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;
import io.deephaven.io.log.LogEntryPool;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogBufferPool;

public class DynamicLogEntryPoolImpl extends ThreadSafeLenientFixedSizePool<LogEntry> implements LogEntryPool {

    public DynamicLogEntryPoolImpl(String name, int entryCount, final LogBufferPool bufferPool) {
        super(name, entryCount, () -> new LogEntryImpl(bufferPool), null);
    }

    @Override
    public void shutdown() {

    }
}
