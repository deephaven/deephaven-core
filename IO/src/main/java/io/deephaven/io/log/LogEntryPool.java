//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.log;

import io.deephaven.base.pool.Pool;

public interface LogEntryPool extends Pool<LogEntry> {
    void shutdown();
}
