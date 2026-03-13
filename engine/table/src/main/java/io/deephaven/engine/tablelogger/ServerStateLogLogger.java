//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import java.io.Closeable;
import java.io.IOException;

/**
 * Logs data that describes the top-level view of the free and total memory available to the process.
 */
public interface ServerStateLogLogger extends Closeable {
    void log(final long intervalStartTime, final int intervalDurationMicros, final int totalMemoryMiB,
            final int freeMemoryMiB, final short intervalCollections, final int intervalCollectionTimeMicros,
            final short intervalUGPCyclesOnBudget, final int[] intervalUGPCyclesTimeMicros,
            final short intervalUGPCyclesSafePoints, final int intervalUGPCyclesSafePointTimeMicros)
            throws IOException;

    enum Noop implements ServerStateLogLogger {
        INSTANCE;

        @Override
        public void log(long intervalStartTime, int intervalDurationMicros, int totalMemoryMiB, int freeMemoryMiB,
                short intervalCollections, int intervalCollectionTimeMicros, short intervalUGPCyclesOnBudget,
                int[] intervalUGPCyclesTimeMicros, short intervalUGPCyclesSafePoints,
                int intervalUGPCyclesSafePointTimeMicros) throws IOException {

        }

        @Override
        public void close() throws IOException {

        }
    }
}
