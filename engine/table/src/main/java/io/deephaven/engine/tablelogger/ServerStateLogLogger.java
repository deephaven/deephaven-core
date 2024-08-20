//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.Row.Flags;

import java.io.IOException;

import static io.deephaven.tablelogger.TableLogger.DEFAULT_INTRADAY_LOGGER_FLAGS;

/**
 * Logs data that describes the top-level view of the free and total memory available to the process.
 */
public interface ServerStateLogLogger {
    default void log(final long intervalStartTime, final int intervalDurationMicros, final int totalMemoryMiB,
            final int freeMemoryMiB, final short intervalCollections, final int intervalCollectionTimeMicros,
            final short intervalUGPCyclesOnBudget, final int[] intervalUGPCyclesTimeMicros,
            final short intervalUGPCyclesSafePoints, final int intervalUGPCyclesSafePointTimeMicros)
            throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, intervalStartTime, intervalDurationMicros, totalMemoryMiB, freeMemoryMiB,
                intervalCollections, intervalCollectionTimeMicros, intervalUGPCyclesOnBudget,
                intervalUGPCyclesTimeMicros, intervalUGPCyclesSafePoints, intervalUGPCyclesSafePointTimeMicros);
    }

    void log(final Row.Flags flags, final long intervalStartTime, final int intervalDurationMicros,
            final int totalMemoryMiB, final int freeMemoryMiB, final short intervalCollections,
            final int intervalCollectionTimeMicros, final short intervalUGPCyclesOnBudget,
            final int[] intervalUGPCyclesTimeMicros, final short intervalUGPCyclesSafePoints,
            final int intervalUGPCyclesSafePointTimeMicros) throws IOException;

    enum Noop implements ServerStateLogLogger {
        INSTANCE;

        @Override
        public void log(Flags flags, long intervalStartTime, int intervalDurationMicros, int totalMemoryMiB,
                int freeMemoryMiB, short intervalCollections, int intervalCollectionTimeMicros,
                short intervalUGPCyclesOnBudget, int[] intervalUGPCyclesTimeMicros, short intervalUGPCyclesSafePoints,
                int intervalUGPCyclesSafePointTimeMicros) throws IOException {

        }
    }
}
