package io.deephaven.engine.tablelogger;

import io.deephaven.tablelogger.Row;

import java.io.IOException;

public interface ServerStateLog {
    void log(final long intervalStartTime, final int intervalDurationMicros, final int totalMemoryMiB, final int freeMemoryMiB, final short intervalCollections, final int intervalCollectionTimeMicros, final short intervalUGPCyclesOnBudget, final int[] intervalUGPCyclesTimeMicros, final short intervalUGPCyclesSafePoints, final int intervalUGPCyclesSafePointTimeMicros) throws IOException;

    void log(final Row.Flags flags, final long intervalStartTime, final int intervalDurationMicros, final int totalMemoryMiB, final int freeMemoryMiB, final short intervalCollections, final int intervalCollectionTimeMicros, final short intervalUGPCyclesOnBudget, final int[] intervalUGPCyclesTimeMicros, final short intervalUGPCyclesSafePoints, final int intervalUGPCyclesSafePointTimeMicros) throws IOException;
}
