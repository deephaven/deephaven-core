package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.tablelogger.EngineTableLoggerProvider;
import io.deephaven.engine.tablelogger.impl.memory.MemoryTableLogger;

/**
 * This class is an artifact of DHC reliance on the memory table logger implementation before engine table loggers were generalized. It handles runtime type checking for clients and throws {@link UnsupportedOperationException} where the previous memory table logger coupling fails.
 * @param <T> Engine table logger type in which can instance can be retrieved
 */
public class MemoryTableLoggerWrapper<T extends EngineTableLoggerProvider.EngineTableLogger> {
    private final T tableLogger;
    private final MemoryTableLogger memoryTableLogger;

    public MemoryTableLoggerWrapper(final T tableLogger) {
        this.tableLogger = tableLogger;
        memoryTableLogger = tableLogger instanceof MemoryTableLogger ? (MemoryTableLogger) tableLogger : null;
    }

    public T getTableLogger() {
        return tableLogger;
    }

    public DynamicTableWriter getTableWriter() {
        if (memoryTableLogger != null) {
            return memoryTableLogger.getTableWriter();
        } else {
            throw new UnsupportedOperationException("Only supported for memory table loggers.");
        }
    }

    public QueryTable getQueryTable() {
        if (memoryTableLogger != null) {
            return memoryTableLogger.getQueryTable();
        } else {
            throw new UnsupportedOperationException("Only supported for memory table loggers.");
        }
    }
}
