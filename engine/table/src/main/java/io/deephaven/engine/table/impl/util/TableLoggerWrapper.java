package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.tablelogger.EngineTableLoggerProvider;
import io.deephaven.engine.tablelogger.impl.memory.MemoryTableLogger;

public class TableLoggerWrapper<T extends EngineTableLoggerProvider.EngineTableLogger> {
    private final T tableLogger;
    private final MemoryTableLogger memoryTableLogger;

    public TableLoggerWrapper(final T tableLogger) {
        this.tableLogger = tableLogger;
        memoryTableLogger = tableLogger instanceof MemoryTableLogger ? (MemoryTableLogger) tableLogger : null;
    }

    public T getTableLogger() {
        return tableLogger;
    }

    public QueryTable getQueryTable() {
        if (memoryTableLogger != null) {
            return memoryTableLogger.getQueryTable();
        } else {
            throw new UnsupportedOperationException("Only supported for memory table loggers.");
        }
    }

    public DynamicTableWriter getTableWriter() {
        if (memoryTableLogger != null) {
            return memoryTableLogger.getTableWriter();
        } else {
            throw new UnsupportedOperationException("Only supported for memory table loggers.");
        }
    }
}
