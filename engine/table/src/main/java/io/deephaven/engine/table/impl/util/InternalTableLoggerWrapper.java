package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.tablelogger.UpdatePerformanceLogLogger;
import io.deephaven.io.logger.Logger;
import io.deephaven.tablelogger.TableLogger;

public interface InternalTableLoggerWrapper<T extends TableLogger> {
    T getTableLogger();

    QueryTable getQueryTable();

    @FunctionalInterface
    interface Factory {
        InternalTableLoggerWrapper<UpdatePerformanceLogLogger> create(Logger logger, UpdatePerformanceLogLogger uplLogger, TableDefinition tableDefinition);
    }
}
