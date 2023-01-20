package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.tablelogger.TableLogger;

public interface TableLoggerWrapperUtility<T extends TableLogger> {
     T getTableLogger();

    default QueryTable getQueryTable() {
        throw new UnsupportedOperationException();
    };
}
