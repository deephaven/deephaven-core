package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.tablelogger.UpdatePerformanceLogLogger;
import io.deephaven.io.logger.Logger;
import io.deephaven.tablelogger.TableLogger;
import io.deephaven.util.FunctionalInterfaces;

public interface TableLoggerWrapperUtility<T extends TableLogger> {
     T getTableLogger();

    default QueryTable getQueryTable() {
        throw new UnsupportedOperationException();
    }

    class Factory {
        private Factory() {
            throw new UnsupportedOperationException();
        }

        private static FunctionalInterfaces.TriFunction<Logger, UpdatePerformanceLogLogger, TableDefinition, TableLoggerWrapperUtility<UpdatePerformanceLogLogger>> createFunction;

        public static void setCreateFunction(FunctionalInterfaces.TriFunction<Logger, UpdatePerformanceLogLogger, TableDefinition, TableLoggerWrapperUtility<UpdatePerformanceLogLogger>> createFunction){
            Factory.createFunction = createFunction;
        }

        public static TableLoggerWrapperUtility<UpdatePerformanceLogLogger> create(Logger logger, UpdatePerformanceLogLogger uplLogger, TableDefinition tableDefinition) {
            return createFunction.apply(logger, uplLogger, tableDefinition);
        }
    }
}
