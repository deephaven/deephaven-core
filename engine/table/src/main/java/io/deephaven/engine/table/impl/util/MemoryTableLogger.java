/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.tablelogger.TableLogger;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;

public class MemoryTableLogger<T extends TableLogger> {
    private final DynamicTableWriter tableWriter;
    private final T tableLogger;
    private final TableDefinition tableDefinition;

    public MemoryTableLogger(@NotNull Logger logger, @NotNull T tableLogger, @NotNull TableDefinition tableDefinition,
            final int initialSizeArg) {
        this.tableLogger = tableLogger;
        this.tableDefinition = tableDefinition;

        final Class loggerClass = tableLogger.getClass();
        final int initialSize = (initialSizeArg == -1)
                ? Configuration.getInstance().getIntegerForClassWithDefault(
                        MemoryTableLogger.class,
                        loggerClass.getSimpleName() + ".logQueueSize",
                        10000)
                : initialSizeArg;
        try {
            tableWriter = new DynamicTableWriter(tableDefinition);
            tableLogger.init(tableWriter, initialSize);
        } catch (IOException e) {
            // If we can't get the table definition there's a real problem
            logger.error()
                    .append("Error creating in-memory performance logger for ")
                    .append(loggerClass.getSimpleName())
                    .append(":")
                    .append(e.toString())
                    .endl();
            throw new UncheckedIOException(e);
        }
    }

    public MemoryTableLogger(@NotNull Logger logger, @NotNull T tableLogger, @NotNull TableDefinition tableDefinition) {
        this(logger, tableLogger, tableDefinition, -1);
    }

    public T getTableLogger() {
        return tableLogger;
    }

    public DynamicTableWriter getTableWriter() {
        return tableWriter;
    }

    public QueryTable getQueryTable() {
        return tableWriter.getTable();
    }
}
