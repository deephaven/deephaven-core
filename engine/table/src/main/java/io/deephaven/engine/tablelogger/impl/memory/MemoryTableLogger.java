//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger.impl.memory;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.util.DynamicTableWriter;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.tablelogger.TableLoggerImpl2;
import io.deephaven.tablelogger.WritableRowContainer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Base class for memory table loggers that create and initialize a {@link DynamicTableWriter}.
 *
 * <p>
 * Deprecated: prefer constructions using blink tables, see {@link io.deephaven.stream.StreamToBlinkTableAdapter}.
 */
@Deprecated(since = "0.26.0", forRemoval = true)
public abstract class MemoryTableLogger<T extends WritableRowContainer> extends TableLoggerImpl2<T> {
    @NotNull
    public static QueryTable maybeGetQueryTable(final Object maybeMemoryTableLogger) {
        if (maybeMemoryTableLogger instanceof MemoryTableLogger) {
            return ((MemoryTableLogger) maybeMemoryTableLogger).getQueryTable();
        }

        throw new UnsupportedOperationException("Only supported for memory table loggers.");
    }

    private final DynamicTableWriter tableWriter;

    protected MemoryTableLogger(final String tableName, final TableDefinition tableDefinition,
            final int initialSizeArg) {
        super(tableName);

        final Class loggerClass = this.getClass();
        final int initialSize = (initialSizeArg == -1)
                ? Configuration.getInstance().getIntegerForClassWithDefault(
                        MemoryTableLogger.class,
                        loggerClass.getSimpleName() + ".logQueueSize",
                        10000)
                : initialSizeArg;
        try {
            tableWriter = new DynamicTableWriter(tableDefinition);
            init(tableWriter, initialSize);
        } catch (IOException e) {
            final Logger logger = LoggerFactory.getLogger(loggerClass);
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

    protected MemoryTableLogger(final String tableName, final TableDefinition tableDefinition) {
        this(tableName, tableDefinition, -1);
    }

    public DynamicTableWriter getTableWriter() {
        return tableWriter;
    }

    public QueryTable getQueryTable() {
        return tableWriter.getTable();
    }
}
