/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tablelogger;

import io.deephaven.db.tables.TableDefinition;
import io.deephaven.tablelogger.*;
import io.deephaven.db.tables.utils.ColumnsSpecHelper;

import java.io.IOException;

public class ProcessInfoLogLogger extends TableLoggerImpl2<ProcessInfoLogLogger.ISetter> {

    private static final String TABLE_NAME = "ProcessInfoLog";

    public ProcessInfoLogLogger() {
        super(TABLE_NAME);
    }

    public static String getDefaultTableName() {
        return TABLE_NAME;
    }

    interface ISetter extends WritableRowContainer {
        void log(Row.Flags flags, String id, String type, String key, String value) throws java.io.IOException;
    }

    class DirectSetter extends BaseSetter implements ISetter {
        RowSetter<String> Id;
        RowSetter<String> Type;
        RowSetter<String> Key;
        RowSetter<String> Value;

        DirectSetter() {
            Id = row.getSetter("Id", String.class);
            Type = row.getSetter("Type", String.class);
            Key = row.getSetter("Key", String.class);
            Value = row.getSetter("Value", String.class);
        }

        @Override
        public void log(Row.Flags flags, String id, String type, String key, String value) throws java.io.IOException {
            setRowFlags(flags);
            this.Id.set(id);
            this.Type.set(type);
            this.Key.set(key);
            this.Value.set(value);
        }
    }

    @Override
    protected String threadName() {
        return TABLE_NAME;
    }

    private static final String[] columnNames;
    private static final Class<?>[] columnDbTypes;

    static {
        final ColumnsSpecHelper cols = new ColumnsSpecHelper()
                .add("Id", String.class)
                .add("Type", String.class)
                .add("Key", String.class)
                .add("Value", String.class);
        columnNames = cols.getColumnNames();
        columnDbTypes = cols.getDbTypes();
    }

    @Override
    protected ISetter createSetter() {
        outstandingSetters.getAndIncrement();
        return new DirectSetter();
    }

    public void log(final String id, final String type, final String key, final String value) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, id, type, key, value);
    }

    public void log(
            final Row.Flags flags, final String id, final String type, final String key, final String value)
            throws IOException {
        verifyCondition(isInitialized(), "init() must be called before calling log()");
        verifyCondition(!isClosed, "cannot call log() after the logger is closed");
        verifyCondition(!isShuttingDown, "cannot call log() while the logger is shutting down");
        final ISetter setter = setterPool.take();
        try {
            setter.log(flags, id, type, key, value);
        } catch (Exception e) {
            setterPool.give(setter);
            throw e;
        }
        flush(setter);
    }

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.tableDefinition(columnDbTypes, columnNames);

    public static TableDefinition getTableDefinition() {
        return TABLE_DEFINITION;
    }

    public static String[] getColumnNames() {
        return columnNames;
    }
}
