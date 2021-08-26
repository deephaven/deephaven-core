/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.db.tables.DataColumn;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.LiveQueryTable;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.SingleValueColumnSource;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/**
 * The DynamicTableWriter creates an in-memory table using ArrayBackedColumnSources of the type
 * specified in the constructor. You can retrieve the table using the {@code getTable} function.
 * <p>
 * This class is not thread safe, you must synchronize externally.
 */
public class DynamicTableWriter implements TableWriter {
    private final LiveQueryTable table;
    private final ArrayBackedColumnSource[] arrayColumnSources;

    private final String[] columnNames;
    private int allocatedSize;

    private final Map<String, IntFunction<RowSetterImpl>> factoryMap = new HashMap<>();
    private DynamicTableRow primaryRow;
    private int lastCommittedRow = -1;
    private int lastSetterRow;

    /**
     * Creates a TableWriter that produces an in-memory table using the provided column names and
     * types.
     *
     * @param columnNames the names of the columns in the output table (and our input)
     * @param columnTypes the types of the columns in the output table (must be compatible with the
     *        input)
     * @param constantValues a Map of columns with constant values
     */
    @SuppressWarnings("WeakerAccess")
    public DynamicTableWriter(final String[] columnNames, final Class<?>[] columnTypes,
        final Map<String, Object> constantValues) {
        final Map<String, ColumnSource> sources = new LinkedHashMap<>();
        arrayColumnSources = new ArrayBackedColumnSource[columnTypes.length];
        allocatedSize = 256;
        for (int i = 0; i < columnTypes.length; i++) {
            if (constantValues.containsKey(columnNames[i])) {
                final SingleValueColumnSource singleValueColumnSource =
                    SingleValueColumnSource.getSingleValueColumnSource(columnTypes[i]);
                // noinspection unchecked
                singleValueColumnSource.set(constantValues.get(columnNames[i]));
                sources.put(columnNames[i], singleValueColumnSource);
            } else {
                arrayColumnSources[i] =
                    ArrayBackedColumnSource.getMemoryColumnSource(allocatedSize, columnTypes[i]);
                sources.put(columnNames[i], arrayColumnSources[i]);
            }
        }


        this.table = new LiveQueryTable(Index.FACTORY.getIndexByValues(), sources);
        LiveTableMonitor.DEFAULT.addTable(table);
        this.columnNames = columnNames;
        final DataColumn[] columns = table.getColumns();
        for (int ii = 0; ii < columns.length; ii++) {
            if (constantValues.containsKey(columnNames[ii])) {
                continue;
            }
            final int index = ii;
            factoryMap.put(columns[index].getName(),
                (currentRow) -> createRowSetter(columns[index].getType(),
                    arrayColumnSources[index]));
        }
    }

    /**
     * Creates a TableWriter that produces an in-memory table using the provided column names and
     * types.
     *
     * @param columnNames the names of the columns in the output table (and our input)
     * @param columnTypes the types of the columns in the output table (must be compatible with the
     *        input)
     */
    public DynamicTableWriter(final String[] columnNames, final Class<?>[] columnTypes) {
        this(columnNames, columnTypes, Collections.emptyMap());
    }

    /**
     * Creates a write object that would write an object at a given location
     *
     * @param definition The table definition to create the dynamic table writer for
     */
    public DynamicTableWriter(@NotNull TableDefinition definition) {
        this(definition.getColumnNamesArray(), definition.getColumnTypesArray());
    }

    /**
     * Creates a write object that would write an object at a given location
     *
     * @param definition The table definition to create the dynamic table writer for
     */
    public DynamicTableWriter(TableDefinition definition, Map<String, Object> constantValues) {
        this(definition.getColumnNamesArray(), definition.getColumnTypesArray(), constantValues);
    }

    /**
     * Gets the table created by this DynamicTableWriter.
     * <p>
     * The returned table is registered with the LiveTableMonitor, and new rows become visible
     * within the refresh loop.
     *
     * @return a live table with the output of this log
     */
    public LiveQueryTable getTable() {
        return table;
    }

    /**
     * Returns a row writer, which allocates the row. You may get setters for the row, and then call
     * addRowToTableIndex when you are finished. Because the row is allocated when you call this
     * function, it is possible to get several Row objects before calling addRowToTableIndex.
     * <p>
     * This contrasts with {@code DynamicTableWriter.getSetter}, which allocates a single row; and
     * you must call {@code DynamicTableWriter.addRowToTableIndex} before advancing to the next row.
     *
     * @return a Row from which you can retrieve setters and call write row.
     */
    @Override
    public Row getRowWriter() {
        return new DynamicTableRow();
    }

    /**
     * Returns a RowSetter for the given column. If required, a Row object is allocated. You can not
     * mix calls with {@code getSetter} and {@code getRowWriter}. After setting each column, you
     * must call {@code addRowToTableIndex}, before beginning to write the next row.
     *
     * @param name column name.
     * @return a RowSetter for the given column
     */
    @Override
    public RowSetter getSetter(String name) {
        if (primaryRow == null) {
            primaryRow = new DynamicTableRow();
        }
        return primaryRow.getSetter(name);
    }

    @Override
    public void setFlags(Row.Flags flags) {
        if (primaryRow == null) {
            primaryRow = new DynamicTableRow();
        }
        primaryRow.setFlags(flags);
    }

    /**
     * Writes the current row created with the {@code getSetter} call, and advances the current row
     * by one.
     * <p>
     * The row will be made visible in the table after the LiveTableMonitor refresh cycle completes.
     */
    @Override
    public void writeRow() {
        Assert.neqNull(primaryRow, "primaryRow");
        primaryRow.writeRow();
    }

    private void addRangeToTableIndex(int first, int last) {
        table.addRange(first, last);
    }

    private void ensureCapacity(int row) {
        if (row < allocatedSize) {
            return;
        }

        int newSize = allocatedSize;
        while (row >= newSize) {
            newSize = 2 * newSize;
        }

        for (final ArrayBackedColumnSource arrayColumnSource : arrayColumnSources) {
            if (arrayColumnSource != null) {
                arrayColumnSource.ensureCapacity(newSize);
            }
        }

        allocatedSize = newSize;
    }

    /**
     * This is a convenience function so that you can log an entire row at a time using a Map. You
     * must specify all values in the setters map (and can't have any extras). The type of the value
     * must be castable to the type of the setter.
     *
     * @param values a map from column name to value for the row to be logged
     */
    @SuppressWarnings("unused")
    public void logRow(Map<String, Object> values) {
        if (values.size() != factoryMap.size()) {
            throw new RuntimeException(
                "Incompatible logRow call: " + values.keySet() + " != " + factoryMap.keySet());
        }
        for (final Map.Entry<String, Object> value : values.entrySet()) {
            // noinspection unchecked
            getSetter(value.getKey()).set(value.getValue());
        }
        writeRow();
        flush();
    }

    /**
     * This is a convenience function so that you can log an entire row at a time.
     *
     * @param values an array containing values to be logged, in order of the fields specified by
     *        the constructor
     */
    @SuppressWarnings("unused")
    public void logRow(Object... values) {
        if (values.length != factoryMap.size()) {
            throw new RuntimeException("Incompatible logRow call, values length=" + values.length
                + " != setters=" + factoryMap.size());
        }
        for (int ii = 0; ii < values.length; ++ii) {
            // noinspection unchecked
            getSetter(columnNames[ii]).set(values[ii]);
        }
        writeRow();
        flush();
    }

    @Override
    public void flush() {}


    @Override
    public long size() {
        return table.size();
    }

    @Override
    public void close() throws IOException {
        flush();
        table.close();
    }

    @Override
    public Class[] getColumnTypes() {
        return table.getDefinition().getColumnTypesArray();
    }

    @Override
    public String[] getColumnNames() {
        return columnNames;
    }


    private RowSetterImpl createRowSetter(Class type, ArrayBackedColumnSource buffer) {
        if (type == boolean.class || type == Boolean.class) {
            return new BooleanRowSetterImpl(buffer);
        } else if (type == byte.class || type == Byte.class) {
            return new ByteRowSetterImpl(buffer);
        } else if (type == char.class || type == Character.class) {
            return new CharRowSetterImpl(buffer);
        } else if (type == double.class || type == Double.class) {
            return new DoubleRowSetterImpl(buffer);
        } else if (type == float.class || type == Float.class) {
            return new FloatRowSetterImpl(buffer);
        } else if (type == int.class || type == Integer.class) {
            return new IntRowSetterImpl(buffer);
        } else if (type == long.class || type == Long.class) {
            return new LongRowSetterImpl(buffer);
        } else if (type == short.class || type == Short.class) {
            return new ShortRowSetterImpl(buffer);
        } else if (CharSequence.class.isAssignableFrom(type)) {
            return new StringRowSetterImpl(buffer);
        }
        return new ObjectRowSetterImpl(buffer, type);
    }

    private static abstract class RowSetterImpl implements RowSetter {
        protected final ArrayBackedColumnSource columnSource;
        protected int row;
        private final Class type;

        RowSetterImpl(ArrayBackedColumnSource columnSource, Class type) {
            this.columnSource = columnSource;
            this.type = type;
        }

        void setRow(int row) {
            this.row = row;
            writeToColumnSource();
        }

        abstract void writeToColumnSource();

        @Override
        public Class getType() {
            return type;
        }

        @Override
        public void set(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setBoolean(Boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setByte(byte value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setChar(char value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDouble(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setFloat(float value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setInt(int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLong(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setShort(short value) {
            throw new UnsupportedOperationException();
        }
    }

    private static class BooleanRowSetterImpl extends RowSetterImpl {
        BooleanRowSetterImpl(ArrayBackedColumnSource array) {
            super(array, Boolean.class);
        }

        Boolean pendingBoolean;

        @Override
        public void set(Object value) {
            setBoolean(value == null ? QueryConstants.NULL_BOOLEAN : (Boolean) value);
        }

        @Override
        public void setBoolean(Boolean value) {
            pendingBoolean = value;
        }

        @Override
        void writeToColumnSource() {
            // noinspection unchecked
            columnSource.set(row, pendingBoolean);
        }
    }

    private static class ByteRowSetterImpl extends RowSetterImpl {
        ByteRowSetterImpl(ArrayBackedColumnSource array) {
            super(array, byte.class);
        }

        byte pendingByte = QueryConstants.NULL_BYTE;

        @Override
        public void set(Object value) {
            setByte(value == null ? QueryConstants.NULL_BYTE : (Byte) value);
        }

        @Override
        public void setByte(byte value) {
            pendingByte = value;
        }

        @Override
        void writeToColumnSource() {
            columnSource.set(row, pendingByte);
        }
    }

    private static class CharRowSetterImpl extends RowSetterImpl {
        CharRowSetterImpl(ArrayBackedColumnSource array) {
            super(array, char.class);
        }

        char pendingChar = QueryConstants.NULL_CHAR;

        @Override
        public void set(Object value) {
            setChar(value == null ? QueryConstants.NULL_CHAR : (Character) value);
        }

        @Override
        public void setChar(char value) {
            pendingChar = value;
        }

        @Override
        void writeToColumnSource() {
            columnSource.set(row, pendingChar);
        }
    }

    private static class IntRowSetterImpl extends RowSetterImpl {
        IntRowSetterImpl(ArrayBackedColumnSource array) {
            super(array, int.class);
        }

        int pendingInt = QueryConstants.NULL_INT;

        @Override
        public void set(Object value) {
            setInt(value == null ? QueryConstants.NULL_INT : (Integer) value);
        }

        @Override
        public void setInt(int value) {
            pendingInt = value;
        }

        @Override
        void writeToColumnSource() {
            columnSource.set(row, pendingInt);
        }
    }

    private static class DoubleRowSetterImpl extends RowSetterImpl {
        DoubleRowSetterImpl(ArrayBackedColumnSource array) {
            super(array, double.class);
        }

        double pendingDouble = QueryConstants.NULL_DOUBLE;

        @Override
        public void set(Object value) {
            setDouble(value == null ? QueryConstants.NULL_DOUBLE : (Double) value);
        }

        @Override
        public void setDouble(double value) {
            pendingDouble = value;
        }

        @Override
        void writeToColumnSource() {
            columnSource.set(row, pendingDouble);
        }
    }

    private static class FloatRowSetterImpl extends RowSetterImpl {
        FloatRowSetterImpl(ArrayBackedColumnSource array) {
            super(array, float.class);
        }

        float pendingFloat = QueryConstants.NULL_FLOAT;

        @Override
        public void set(Object value) {
            setFloat(value == null ? QueryConstants.NULL_FLOAT : (Float) value);
        }

        @Override
        public void setFloat(float value) {
            pendingFloat = value;
        }

        @Override
        void writeToColumnSource() {
            columnSource.set(row, pendingFloat);
        }
    }

    private static class LongRowSetterImpl extends RowSetterImpl {
        LongRowSetterImpl(ArrayBackedColumnSource array) {
            super(array, long.class);
        }

        long pendingLong = QueryConstants.NULL_LONG;

        @Override
        public void set(Object value) {
            setLong(value == null ? QueryConstants.NULL_LONG : (Long) value);
        }

        @Override
        public void setLong(long value) {
            pendingLong = value;
        }

        @Override
        void writeToColumnSource() {
            columnSource.set(row, pendingLong);
        }
    }

    private static class ShortRowSetterImpl extends RowSetterImpl {
        ShortRowSetterImpl(ArrayBackedColumnSource array) {
            super(array, short.class);
        }

        short pendingShort = QueryConstants.NULL_SHORT;

        @Override
        public void set(Object value) {
            setShort(value == null ? QueryConstants.NULL_SHORT : (Short) value);
        }

        @Override
        public void setShort(short value) {
            pendingShort = value;
        }

        @Override
        void writeToColumnSource() {
            columnSource.set(row, pendingShort);
        }
    }

    private static class ObjectRowSetterImpl extends RowSetterImpl {
        ObjectRowSetterImpl(ArrayBackedColumnSource array, Class type) {
            super(array, type);
        }

        Object pendingObject;

        @Override
        public void set(Object value) {
            pendingObject = value;
        }

        @Override
        void writeToColumnSource() {
            // noinspection unchecked
            columnSource.set(row, pendingObject);
        }
    }

    private static class StringRowSetterImpl extends ObjectRowSetterImpl {
        StringRowSetterImpl(@NotNull final ArrayBackedColumnSource array) {
            super(array, String.class);
        }

        @Override
        public void set(final Object value) {
            super.set(value == null ? null : value.toString());
        }
    }

    private class DynamicTableRow implements Row {
        private int row = lastSetterRow;
        private final Map<String, RowSetterImpl> setterMap = factoryMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, (e) -> (e.getValue().apply(row))));
        private Row.Flags flags = Flags.SingleRow;

        @Override
        public RowSetter getSetter(final String name) {
            final RowSetter rowSetter = setterMap.get(name);
            if (rowSetter == null) {
                if (table.getColumnSourceMap().containsKey(name)) {
                    throw new RuntimeException(
                        "Column has a constant value, can not get setter " + name);
                } else {
                    throw new RuntimeException("Unknown column name " + name);
                }
            }
            return rowSetter;
        }

        @Override
        public void writeRow() {
            boolean doFlush = false;
            switch (flags) {
                case SingleRow:
                    doFlush = true;
                case StartTransaction:
                    if (lastCommittedRow != lastSetterRow) {
                        lastSetterRow = lastCommittedRow + 1;
                    }
                    break;
                case EndTransaction:
                    doFlush = true;
                    break;
                case None:
                    break;
            }
            row = lastSetterRow++;

            // Before this row can be returned to a pool, it needs to ensure that the underlying
            // sources
            // are appropriately sized to avoid race conditions.
            ensureCapacity(row);
            setterMap.values().forEach((x) -> x.setRow(row));

            // The row has been committed during set, we just need to insert the index into the
            // table
            if (doFlush) {
                DynamicTableWriter.this.addRangeToTableIndex(lastCommittedRow + 1, row);
                lastCommittedRow = row;
            }
        }

        @Override
        public long size() {
            return DynamicTableWriter.this.size();
        }

        @Override
        public void setFlags(Row.Flags flags) {
            this.flags = flags;
        }
    }
}
