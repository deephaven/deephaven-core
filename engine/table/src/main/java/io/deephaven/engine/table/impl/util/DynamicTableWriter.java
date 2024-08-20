//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.type.Type;
import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.table.impl.UpdateSourceQueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/**
 * The DynamicTableWriter creates an in-memory table using ArrayBackedColumnSources of the type specified in the
 * constructor. You can retrieve the table using the {@code getTable} function.
 * <p>
 * This class is not thread safe, you must synchronize externally. However, multiple setters may safely log
 * concurrently.
 *
 * @implNote The constructor publishes {@code this} to the {@link UpdateGraph} and thus cannot be subclassed.
 */
public final class DynamicTableWriter implements TableWriter {
    private final UpdateSourceQueryTable table;
    private final WritableColumnSource[] arrayColumnSources;

    private final String[] columnNames;
    private int allocatedSize;

    private final Map<String, IntFunction<RowSetterImpl>> factoryMap = new HashMap<>();
    private DynamicTableRow primaryRow;
    private int lastCommittedRow = -1;
    private int lastSetterRow;

    /**
     * Creates a TableWriter that produces an in-memory table using the provided column names and types.
     *
     * @param header the names and types of the columns in the output table (and our input)
     * @param constantValues a Map of columns with constant values
     */
    public DynamicTableWriter(final TableHeader header, final Map<String, Object> constantValues) {
        this(getSources(header, constantValues, 256), constantValues, 256);
    }

    /**
     * Creates a TableWriter that produces an in-memory table using the provided column names and types.
     *
     * @param header the names and types of the columns in the output table (and our input)
     */
    public DynamicTableWriter(final TableHeader header) {
        this(header, Collections.emptyMap());
    }

    // This constructor is no longer public to simplify access from python: jpy cannot resolve
    // calls with arguments of list type when there is more than one alternative with array element type
    // on the java side. Prefer the constructor taking qst.table.TableHeader or an array of qst.type.Type
    // objects.
    @SuppressWarnings("WeakerAccess")
    DynamicTableWriter(
            final String[] columnNames,
            final Class<?>[] columnTypes,
            final Map<String, Object> constantValues) {
        this(columnNames, (int i) -> columnTypes[i], constantValues);
    }

    /**
     * Creates a TableWriter that produces an in-memory table using the provided column names and types.
     *
     * @param columnNames the names of the columns in the output table (and our input)
     * @param columnTypes the types of the columns in the output table (must be compatible with the input)
     * @param constantValues a Map of columns with constant values
     */
    @SuppressWarnings("WeakerAccess")
    public DynamicTableWriter(
            final String[] columnNames,
            final Type<?>[] columnTypes,
            final Map<String, Object> constantValues) {
        this(columnNames, (int i) -> columnTypes[i].clazz(), constantValues);
    }

    // This constructor is no longer public to simplify access from python: jpy cannot resolve
    // calls with arguments of list type when there is more than one alternative with array element type
    // on the java side. Prefer the constructor taking qst.table.TableHeader or an array of qst.type.Type
    // objects.
    DynamicTableWriter(final String[] columnNames, final Class<?>[] columnTypes) {
        this(columnNames, columnTypes, Collections.emptyMap());
    }

    /**
     * Creates a TableWriter that produces an in-memory table using the provided column names and types.
     *
     * @param columnNames the names of the columns in the output table (and our input)
     * @param columnTypes the types of the columns in the output table (must be compatible with the input)
     */
    public DynamicTableWriter(final String[] columnNames, final Type<?>[] columnTypes) {
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
     * The returned table is registered with the PeriodicUpdateGraph, and new rows become visible within the run loop.
     *
     * @return a live table with the output of this log
     */
    public UpdateSourceQueryTable getTable() {
        return table;
    }

    /**
     * Returns a row writer, which allocates the row. You may get setters for the row, and then call addRowToTableIndex
     * when you are finished. Because the row is allocated when you call this function, it is possible to get several
     * Row objects before calling addRowToTableIndex.
     * <p>
     * This contrasts with {@code DynamicTableWriter.getSetter}, which allocates a single row; and you must call
     * {@code DynamicTableWriter.addRowToTableIndex} before advancing to the next row.
     *
     * @return a Row from which you can retrieve setters and call write row.
     */
    @Override
    public Row getRowWriter() {
        return new DynamicTableRow();
    }

    /**
     * Returns a RowSetter for the given column. If required, a Row object is allocated. You can not mix calls with
     * {@code getSetter} and {@code getRowWriter}. After setting each column, you must call {@code addRowToTableIndex},
     * before beginning to write the next row.
     *
     * @param name column name.
     * @return a RowSetter for the given column
     */
    @Override
    public PermissiveRowSetter getSetter(String name) {
        if (primaryRow == null) {
            primaryRow = new DynamicTableRow();
        }
        return primaryRow.getSetter(name);
    }

    private RowSetterImpl getSetter(final int columnIndex) {
        if (primaryRow == null) {
            primaryRow = new DynamicTableRow();
        }
        return primaryRow.setters[columnIndex];
    }

    @Override
    public void setFlags(Row.Flags flags) {
        if (primaryRow == null) {
            primaryRow = new DynamicTableRow();
        }
        primaryRow.setFlags(flags);
    }

    /**
     * Writes the current row created with the {@code getSetter} call, and advances the current row by one.
     * <p>
     * The row will be made visible in the table after the PeriodicUpdateGraph run cycle completes.
     */
    @Override
    public void writeRow() {
        Assert.neqNull(primaryRow, "primaryRow");
        primaryRow.writeRow();
    }

    private void addRangeToTableIndex(int first, int last) {
        table.addRowKeyRange(first, last);
    }

    private void ensureCapacity(int row) {
        if (row < allocatedSize) {
            return;
        }

        int newSize = allocatedSize;
        while (row >= newSize) {
            newSize = 2 * newSize;
        }

        for (final WritableColumnSource arrayColumnSource : arrayColumnSources) {
            if (arrayColumnSource != null) {
                arrayColumnSource.ensureCapacity(newSize);
            }
        }

        allocatedSize = newSize;
    }

    /**
     * This is a convenience function so that you can log an entire row at a time using a Map. You must specify all
     * values in the setters map (and can't have any extras). The type of the value must be castable to the type of the
     * setter.
     *
     * @param values a map from column name to value for the row to be logged
     */
    @SuppressWarnings("unused")
    public void logRow(Map<String, Object> values) {
        if (values.size() != factoryMap.size()) {
            throw new RuntimeException("Incompatible logRow call: " + values.keySet() + " != " + factoryMap.keySet());
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
     * @param values an array containing values to be logged, in order of the fields specified by the constructor
     */
    @SuppressWarnings("unused")
    public void logRow(Object... values) {
        if (values.length != factoryMap.size()) {
            throw new RuntimeException(
                    "Incompatible logRow call, values length=" + values.length + " != setters=" + factoryMap.size());
        }
        for (int ii = 0; ii < values.length; ++ii) {
            // noinspection unchecked
            getSetter(ii).set(values[ii]);
        }
        writeRow();
        flush();
    }

    /**
     * This is a convenience function so that you can log an entire row at a time using a Map. You must specify all
     * values in the setters map (and can't have any extras). The type of the value must be convertible (safely or
     * unsafely) to the type of the permissive setter.
     *
     * @param values a map from column name to value for the row to be logged
     */
    @SuppressWarnings("unused")
    public void logRowPermissive(Map<String, Object> values) {
        if (values.size() != factoryMap.size()) {
            throw new RuntimeException(
                    "Incompatible logRowPermissive call: " + values.keySet() + " != " + factoryMap.keySet());
        }
        for (final Map.Entry<String, Object> value : values.entrySet()) {
            getSetter(value.getKey()).setPermissive(value.getValue());
        }
        writeRow();
        flush();
    }

    /**
     * This is a convenience function so that you can log an entire row at a time. You must specify all values in the
     * setters map (and can't have any extras). The type of the value must be convertible (safely or unsafely) to the
     * type of the permissive setter.
     *
     * @param values an array containing values to be logged, in order of the fields specified by the constructor
     */
    @SuppressWarnings("unused")
    public void logRowPermissive(Object... values) {
        if (values.length != factoryMap.size()) {
            throw new RuntimeException(
                    "Incompatible logRowPermissive call, values length=" + values.length + " != setters="
                            + factoryMap.size());
        }
        for (int ii = 0; ii < values.length; ++ii) {
            getSetter(ii).setPermissive(values[ii]);
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

    private static Map<String, ColumnSource<?>> getSources(
            final TableHeader header,
            final Map<String, Object> constantValues,
            final int allocatedSize) {
        final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>();
        final Iterator<ColumnHeader<?>> it = header.iterator();
        while (it.hasNext()) {
            final ColumnHeader<?> colHeader = it.next();
            final String colName = colHeader.name();
            final Class<?> colType = colHeader.componentType().clazz();
            if (constantValues.containsKey(colName)) {
                final SingleValueColumnSource singleValueColumnSource =
                        SingleValueColumnSource.getSingleValueColumnSource(colType);
                // noinspection unchecked
                singleValueColumnSource.set(constantValues.get(colName));
                sources.put(colName, singleValueColumnSource);
            } else {
                ColumnSource<?> source =
                        ArrayBackedColumnSource.getMemoryColumnSource(allocatedSize, colType);
                sources.put(colName, source);
            }
        }
        return sources;
    }

    private static Map<String, ColumnSource<?>> getSources(
            final String[] columnNames,
            final IntFunction<Class<?>> columnTypes,
            final Map<String, Object> constantValues,
            final int allocatedSize) {

        final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            if (constantValues.containsKey(columnNames[i])) {
                final SingleValueColumnSource singleValueColumnSource =
                        SingleValueColumnSource.getSingleValueColumnSource(columnTypes.apply(i));
                // noinspection unchecked
                singleValueColumnSource.set(constantValues.get(columnNames[i]));
                sources.put(columnNames[i], singleValueColumnSource);
            } else {
                WritableColumnSource<?> source =
                        ArrayBackedColumnSource.getMemoryColumnSource(allocatedSize,
                                columnTypes.apply(i));
                sources.put(columnNames[i], source);
            }
        }
        return sources;
    }

    // Convenience implementation method.
    private DynamicTableWriter(
            final String[] columnNames,
            final IntFunction<Class<?>> columnTypes,
            final Map<String, Object> constantValues) {
        this(getSources(columnNames, columnTypes, constantValues, 256), constantValues, 256);
    }

    private DynamicTableWriter(final Map<String, ColumnSource<?>> sources, final Map<String, Object> constantValues,
            final int allocatedSize) {
        this.allocatedSize = allocatedSize;
        table = new UpdateSourceQueryTable(RowSetFactory.fromKeys().toTracking(), sources);
        table.setFlat();
        table.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
        table.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);

        final int nCols = sources.size();;
        this.columnNames = new String[nCols];
        this.arrayColumnSources = new WritableColumnSource[nCols];
        int ii = 0;
        for (Map.Entry<String, ColumnSource<?>> entry : sources.entrySet()) {
            final String columnName = columnNames[ii] = entry.getKey();
            final ColumnSource<?> source = entry.getValue();
            if (constantValues.containsKey(columnName)) {
                continue;
            }
            if (source instanceof WritableColumnSource) {
                arrayColumnSources[ii] = (WritableColumnSource) source;
            } else {
                throw new IllegalStateException(
                        "Expected ArrayBackedColumnSource, instead found " + source.getClass());
            }
            factoryMap.put(columnName,
                    (currentRow) -> createRowSetter(source.getType(), (WritableColumnSource) source));
            ++ii;
        }
        UpdateGraph updateGraph = table.getUpdateGraph();
        updateGraph.addSource(table);
    }

    @SuppressWarnings("unchecked")
    private <T> RowSetterImpl<T> createRowSetter(Class<T> type, WritableColumnSource<T> buffer) {
        final RowSetterImpl<?> result;
        if (type == boolean.class || type == Boolean.class) {
            result = new BooleanRowSetterImpl((WritableColumnSource<Boolean>) buffer);
        } else if (type == byte.class || type == Byte.class) {
            result = new ByteRowSetterImpl((WritableColumnSource<Byte>) buffer);
        } else if (type == char.class || type == Character.class) {
            result = new CharRowSetterImpl((WritableColumnSource<Character>) buffer);
        } else if (type == double.class || type == Double.class) {
            result = new DoubleRowSetterImpl((WritableColumnSource<Double>) buffer);
        } else if (type == float.class || type == Float.class) {
            result = new FloatRowSetterImpl((WritableColumnSource<Float>) buffer);
        } else if (type == int.class || type == Integer.class) {
            result = new IntRowSetterImpl((WritableColumnSource<Integer>) buffer);
        } else if (type == long.class || type == Long.class) {
            result = new LongRowSetterImpl((WritableColumnSource<Long>) buffer);
        } else if (type == short.class || type == Short.class) {
            result = new ShortRowSetterImpl((WritableColumnSource<Short>) buffer);
        } else if (CharSequence.class.isAssignableFrom(type)) {
            result = new StringRowSetterImpl((WritableColumnSource<String>) buffer);
        } else {
            result = new ObjectRowSetterImpl<>(buffer, type);
        }

        return (RowSetterImpl<T>) result;
    }

    public interface PermissiveRowSetter<T> extends RowSetter<T> {
        void setPermissive(Object value);
    }

    private static abstract class RowSetterImpl<T> implements PermissiveRowSetter<T> {
        protected final WritableColumnSource<T> columnSource;
        protected int row;
        private final Class<T> type;

        RowSetterImpl(WritableColumnSource<T> columnSource, Class<T> type) {
            this.columnSource = columnSource;
            this.type = type;
        }

        void setRow(int row) {
            this.row = row;
            writeToColumnSource();
        }

        abstract void writeToColumnSource();

        @Override
        public Class<T> getType() {
            return type;
        }

        @Override
        public void set(T value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setPermissive(Object value) {
            // noinspection unchecked
            set((T) value);
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

    private static class BooleanRowSetterImpl extends RowSetterImpl<Boolean> {
        BooleanRowSetterImpl(WritableColumnSource<Boolean> array) {
            super(array, Boolean.class);
        }

        Boolean pendingBoolean;

        @Override
        public void set(Boolean value) {
            setBoolean(value == null ? QueryConstants.NULL_BOOLEAN : value);
        }

        @Override
        public void setBoolean(Boolean value) {
            pendingBoolean = value;
        }

        @Override
        void writeToColumnSource() {
            columnSource.set(row, pendingBoolean);
        }
    }

    private static class ByteRowSetterImpl extends RowSetterImpl<Byte> {
        ByteRowSetterImpl(WritableColumnSource<Byte> array) {
            super(array, byte.class);
        }

        byte pendingByte = QueryConstants.NULL_BYTE;

        @Override
        public void set(Byte value) {
            setByte(value == null ? QueryConstants.NULL_BYTE : value);
        }

        @Override
        public void setPermissive(Object value) {
            setByte(value == null ? QueryConstants.NULL_BYTE : ((Number) value).byteValue());
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

    private static class CharRowSetterImpl extends RowSetterImpl<Character> {
        CharRowSetterImpl(WritableColumnSource<Character> array) {
            super(array, char.class);
        }

        char pendingChar = QueryConstants.NULL_CHAR;

        @Override
        public void set(Character value) {
            setChar(value == null ? QueryConstants.NULL_CHAR : value);
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

    private static class IntRowSetterImpl extends RowSetterImpl<Integer> {
        IntRowSetterImpl(WritableColumnSource<Integer> array) {
            super(array, int.class);
        }

        int pendingInt = QueryConstants.NULL_INT;

        @Override
        public void set(Integer value) {
            setInt(value == null ? QueryConstants.NULL_INT : value);
        }

        @Override
        public void setPermissive(Object value) {
            setInt(value == null ? QueryConstants.NULL_INT : ((Number) value).intValue());
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

    private static class DoubleRowSetterImpl extends RowSetterImpl<Double> {
        DoubleRowSetterImpl(WritableColumnSource<Double> array) {
            super(array, double.class);
        }

        double pendingDouble = QueryConstants.NULL_DOUBLE;

        @Override
        public void set(Double value) {
            setDouble(value == null ? QueryConstants.NULL_DOUBLE : value);
        }

        @Override
        public void setPermissive(Object value) {
            setDouble(value == null ? QueryConstants.NULL_DOUBLE : ((Number) value).doubleValue());
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

    private static class FloatRowSetterImpl extends RowSetterImpl<Float> {
        FloatRowSetterImpl(WritableColumnSource<Float> array) {
            super(array, float.class);
        }

        float pendingFloat = QueryConstants.NULL_FLOAT;

        @Override
        public void set(Float value) {
            setFloat(value == null ? QueryConstants.NULL_FLOAT : value);
        }

        @Override
        public void setPermissive(Object value) {
            setFloat(value == null ? QueryConstants.NULL_FLOAT : ((Number) value).floatValue());
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

    private static class LongRowSetterImpl extends RowSetterImpl<Long> {
        LongRowSetterImpl(WritableColumnSource<Long> array) {
            super(array, long.class);
        }

        long pendingLong = QueryConstants.NULL_LONG;

        @Override
        public void set(Long value) {
            setLong(value == null ? QueryConstants.NULL_LONG : value);
        }

        @Override
        public void setPermissive(Object value) {
            setLong(value == null ? QueryConstants.NULL_LONG : ((Number) value).longValue());
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

    private static class ShortRowSetterImpl extends RowSetterImpl<Short> {
        ShortRowSetterImpl(WritableColumnSource<Short> array) {
            super(array, short.class);
        }

        short pendingShort = QueryConstants.NULL_SHORT;

        @Override
        public void set(Short value) {
            setShort(value == null ? QueryConstants.NULL_SHORT : value);
        }

        @Override
        public void setPermissive(Object value) {
            setShort(value == null ? QueryConstants.NULL_SHORT : ((Number) value).shortValue());
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

    private static class ObjectRowSetterImpl<T> extends RowSetterImpl<T> {
        ObjectRowSetterImpl(WritableColumnSource<T> array, Class<T> type) {
            super(array, type);
        }

        T pendingObject;

        @Override
        public void set(T value) {
            pendingObject = value;
        }

        @Override
        void writeToColumnSource() {
            columnSource.set(row, pendingObject);
        }
    }

    private static class StringRowSetterImpl extends ObjectRowSetterImpl<String> {
        StringRowSetterImpl(@NotNull final WritableColumnSource<String> array) {
            super(array, String.class);
        }
    }

    private class DynamicTableRow implements Row {
        private final RowSetterImpl<?>[] setters;
        private final Map<String, RowSetterImpl<?>> columnToSetter;
        private int row = lastSetterRow;
        private Row.Flags flags = Flags.SingleRow;

        private DynamicTableRow() {
            setters = Arrays.stream(columnNames).map(cn -> factoryMap.get(cn).apply(row)).toArray(RowSetterImpl[]::new);
            final MutableInt ci = new MutableInt(0);
            columnToSetter = Arrays.stream(columnNames)
                    .collect(Collectors.toMap(Function.identity(), cn -> setters[ci.getAndIncrement()]));
        }

        @Override
        public PermissiveRowSetter<?> getSetter(final String name) {
            final PermissiveRowSetter<?> rowSetter = columnToSetter.get(name);
            if (rowSetter == null) {
                if (table.hasColumns(name)) {
                    throw new RuntimeException("Column has a constant value, can not get setter " + name);
                } else {
                    throw new RuntimeException("Unknown column name " + name);
                }
            }
            return rowSetter;
        }

        @Override
        public void writeRow() {
            synchronized (DynamicTableWriter.this) {
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

                // Before this row can be returned to a pool, it needs to ensure that the underlying sources
                // are appropriately sized to avoid race conditions.
                ensureCapacity(row);
                columnToSetter.values().forEach((x) -> x.setRow(row));

                // The row has been committed during set, we just need to insert the row keys into the table
                if (doFlush) {
                    DynamicTableWriter.this.addRangeToTableIndex(lastCommittedRow + 1, row);
                    lastCommittedRow = row;
                }
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
