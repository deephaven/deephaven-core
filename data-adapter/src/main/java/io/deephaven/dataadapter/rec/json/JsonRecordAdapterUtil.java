//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.base.verify.Assert;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.updaters.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

/**
 * Adapter to convert table data into JSON records.
 */
public class JsonRecordAdapterUtil {
    /**
     * Classes that are safe/reasonable to convert to a String representation when storing in a JSON {@code ObjectNode}.
     */
    protected static final Set<Class<?>> CONVERTIBLE_TO_STRING_CLASSES =
            Set.of(Instant.class, LocalDate.class, LocalTime.class, LocalDateTime.class);

    /**
     * Creates a RecordAdapterDescriptor for representing rows of a table as JSON ObjectNodes. All columns of the table
     * are included.
     *
     * @return A RecordAdapterDescriptor that converts each row of a table into a JSON ObjectNode.
     */
    @NotNull
    public static RecordAdapterDescriptor<ObjectNode> createJsonRecordAdapterDescriptor(
            @NotNull final Table sourceTable) {
        final TableDefinition tableDefinition = sourceTable.getDefinition();
        return createJsonRecordAdapterDescriptor(tableDefinition, tableDefinition.getColumnNames());
    }

    /**
     * Creates a RecordAdapterDescriptor for a record adapter that stores the {@code columns} in a JSON ObjectNode.
     *
     * @param tableDefinition The table definition, used for mapping the columns to their data types.
     * @param columns The columns to include in the JSON ObjectNode.
     * @return A RecordAdapterDescriptor that converts each row of a table into a JSON ObjectNode.
     */
    @NotNull
    public static RecordAdapterDescriptor<ObjectNode> createJsonRecordAdapterDescriptor(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final List<String> columns) {
        tableDefinition.checkHasColumns(columns);
        final List<Class<?>> colTypes = new ArrayList<>();
        for (String sourceName : columns) {
            Class<?> type = tableDefinition.getColumn(sourceName).getDataType();
            colTypes.add(type);
        }
        return createJsonRecordAdapterDescriptor(columns, colTypes);
    }

    @NotNull
    public static RecordAdapterDescriptor<ObjectNode> createJsonRecordAdapterDescriptor(
            @NotNull final List<String> columnNames,
            @NotNull final List<Class<?>> colTypes) {
        if (columnNames.size() != colTypes.size()) {
            throw new IllegalArgumentException("Column names and column types must have the same size: "
                    + columnNames.size() + " != " + colTypes.size());
        }

        // These RecordUpdaters are only used for updating key column values in KeyedRecordAdapter.
        // For data extracted from a table, the generated populateRecords() method is used to directly populate
        // the ObjectNodes from the data arrays.
        final Map<String, RecordUpdater<ObjectNode, ?>> columnAdapters = new LinkedHashMap<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            final String colName = columnNames.get(i);
            final Class<?> colType = colTypes.get(i);
            final RecordUpdater<ObjectNode, ?> updater = getObjectNodeUpdater(colName, colType);

            columnAdapters.put(colName, updater);
        }

        return new JsonRecordAdapterDescriptor(columnAdapters);
    }

    private static <T> RecordUpdater<ObjectNode, ?> getObjectNodeUpdater(
            @NotNull final String colName,
            @NotNull final Class<T> colType) {
        final RecordUpdater<ObjectNode, ?> updater;

        final boolean isConvertibleToString =
                CharSequence.class.isAssignableFrom(colType) || CONVERTIBLE_TO_STRING_CLASSES.contains(colType);

        if (isConvertibleToString || CharSequence.class.isAssignableFrom(colType)) {
            updater = new ObjRecordUpdater<ObjectNode, T>() {
                @Override
                public void accept(ObjectNode record, T v) {
                    if (v == null)
                        record.putNull(colName);
                    else
                        record.put(colName, v.toString());
                }

                @Override
                public Class<T> getSourceType() {
                    return colType;
                }
            };
        } else if (Boolean.class.equals(colType)) {
            updater = new ObjRecordUpdater<ObjectNode, Boolean>() {
                @Override
                public void accept(ObjectNode record, Boolean v) {
                    if (v == null)
                        record.putNull(colName);
                    else
                        record.put(colName, v);
                }

                @Override
                public Class<Boolean> getSourceType() {
                    return Boolean.class;
                }
            };
        } else if (!colType.isPrimitive()) {
            // Other reference type are unsupported
            throw new IllegalArgumentException(
                    "Could not update ObjectNode with column \"" + colName + "\", type: " + colType.getCanonicalName());

        } else if (char.class.equals(colType)) {
            updater = (CharRecordUpdater<ObjectNode>) ((record, v) -> {
                if (io.deephaven.function.Basic.isNull(v))
                    record.putNull(colName);
                // char must be treated as string
                else
                    record.put(colName, Character.toString(v));
            });
        } else if (byte.class.equals(colType)) {
            updater = (ByteRecordUpdater<ObjectNode>) ((record, v) -> {
                if (io.deephaven.function.Basic.isNull(v))
                    record.putNull(colName);
                else
                    record.put(colName, v);
            });
        } else if (short.class.equals(colType)) {
            updater = (ShortRecordUpdater<ObjectNode>) ((record, v) -> {
                if (io.deephaven.function.Basic.isNull(v))
                    record.putNull(colName);
                else
                    record.put(colName, v);
            });
        } else if (int.class.equals(colType)) {
            updater = (IntRecordUpdater<ObjectNode>) ((record, v) -> {
                if (io.deephaven.function.Basic.isNull(v))
                    record.putNull(colName);
                else
                    record.put(colName, v);
            });
        } else if (float.class.equals(colType)) {
            updater = (FloatRecordUpdater<ObjectNode>) ((record, v) -> {
                if (io.deephaven.function.Basic.isNull(v))
                    record.putNull(colName);
                else
                    record.put(colName, v);
            });
        } else if (long.class.equals(colType)) {
            updater = (LongRecordUpdater<ObjectNode>) ((record, v) -> {
                if (io.deephaven.function.Basic.isNull(v))
                    record.putNull(colName);
                else
                    record.put(colName, v);
            });
        } else if (double.class.equals(colType)) {
            updater = (DoubleRecordUpdater<ObjectNode>) ((record, v) -> {
                if (io.deephaven.function.Basic.isNull(v))
                    record.putNull(colName);
                else
                    record.put(colName, v);
            });
        } else {
            throw Assert.statementNeverExecuted();
        }

        return updater;
    }

    private JsonRecordAdapterUtil() {}
}
