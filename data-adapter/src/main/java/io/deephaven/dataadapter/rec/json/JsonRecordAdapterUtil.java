//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.rec.json;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptorBuilder;
import io.deephaven.dataadapter.rec.updaters.*;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

/**
 * Adapter to convert table data into JSON records.
 */
public class JsonRecordAdapterUtil {
    static final Set<Class<?>> CONVERTIBLE_TO_STRING_CLASSES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            Instant.class,
            LocalDate.class,
            LocalTime.class,
            LocalDateTime.class)));

    /**
     * Creates a RecordAdapterDescriptor for a record adapter that stores the {@code columns} in a HashMap.
     *
     * @param columns The columns to include in the map.
     * @return A RecordAdapterDescriptor that converts each row of a table into a JSON ObjectNode.
     */
    @NotNull
    public static RecordAdapterDescriptor<ObjectNode> createJsonRecordAdapterDescriptor(
            @NotNull final Table sourceTable,
            @NotNull final List<String> columns) {
        final RecordAdapterDescriptorBuilder<ObjectNode> descriptorBuilder =
                RecordAdapterDescriptorBuilder.create(() -> new ObjectNode(JsonNodeFactory.instance));

        for (String colName : columns) {
            final ColumnSource<?> colSource = sourceTable.getColumnSource(colName);
            final Class<?> colType = colSource.getType();
            final RecordUpdater<ObjectNode, ?> updater = getObjectNodeUpdater(colName, colType);

            descriptorBuilder.addColumnAdapter(colName, updater);
        }

        // Configure the descriptor to create generated JsonRecordAdapters instead of using DefaultMultiRowRecordAdapter
        descriptorBuilder.setMultiRowAdapterSupplier((table, descriptor) -> {

            // Generate a class that creates JSON records from the data arrays
            Class<? extends BaseJsonRecordAdapter> c = new JsonRecordAdapterGenerator(descriptor).generate();
            final Constructor<? extends BaseJsonRecordAdapter> constructor;
            try {
                constructor = c.getConstructor(Table.class, RecordAdapterDescriptor.class);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Could not find constructor for generated JsonRecordAdapter", e);
            }

            try {
                return constructor.newInstance(table, descriptor);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("Could not instantiate generated JsonRecordAdapter", e);
            }
        });
        descriptorBuilder.setMultiRowPartitionedTableAdapterSupplier((partitionedTable, descriptor) -> {

            // Generate a class that creates JSON records from the data arrays
            Class<? extends BaseJsonRecordAdapter> c = new JsonRecordAdapterGenerator(descriptor).generate();
            final Constructor<? extends BaseJsonRecordAdapter> constructor;
            try {
                constructor = c.getConstructor(PartitionedTable.class, RecordAdapterDescriptor.class);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Could not find constructor for generated JsonRecordAdapter", e);
            }

            try {
                return constructor.newInstance(partitionedTable, descriptor);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("Could not instantiate generated JsonRecordAdapter", e);
            }
        });

        return descriptorBuilder.build();
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
