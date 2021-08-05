/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.avro.generic.GenericRecord;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

/**
 *  Convert an Avro {@link GenericRecord} to Deephaven rows.
 *
 *  Each GenericRecord produces a single row of output, according to the maps of Table column names to Avro field names
 *  for the keys and values.
 */
public class GenericRecordConsumerRecordToTableWriterAdapter implements ConsumerRecordToTableWriterAdapter {
    private final TableWriter<?> writer;
    private final Map<String, String> keyColumns;
    private final Map<String, String> valueColumns;
    private final RowSetter<Integer> kafkaPartitionColumnSetter;
    private final RowSetter<Long> offsetColumnSetter;
    private final RowSetter<DBDateTime> timestampColumnSetter;
    @SuppressWarnings("rawtypes")
    private final RowSetter rawKeyColumnSetter;
    private final Map<String, Function<GenericRecord, ?>> columnToKeyFunction;
    private final Map<String, Function<GenericRecord, ?>> columnToValueFunction;
    private final boolean allowNullKeys;
    private final boolean allowNullValues;

    private GenericRecordConsumerRecordToTableWriterAdapter(
            final TableWriter<?> writer,
            final String kafkaPartitionColumn,
            final String offsetColumnName,
            final String timestampColumnName,
            String rawKeyColumnName,
            final Map<String, String> keyColumns,
            final Map<String, String> valueColumns,
            final Map<String, Function<GenericRecord, ?>> columnToKeyFunction,
            final Map<String, Function<GenericRecord, ?>> columnToValueFunction,
            final Set<String> unusedColumns,
            final boolean allowNullKeys,
            final boolean allowNullValues,
            final boolean autoValueMapping) {
        this.writer = writer;
        this.keyColumns = keyColumns;
        this.valueColumns = valueColumns;
        this.columnToKeyFunction = columnToKeyFunction;
        this.columnToValueFunction = columnToValueFunction;
        this.allowNullKeys = allowNullKeys;
        this.allowNullValues = allowNullValues;

        final String [] columnNames = writer.getColumnNames();
        final Class<?> [] columnTypes = writer.getColumnTypes();

        final Map<String, Class<?>> columnNameToType = new LinkedHashMap<>();
        for (int ii = 0; ii < columnNames.length; ++ii) {
            columnNameToType.put(columnNames[ii], columnTypes[ii]);
        }

        if (kafkaPartitionColumn != null) {
            kafkaPartitionColumnSetter = writer.getSetter(kafkaPartitionColumn, Integer.class);
            columnNameToType.remove(kafkaPartitionColumn);
        } else {
            kafkaPartitionColumnSetter = null;
        }
        if (offsetColumnName != null) {
            offsetColumnSetter = writer.getSetter(offsetColumnName, Long.class);
            columnNameToType.remove(offsetColumnName);
        } else {
            offsetColumnSetter = null;
        }
        if (timestampColumnName != null) {
            timestampColumnSetter = writer.getSetter(timestampColumnName, DBDateTime.class);
            columnNameToType.remove(timestampColumnName);
        } else {
            timestampColumnSetter = null;
        }
        if (rawKeyColumnName != null) {
            rawKeyColumnSetter = writer.getSetter(rawKeyColumnName);
            columnNameToType.remove(rawKeyColumnName);
        } else {
            rawKeyColumnSetter = null;
        }
        keyColumns.keySet().forEach(columnNameToType::remove);
        valueColumns.keySet().forEach(columnNameToType::remove);
        unusedColumns.forEach(columnNameToType::remove);
        columnToKeyFunction.keySet().forEach(columnNameToType::remove);
        columnToValueFunction.keySet().forEach(columnNameToType::remove);

        if (autoValueMapping) {
            columnNameToType.keySet().forEach(column -> valueColumns.put(column, column));
        } else if (!columnNameToType.isEmpty()) {
            throw new RuntimeException("No mapping defined for columns " + columnNameToType.keySet());
        }
    }

    public static GenericRecordConsumerRecordToTableWriterAdapter make(
            final TableWriter<?> writer,
            final String kafkaPartitionColumn,
            final String offsetColumnName,
            final String timestampColumnName,
            String rawKeyColumnName,
            final Map<String, String> keyColumns,
            final Map<String, String> valueColumns) {
        return new GenericRecordConsumerRecordToTableWriterAdapter(
                writer,
                kafkaPartitionColumn,
                offsetColumnName,
                timestampColumnName,
                rawKeyColumnName,
                keyColumns,
                valueColumns,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                false,
                true,
                true);
    }

    /**
     * A builder to map key and value fields to table columns.
     *
     *
     */
    public static class Builder {
        private String kafkaPartitionColumnName;
        private String offsetColumnName;
        private String timestampColumnName;
        private String rawKeyColumnName;
        private final Set<String> columnsUnmapped = new HashSet<>();
        private final Map<String, String> columnToKeyFieldSetter = new LinkedHashMap<>();
        private final Map<String, String> columnToValueFieldSetter = new LinkedHashMap<>();
        private final Map<String, Function<GenericRecord, ?>> columnToKeyFunction = new LinkedHashMap<>();
        private final Map<String, Function<GenericRecord, ?>> columnToValueFunction = new LinkedHashMap<>();
        private boolean allowNullKeys = false;
        private boolean allowNullValues = false;
        private boolean autoValueMapping = false;

        /**
         * Set the name of the column which stores the Kafka partition identifier of the record.
         *
         * @param kafkaPartitionColumnName the name of the column in the output table
         *
         * @return this builder
         */
        @NotNull public Builder kafkaPartitionColumnName(@NotNull String kafkaPartitionColumnName) {
            this.kafkaPartitionColumnName = kafkaPartitionColumnName;
            return this;
        }

        /**
         * Set the name of the column which stores the Kafka offset of the record.
         *
         * @param offsetColumnName the name of the column in the output table
         *
         * @return this builder
         */
        public Builder offsetColumnName(@NotNull String offsetColumnName) {
            this.offsetColumnName = offsetColumnName;
            return this;
        }

        /**
         * Set the name of the column which stores the Kafka timestamp of the record.
         *
         * @param timestampColumnName the name of the column in the output table
         *
         * @return this builder
         */
        @NotNull public Builder timestampColumnName(@NotNull String timestampColumnName) {
            this.timestampColumnName = timestampColumnName;
            return this;
        }

        /**
         * Allow the column with the given name to be unmapped in the output.
         *
         * Unmapped columns will have no setter, and will thus be null filled in the output.
         *
         * @param allowUnmapped the column name to allow to be unmapped
         *
         * @return this builder
         */
        @NotNull public Builder allowUnmapped(@NotNull String allowUnmapped) {
            if (isDefined(allowUnmapped)) {
                throw new RuntimeException("Column " + allowUnmapped + " is already defined!");
            }
            columnsUnmapped.add(allowUnmapped);
            return this;
        }

        /**
         * Map a column to a field in the key record.
         *
         * @param column the name of the output column
         * @param field the name of the field in the key record
         *
         * @return this builder
         */
        @NotNull public Builder addColumnToKeyField(@NotNull String column, @NotNull String field) {
            if (isDefined(column)) {
                throw new RuntimeException("Column " + column + " is already defined!");
            }
            columnToKeyFieldSetter.put(column, field);
            return this;
        }

        /**
         * Map a column to a the key of the record, the key must match the type of the column.
         *
         * This configuration is intended for when the keys of your records are not in fact GenericRecords, but rather
         * a primitive or a String.
         *
         * @param column the name of the output column
         *
         * @return this builder
         */
        @NotNull public Builder addColumnToKey(@NotNull String column) {
            if (isDefined(column)) {
                throw new RuntimeException("Column " + column + " is already defined!");
            }
            rawKeyColumnName = column;
            return this;
        }

        /**
         * Map a column to a field in the value record.
         *
         * @param column the name of the output column
         * @param field the name of the field in the value record
         *
         * @return this builder
         */
        @NotNull public Builder addColumnToValueField(@NotNull String column, @NotNull String field) {
            if (isDefined(column)) {
                throw new RuntimeException("Column " + column + " is already defined!");
            }
            columnToValueFieldSetter.put(column, field);
            return this;
        }

        /**
         * Map a column to a function of the key record.
         *
         * @param column the name of the output column
         * @param function the function to apply to the key record
         *
         * @return this builder
         */
        @NotNull public Builder addColumnToKeyFunction(@NotNull String column, @NotNull Function<GenericRecord, ?> function) {
            if (isDefined(column)) {
                throw new RuntimeException("Column " + column + " is already defined!");
            }
            columnToKeyFunction.put(column, function);
            return this;
        }

        /**
         * Map a column to a function of the value record.
         *
         * @param column the name of the output column
         * @param function the function to apply to the value record
         *
         * @return this builder
         */
        @NotNull public Builder addColumnToValueFunction(@NotNull String column, @NotNull Function<GenericRecord, ?> function) {
            if (isDefined(column)) {
                throw new RuntimeException("Column " + column + " is already defined!");
            }
            columnToValueFunction.put(column, function);
            return this;
        }

        /**
         * If allowNullKeys is set, then records with a null key will have their columns null filled; otherwise
         * an exception is thrown on receipt of a null key.
         *
         * If no key fields are set, then no columns are taken from the value; so this flag has no effect.
         *
         * @param allowNullKeys if null key are allowed
         *
         * @return this builder
         */
        @NotNull public Builder allowNullKeys(boolean allowNullKeys) {
            this.allowNullKeys = allowNullKeys;
            return this;
        }

        /**
         * If allowNullValues is set, then records with a null value will have their columns null filled; otherwise
         * an exception is thrown on receipt of a null value.
         *
         * If no value fiels are set, then no columns are taken from the value; so this flag has no effect.
         *
         * @param allowNullValues if null values are allowed
         *
         * @return this builder
         */
        @NotNull public Builder allowNullValues(boolean allowNullValues) {
            this.allowNullValues = allowNullValues;
            return this;
        }

        /**
         * If autoValueMapping is set, then all unused columns are mapped to a field of the same name in the generic record.
         *
         * @param autoValueMapping if unused columns should be automatically mapped to a field of the same name
         *
         * @return this builder
         */
        @NotNull public Builder autoValueMapping(boolean autoValueMapping) {
            this.autoValueMapping = autoValueMapping;
            return this;
        }

        private boolean isDefined(String column) {
            return columnsUnmapped.contains(column)
                    || columnToKeyFieldSetter.containsKey(column)
                    || columnToValueFieldSetter.containsKey(column)
                    || columnToKeyFunction.containsKey(column)
                    || columnToValueFunction.containsKey(column)
                    || column.equals(kafkaPartitionColumnName)
                    || column.equals(offsetColumnName)
                    || column.equals(timestampColumnName)
                    || column.equals(rawKeyColumnName);
        }

        /**
         * Create a factory that produces a ConsumerRecordToTableWriterAdapter for a TableWriter.
         *
         * @return the factory
         */
        @NotNull Function<TableWriter, ConsumerRecordToTableWriterAdapter> buildFactory() {
            return (TableWriter tw) -> new GenericRecordConsumerRecordToTableWriterAdapter(tw,
                    kafkaPartitionColumnName, offsetColumnName, timestampColumnName,
                    rawKeyColumnName,
                    columnToKeyFieldSetter, columnToValueFieldSetter,
                    columnToKeyFunction, columnToValueFunction, columnsUnmapped,
                    allowNullKeys, allowNullValues, autoValueMapping);
        }
    }

    @Override
    public void consumeRecord(ConsumerRecord<?, ?> record) throws IOException {
        if (kafkaPartitionColumnSetter != null) {
            kafkaPartitionColumnSetter.setInt(record.partition());
        }
        if (offsetColumnSetter != null) {
            offsetColumnSetter.setLong(record.offset());
        }
        if (timestampColumnSetter != null) {
            if (record.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
                timestampColumnSetter.set(null);
            } else {
                timestampColumnSetter.set(DBTimeUtils.millisToTime(record.timestamp()));
            }
        }

        if (rawKeyColumnSetter != null) {
            //noinspection unchecked
            rawKeyColumnSetter.set(record.key());
        }

        if (!keyColumns.isEmpty()) {
            final GenericRecord keyGenericRecord = (GenericRecord) record.key();
            if (keyGenericRecord == null) {
                if (!allowNullKeys) {
                    throw new KafkaIngesterException("Null keys are not permitted");
                }
                for (final Map.Entry<String, String> keyEntry : keyColumns.entrySet()) {
                    //noinspection unchecked
                    writer.getSetter(keyEntry.getKey()).set(null);
                }
            } else {
                for (final Map.Entry<String, String> keyEntry : keyColumns.entrySet()) {
                    //noinspection unchecked
                    writer.getSetter(keyEntry.getKey()).set(keyGenericRecord.get(keyEntry.getValue()));
                }
            }
        }

        if (!valueColumns.isEmpty()) {
            final GenericRecord valueGenericRecord = (GenericRecord) record.value();
            if (valueGenericRecord == null) {
                if (!allowNullValues) {
                    throw new KafkaIngesterException("Null values are not permitted");
                }
                for (final Map.Entry<String, String> keyEntry : valueColumns.entrySet()) {
                    //noinspection unchecked
                    writer.getSetter(keyEntry.getKey()).set(null);
                }
            } else {
                for (final Map.Entry<String, String> valueEntry : valueColumns.entrySet()) {
                    //noinspection unchecked
                    writer.getSetter(valueEntry.getKey()).set(valueGenericRecord.get(valueEntry.getValue()));
                }
            }
        }

        if (!columnToKeyFunction.isEmpty()) {
            applyFunctions(columnToKeyFunction, (GenericRecord) record.key());
        }
        if (!columnToValueFunction.isEmpty()) {
            applyFunctions(columnToValueFunction, (GenericRecord) record.value());
        }

        writer.writeRow();
    }

    private void applyFunctions(Map<String, Function<GenericRecord, ?>> columnToFunction, GenericRecord record) {
        if (record == null && !allowNullValues) {
            throw new KafkaIngesterException("Null values are not permitted");
        }
        for (final Map.Entry<String, Function<GenericRecord, ?>> keyEntry : columnToFunction.entrySet()) {
            //noinspection unchecked
            writer.getSetter(keyEntry.getKey()).set(keyEntry.getValue().apply(record));
        }
    }
}
