package io.deephaven.kafka.ingest;

import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Converts a consumer record containing JSON serialized objects to a Deephaven row.
 *
 * <p></p>After processing explicit mappings for fields; the JSON record is searched for exact matches of the key.
 */
@SuppressWarnings("unused")
public class JsonConsumerRecordToTableWriterAdapter implements ConsumerRecordToTableWriterAdapter {
    private final TableWriter<?> writer;
    private final Map<String, String> valueColumns;
    private final RowSetter<Integer> kafkaPartitionColumnSetter;
    private final RowSetter<Long> offsetColumnSetter;
    private final RowSetter<DBDateTime> timestampColumnSetter;
    private final boolean allowMissingKeys;
    private final boolean allowNullValues;
    private final Map<String, JsonRecordSetter> columnNameToSetter;


    private JsonConsumerRecordToTableWriterAdapter(TableWriter<?> writer, String kafkaPartitionColumn, String offsetColumnName,
                                                   String timestampColumnName, Map<String, String> valueColumns, Set<String> unusedColumns,
                                                   boolean allowMissingKeys, boolean allowNullValues, boolean autoValueMapping) {
        this.writer = writer;
        this.valueColumns = valueColumns;
        this.allowMissingKeys = allowMissingKeys;
        this.allowNullValues = allowNullValues;

        final String [] columnNames = writer.getColumnNames();
        final Class [] columnTypes = writer.getColumnTypes();

        columnNameToSetter = new LinkedHashMap<>();
        for (int ii = 0; ii < columnNames.length; ++ii) {
            columnNameToSetter.put(columnNames[ii], JsonRecordUtil.getSetter(columnTypes[ii]));
        }

        if (kafkaPartitionColumn != null) {
            kafkaPartitionColumnSetter = writer.getSetter(kafkaPartitionColumn, Integer.class);
            columnNameToSetter.remove(kafkaPartitionColumn);
        } else {
            kafkaPartitionColumnSetter = null;
        }
        if (offsetColumnName != null) {
            offsetColumnSetter = writer.getSetter(offsetColumnName, Long.class);
            columnNameToSetter.remove(offsetColumnName);
        } else {
            offsetColumnSetter = null;
        }
        if (timestampColumnName != null) {
            timestampColumnSetter = writer.getSetter(timestampColumnName, DBDateTime.class);
            columnNameToSetter.remove(timestampColumnName);
        } else {
            timestampColumnSetter = null;
        }
        unusedColumns.forEach(columnNameToSetter::remove);

        if (autoValueMapping) {
            columnNameToSetter.keySet().forEach(column -> valueColumns.putIfAbsent(column, column));
        } else if (!columnNameToSetter.isEmpty()) {
            throw new RuntimeException("No mapping defined for columns " + columnNameToSetter.keySet());
        }

    }

    /**
     * A builder to map key and value fields to table columns.
     */
    public static class Builder {
        private String kafkaPartitionColumnName;
        private String offsetColumnName;
        private String timestampColumnName;
        private final Set<String> columnsUnmapped = new HashSet<>();
        private final Map<String, String> columnToValueFieldSetter = new LinkedHashMap<>();
        private boolean allowMissingKeys = false;
        private boolean allowNullValues = false;
        private boolean autoValueMapping = false;

        /**
         * Set the name of the column which stores the Kafka partition identifier of the record.
         *
         * @param kafkaPartitionColumnName the name of the column in the output table
         *
         * @return this builder
         */
        @NotNull
        public JsonConsumerRecordToTableWriterAdapter.Builder kafkaPartitionColumnName(@NotNull String kafkaPartitionColumnName) {
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
        public JsonConsumerRecordToTableWriterAdapter.Builder offsetColumnName(@NotNull String offsetColumnName) {
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
        @NotNull public JsonConsumerRecordToTableWriterAdapter.Builder timestampColumnName(@NotNull String timestampColumnName) {
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
        @NotNull public JsonConsumerRecordToTableWriterAdapter.Builder allowUnmapped(@NotNull String allowUnmapped) {
            if (isDefined(allowUnmapped)) {
                throw new RuntimeException("Column " + allowUnmapped + " is already defined!");
            }
            columnsUnmapped.add(allowUnmapped);
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
        @NotNull public JsonConsumerRecordToTableWriterAdapter.Builder addColumnToValueField(@NotNull String column, @NotNull String field) {
            if (isDefined(column)) {
                throw new RuntimeException("Column " + column + " is already defined!");
            }
            columnToValueFieldSetter.put(column, field);
            return this;
        }

        /**
         * If allowMissingKeys is set, then a request for a value using a key that is not found in the record
         * will receive a null value. Otherwise, an exception is thrown when a key is not found in the record.
         *
         * @param allowMissingKeys to allow quietly continuing if requested value's key is not in the current record.
         *
         * @return this builder
         */
        @NotNull public JsonConsumerRecordToTableWriterAdapter.Builder allowMissingKeys(boolean allowMissingKeys) {
            this.allowMissingKeys = allowMissingKeys;
            return this;
        }

        /**
         * If allowNullValues is set, then records with a null value will have their columns null filled; otherwise
         * an exception is thrown on receipt of a null value.
         *
         * If no value fields are set, then no columns are taken from the value; so this flag has no effect.
         *
         * @param allowNullValues if null values are allowed
         *
         * @return this builder
         */
        @NotNull public JsonConsumerRecordToTableWriterAdapter.Builder allowNullValues(boolean allowNullValues) {
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
        @NotNull public JsonConsumerRecordToTableWriterAdapter.Builder autoValueMapping(boolean autoValueMapping) {
            this.autoValueMapping = autoValueMapping;
            return this;
        }

        private boolean isDefined(String column) {
            return columnsUnmapped.contains(column) || columnToValueFieldSetter.containsKey(column);
        }

        /**
         * Create a factory that produces a ConsumerRecordToTableWriterAdapter for a TableWriter.
         *
         * @return the factory
         */
        @NotNull
        Function<TableWriter, ConsumerRecordToTableWriterAdapter> buildFactory() {
            return (TableWriter tw) -> new JsonConsumerRecordToTableWriterAdapter(tw,
                    kafkaPartitionColumnName, offsetColumnName, timestampColumnName,
                    columnToValueFieldSetter, columnsUnmapped, allowMissingKeys,
                    allowNullValues, autoValueMapping);
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

        if (!valueColumns.isEmpty()) {
            final JsonRecord recordDetails = new JsonRecord((String)record.value(), allowMissingKeys, allowNullValues);
            for (final Map.Entry<String, String> valueEntry : valueColumns.entrySet()) {
                columnNameToSetter.get(valueEntry.getKey()).set(recordDetails, valueEntry.getValue(), writer.getSetter(valueEntry.getKey()));
            }
        }

        writer.writeRow();
    }
}
