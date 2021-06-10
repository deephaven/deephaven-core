package io.deephaven.kafka.ingest;

import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Function;

/**
 * An adapter that maps keys and values to single Deephaven columns.  Each Kafka record produces one Deephaven row.
 */
public class SimpleConsumerRecordToTableWriterAdapter implements ConsumerRecordToTableWriterAdapter {
    private final TableWriter writer;
    private final RowSetter<Integer> kafkaPartitionColumnSetter;
    private final RowSetter<Long> offsetColumnSetter;
    private final RowSetter<DBDateTime> timestampColumnSetter;
    private final RowSetter keyColumnSetter;
    @NotNull private final RowSetter valueColumnSetter;

    private SimpleConsumerRecordToTableWriterAdapter(TableWriter<?> writer, String kafkaPartitionColumnName, String offsetColumnName, String timestampColumnName, String keyColumnName, @NotNull String valueColumnName) {
        this.writer = writer;
        if (kafkaPartitionColumnName != null) {
            kafkaPartitionColumnSetter = writer.getSetter(kafkaPartitionColumnName, Integer.class);
        } else {
            kafkaPartitionColumnSetter = null;
        }
        if (offsetColumnName != null) {
            offsetColumnSetter = writer.getSetter(offsetColumnName, Long.class);
        } else {
            offsetColumnSetter = null;
        }
        if (timestampColumnName != null) {
            timestampColumnSetter = writer.getSetter(timestampColumnName, DBDateTime.class);
        } else {
            timestampColumnSetter = null;
        }
        if (keyColumnName != null) {
            keyColumnSetter = writer.getSetter(keyColumnName);
        } else {
            keyColumnSetter = null;
        }
        valueColumnSetter = writer.getSetter(valueColumnName);
    }

    /**
     * Create a {@link ConsumerRecordToTableWriterAdapter} that maps simple keys and values to single columns in a
     * Deephaven table.  Each Kafka record becomes a row in the table's output.
     *
     * @param kafkaPartitionColumnName the name of the Integer column representing the Kafka partition, if null the partition
     *                            is not mapped to a Deephaven column
     * @param offsetColumnName    the name of the Long column representing the Kafka offset, if null the offset is not
     *                            mapped to a Deephaven column
     * @param timestampColumnName the name of the DateTime column representing the Kafka partition, if null the
     *                            partition is not mapped to a Deephaven column
     * @param keyColumnName       the name of the Deephaven column for the record's key
     * @param valueColumnName     the name of the Deephaven column for the record's value
     *
     * @return an adapter for the TableWriter
     */
    public static Function<TableWriter, ConsumerRecordToTableWriterAdapter> makeFactory(final String kafkaPartitionColumnName,
                                                                                 final String offsetColumnName,
                                                                                 final String timestampColumnName,
                                                                                 final String keyColumnName,
                                                                                 @NotNull final String valueColumnName) {
        return (TableWriter tw) -> new SimpleConsumerRecordToTableWriterAdapter(tw, kafkaPartitionColumnName, offsetColumnName, timestampColumnName, keyColumnName, valueColumnName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void consumeRecord(ConsumerRecord<?, ?> record) throws IOException {
        if (kafkaPartitionColumnSetter != null) {
            kafkaPartitionColumnSetter.setInt(record.partition());
        }
        if (offsetColumnSetter != null) {
            offsetColumnSetter.setLong(record.offset());
        }
        if (timestampColumnSetter != null) {
            final long timestamp = record.timestamp();
            if (record.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
                timestampColumnSetter.set(null);
            } else {
                timestampColumnSetter.set(DBTimeUtils.millisToTime(timestamp));
            }
        }
        if (keyColumnSetter != null) {
            keyColumnSetter.set(record.key());
        }
        valueColumnSetter.set(record.value());

        writer.writeRow();
    }
}
