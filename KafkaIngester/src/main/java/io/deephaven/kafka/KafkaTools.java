package io.deephaven.kafka;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.ColumnsSpecHelper;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.utils.DynamicTableWriter;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.kafka.ingest.ConsumerRecordToTableWriterAdapter;
import io.deephaven.kafka.ingest.KafkaIngester;
import io.deephaven.kafka.ingest.SimpleConsumerRecordToTableWriterAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Properties;

public class KafkaTools {

    public static final String KAFKA_PARTITION_COLUMN_NAME = "kafkaPartition";
    public static final String OFFSET_COLUMN_NAME = "offset";
    public static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    public static final String KEY_COLUMN_NAME = "key";
    public static final String VALUE_COLUMN_NAME = "value";
    public static final String KEY_DESERIALIZER_PROPERTY = "key.deserializer";
    public static final String VALUE_DESERIALIZER_PROPERTY = "value.deserializer";
    public static final String DEFAULT_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    private static final Logger log = LoggerFactory.getLogger(KafkaTools .class);

    public static Table simpleConsumeToTable(
            @NotNull final Properties kafkaConsumerProperties,
            @NotNull final String topic) {
        final DynamicTableWriter tableWriter = new DynamicTableWriter(SIMPLE_TABLE_DEFINITION);
        final ConsumerRecordToTableWriterAdapter adapter = SimpleConsumerRecordToTableWriterAdapter.make(
                tableWriter, KAFKA_PARTITION_COLUMN_NAME, OFFSET_COLUMN_NAME, TIMESTAMP_COLUMN_NAME, KEY_COLUMN_NAME, VALUE_COLUMN_NAME);
        if (!kafkaConsumerProperties.containsKey(KEY_DESERIALIZER_PROPERTY)) {
            kafkaConsumerProperties.setProperty(KEY_DESERIALIZER_PROPERTY, DEFAULT_DESERIALIZER);
        }
        if (!kafkaConsumerProperties.containsKey(VALUE_DESERIALIZER_PROPERTY)) {
            kafkaConsumerProperties.setProperty(VALUE_DESERIALIZER_PROPERTY, DEFAULT_DESERIALIZER);
        }
        final KafkaIngester ingester = new KafkaIngester(
                log,
                kafkaConsumerProperties,
                topic,
                (int partition) -> (ConsumerRecord<?, ?> record) -> {
                    try {
                        adapter.consumeRecord(record);
                    } catch (IOException ex) {
                        throw new UncheckedDeephavenException(ex);
                    }
                },
                (int partition) -> -1);
        ingester.start();
        return tableWriter.getTable();
    }

    private static final String[] simpleColumnNames;
    private static final Class<?>[] simpleColumnDbTypes;

    static {
        final ColumnsSpecHelper cols = new ColumnsSpecHelper()
                .add(KAFKA_PARTITION_COLUMN_NAME, int.class)
                .add(OFFSET_COLUMN_NAME, long.class)
                .add(TIMESTAMP_COLUMN_NAME, DBDateTime.class)
                .add(KEY_COLUMN_NAME, String.class)
                .add(VALUE_COLUMN_NAME, String.class)
                ;
        simpleColumnNames = cols.getColumnNames();
        simpleColumnDbTypes = cols.getDbTypes();
    }

    private static final TableDefinition SIMPLE_TABLE_DEFINITION = TableDefinition.tableDefinition(simpleColumnDbTypes, simpleColumnNames);
}
