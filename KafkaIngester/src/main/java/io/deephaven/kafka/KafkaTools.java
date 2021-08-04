package io.deephaven.kafka;

import gnu.trove.map.hash.TIntLongHashMap;
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
import java.util.Arrays;
import java.util.Properties;
import java.util.function.IntPredicate;
import java.util.function.IntToLongFunction;

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
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final IntToLongFunction partitionToInitialOffset) {
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
                partitionFilter,
                (int partition) -> (ConsumerRecord<?, ?> record) -> {
                    try {
                        adapter.consumeRecord(record);
                    } catch (IOException ex) {
                        throw new UncheckedDeephavenException(ex);
                    }
                },
                partitionToInitialOffset
        );
        ingester.start();
        return tableWriter.getTable();
    }

    public static Table simpleConsumeToTable(
            @NotNull final Properties kafkaConsumerProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter) {
        return simpleConsumeToTable(kafkaConsumerProperties, topic, partitionFilter, ALL_PARTITIONS_DONT_SEEK);
    }

    public static Table simpleConsumeToTable(
            @NotNull final Properties kafkaConsumerProperties,
            @NotNull final String topic) {
        return simpleConsumeToTable(kafkaConsumerProperties, topic, ALL_PARTITIONS);
    }

    public static final IntPredicate ALL_PARTITIONS = KafkaIngester.ALL_PARTITIONS;
    public static final IntToLongFunction ALL_PARTITIONS_SEEK_TO_BEGINNING = KafkaIngester.ALL_PARTITIONS_SEEK_TO_BEGINNING;
    public static final IntToLongFunction ALL_PARTITIONS_DONT_SEEK = KafkaIngester.ALL_PARTITIONS_DONT_SEEK;

    private static final String[] simpleColumnNames;
    private static final Class<?>[] simpleColumnDbTypes;

    public static IntPredicate partitionFilterFromArray(final int[] partitions) {
        Arrays.sort(partitions);
        return (final int p) -> Arrays.binarySearch(partitions, p) >= 0;
    }

    // For the benefit of our python integration
    public static IntToLongFunction partitionToOffsetFromParallelArrays(
            final int[] partitions,
            final long[] offsets) {
        if (partitions.length != offsets.length) {
            throw new IllegalArgumentException("lengths of array arguments do not match");
        }
        final TIntLongHashMap map = new TIntLongHashMap(partitions.length, 0.5f, 0, KafkaIngester.DONT_SEEK);
        for (int i = 0; i < partitions.length; ++i) {
            map.put(partitions[i], offsets[i]);
        }
        return map::get;
    }

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
