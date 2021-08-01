package io.deephaven.kafka;

import gnu.trove.map.hash.TIntLongHashMap;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.utils.DynamicTableWriter;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.kafka.ingest.ConsumerRecordToTableWriterAdapter;
import io.deephaven.kafka.ingest.KafkaIngester;
import io.deephaven.kafka.ingest.SimpleConsumerRecordToTableWriterAdapter;
import org.apache.avro.Schema;
import org.apache.commons.codec.Charsets;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.HttpClients;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
    public static final String INT_DESERIALIZER = IntegerDeserializer.class.getName();
    public static final String LONG_DESERIALIZER = LongDeserializer.class.getName();
    public static final String DOUBLE_DESERIALIZER = DoubleDeserializer.class.getName();
    public static final String BYTE_ARRAY_DESERIALIZER = ByteArrayDeserializer.class.getName();
    public static final String STRING_DESERIALIZER = StringDeserializer.class.getName();

    private static final Logger log = LoggerFactory.getLogger(KafkaTools .class);

    public static org.apache.avro.Schema getAvroSchema(final String schemaServerUrl, final String resourceName, final String version) {
        String action = "setup http client";
        try (CloseableHttpClient client = HttpClients.custom().build()) {
            final String requestStr = schemaServerUrl + "/subjects/" + resourceName + "/versions/" + version + "/schema";
            final HttpUriRequest request = RequestBuilder.get().setUri(requestStr).build();
            action = "execute schema request " + requestStr;
            final HttpResponse response = client.execute(request);
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                throw new UncheckedDeephavenException("Got status code " + statusCode + " requesting " + request);
            }
            action = "extract json server response";
            final HttpEntity entity = response.getEntity();
            final Header encodingHeader = entity.getContentEncoding();
            final Charset encoding = encodingHeader == null
                    ? StandardCharsets.UTF_8
                    : Charsets.toCharset(encodingHeader.getValue());
            final String json = EntityUtils.toString(entity, encoding);
            action = "parse schema server response: " + json;
            return new Schema.Parser().parse(json);
        } catch (Exception e) {
            throw new UncheckedDeephavenException("Exception while trying to " + action, e);
        }
    }

    /**
     * Consume a number of partitions from a single, simple type key and single type value Kafka topic to a single table,
     * with table partitions matching Kafka partitions.
     *
     * The types of key and value are deduced from the serializer classes for key and value in the provided Properties
     * object for the Kafka Consumer initialization; if the Properties object provided does not contain
     * keys for key deserializer or value deserializer, they are assumed to be of String type and the corresponding
     * property for the respective deserializer are added.
     *
     * @param kafkaConsumerProperties  Properties to be passed to create the KafkaConsumer.
     * @param topic                    Kafka topic name.
     * @param partitionFilter          A predicate returning true for the partitions to consume.
     * @param partitionToInitialOffset A function specifying the desired initial offset for each partition consumed.
     * @return                         The resulting live table.
     */
    public static Table simpleConsumeToTable(
            @NotNull final Properties kafkaConsumerProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final IntToLongFunction partitionToInitialOffset) {
        final int nCols = 5;
        final ColumnDefinition<?>[] columns = new ColumnDefinition[nCols];
        int c = 0;
        final ColumnDefinition<?> partitionColumn = ColumnDefinition.ofInt(KAFKA_PARTITION_COLUMN_NAME);
        columns[c++] = (partitionFilter == ALL_PARTITIONS)
                ? partitionColumn
                : partitionColumn.withPartitioning()
                ;
        columns[c++] = ColumnDefinition.ofLong(OFFSET_COLUMN_NAME);
        columns[c++] = ColumnDefinition.fromGenericType(TIMESTAMP_COLUMN_NAME, DBDateTime.class);
        columns[c++] = getCol(kafkaConsumerProperties, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_COLUMN_NAME);
        columns[c++] = getCol(kafkaConsumerProperties, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_COLUMN_NAME);
        Assert.eq(nCols, "nCols", c, "c");
        final TableDefinition tableDefinition = new TableDefinition(columns);
        final DynamicTableWriter tableWriter = new DynamicTableWriter(tableDefinition);
        final ConsumerRecordToTableWriterAdapter adapter = SimpleConsumerRecordToTableWriterAdapter.make(
                tableWriter, KAFKA_PARTITION_COLUMN_NAME, OFFSET_COLUMN_NAME, TIMESTAMP_COLUMN_NAME, KEY_COLUMN_NAME, VALUE_COLUMN_NAME);

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

    private static ColumnDefinition<?> getCol(
            @NotNull final Properties properties,
            @NotNull final String deserializerKey,
            @NotNull final String columnName) {
        if (!properties.containsKey(deserializerKey)) {
            properties.setProperty(deserializerKey, STRING_DESERIALIZER);
            return ColumnDefinition.ofString(columnName);
        }
        final String deserializer = properties.getProperty(deserializerKey);
        if (INT_DESERIALIZER.equals(deserializer)) {
            return ColumnDefinition.ofInt(columnName);
        }
        if (LONG_DESERIALIZER.equals(deserializer)) {
            return ColumnDefinition.ofLong(columnName);
        }
        if (DOUBLE_DESERIALIZER.equals(deserializer)) {
            return ColumnDefinition.ofDouble(columnName);
        }
        if (BYTE_ARRAY_DESERIALIZER.equals(deserializer)) {
            return ColumnDefinition.fromGenericType(columnName, byte[].class, byte.class);
        }
        throw new IllegalArgumentException(
                "Deserializer type " + deserializer + " for " + deserializerKey + " not supported.");
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

    // For the benefit of our python integration
    @SuppressWarnings("unused")
    public static IntPredicate partitionFilterFromArray(final int[] partitions) {
        Arrays.sort(partitions);
        return (final int p) -> Arrays.binarySearch(partitions, p) >= 0;
    }

    // For the benefit of our python integration
    @SuppressWarnings("unused")
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
}
