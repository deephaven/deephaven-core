package io.deephaven.kafka;

import gnu.trove.map.hash.TIntLongHashMap;
import io.apicurio.registry.utils.serde.AvroKafkaDeserializer;
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
import io.deephaven.kafka.ingest.GenericRecordConsumerRecordToTableWriterAdapter;
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
import java.util.*;
import java.util.function.Function;
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

    public static Schema getAvroSchema(final String schemaServerUrl, final String resourceName, final String version) {
        String action = "setup http client";
        try (final CloseableHttpClient client = HttpClients.custom().build()) {
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

    public static ColumnDefinition<?>[] avroSchemaToColumnDefinitions(
            final Map<String, String> mappedOut, final Schema schema, final Function<String, String> fieldNameMapping) {
        if (schema.isUnion()) {
            throw new UnsupportedOperationException("Union Avro Schemas are not supported");
        }
        final Schema.Type type = schema.getType();
        if (type != Schema.Type.RECORD) {
            throw new IllegalArgumentException("The schema is not a toplevel redcord definition.");
        }
        final List<Schema.Field> fields = schema.getFields();
        final int nCols = fields.size();
        int c = 0;
        final ColumnDefinition<?>[] columns = new ColumnDefinition[nCols];
        for (final Schema.Field field : fields) {
            final Schema fieldSchema = field.schema();
            final String fieldName = field.name();
            final String mappedName = fieldNameMapping.apply(fieldName);
            final Schema.Type fieldType = fieldSchema.getType();
            switch (fieldType) {
                case BOOLEAN:
                    columns[c++] = ColumnDefinition.ofBoolean(mappedName);
                    break;
                case INT:
                    columns[c++] = ColumnDefinition.ofInt(mappedName);
                    break;
                case LONG:
                    columns[c++] = ColumnDefinition.ofLong(mappedName);
                    break;
                case FLOAT:
                    columns[c++] = ColumnDefinition.ofFloat(mappedName);
                    break;
                case DOUBLE:
                    columns[c++] = ColumnDefinition.ofDouble(mappedName);
                    break;
                case STRING:
                    columns[c++] = ColumnDefinition.ofString(mappedName);
                    break;
                case MAP:
                case ENUM:
                case NULL:
                case ARRAY:
                case RECORD:
                case BYTES:
                case FIXED:
                case UNION:
                default:
                    throw new UnsupportedOperationException("Type " + fieldType + " not supported for field " + fieldName);
            }
            if (mappedOut != null) {
                mappedOut.put(fieldName, mappedName);
            }
        }
        Assert.eq(nCols, "nCols", c, "c");
        return columns;
    }

    public static ColumnDefinition<?>[] avroSchemaToColumnDefinitions(final Schema schema, final Function<String, String> fieldNameMapping) {
        return avroSchemaToColumnDefinitions(null, schema, fieldNameMapping);
    }

    public static ColumnDefinition<?>[] avroSchemaToColumnDefinitions(final Schema schema) {
        return avroSchemaToColumnDefinitions(schema, DIRECT_MAPPING);
    }

    public static Table genericAvroConsumeToTable(
            @NotNull final Properties kafkaConsumerProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final IntToLongFunction partitionToInitialOffset,
            final Schema keySchema,
            final Function<String, String> keyFieldNameMapping,
            final Schema valueSchema,
            final Function<String, String> valueFieldNameMapping) {
        if (keySchema == null && valueSchema == null) {
            throw new IllegalArgumentException("key and value schemas can't be both null");
        }

        final ColumnDefinition<?>[] keyColumns;
        final Map<String, String> keyColumnsMap;
        if (keySchema != null) {
            keyColumnsMap = new HashMap<>();
            keyColumns = avroSchemaToColumnDefinitions(keyColumnsMap, keySchema, keyFieldNameMapping);
        } else {
            keyColumnsMap = Collections.emptyMap();
            keyColumns = null;
        }

        final ColumnDefinition<?>[] valueColumns;
        final Map<String, String> valueColumnsMap;
        if (valueSchema != null) {
            valueColumnsMap = new HashMap<>();
            valueColumns = avroSchemaToColumnDefinitions(valueColumnsMap, valueSchema, valueFieldNameMapping);
        } else {
            valueColumnsMap = Collections.emptyMap();
            valueColumns = null;
        }

        final int nCols = 3 +
                ((keySchema != null) ? keyColumns.length : 0) +
                ((valueSchema != null) ? valueColumns.length : 0);
        final ColumnDefinition<?>[] allColumns = new ColumnDefinition<?>[nCols];
        int c = 0;
        final ColumnDefinition<?> partitionColumn = ColumnDefinition.ofInt(KAFKA_PARTITION_COLUMN_NAME);
        allColumns[c++] = (partitionFilter == ALL_PARTITIONS)
                ? partitionColumn
                : partitionColumn.withPartitioning()
                ;
        allColumns[c++] = ColumnDefinition.ofLong(OFFSET_COLUMN_NAME);
        allColumns[c++] = ColumnDefinition.fromGenericType(TIMESTAMP_COLUMN_NAME, DBDateTime.class);
        if (keySchema != null) {
            System.arraycopy(keyColumns, 0, allColumns, c, keyColumns.length);
            c += keyColumns.length;
        }
        if (valueSchema != null) {
            System.arraycopy(valueColumns, 0, allColumns, c, valueColumns.length);
            c += valueColumns.length;
        }
        Assert.eq(nCols, "nCols", c, "c");
        final TableDefinition tableDefinition = new TableDefinition(allColumns);
        final DynamicTableWriter tableWriter = new DynamicTableWriter(tableDefinition);
        final GenericRecordConsumerRecordToTableWriterAdapter adapter = GenericRecordConsumerRecordToTableWriterAdapter.make(
                tableWriter,
                KAFKA_PARTITION_COLUMN_NAME,
                OFFSET_COLUMN_NAME,
                TIMESTAMP_COLUMN_NAME,
                null,
                keyColumnsMap,
                valueColumnsMap);

        if (!kafkaConsumerProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            if (keySchema != null) {
                kafkaConsumerProperties.setProperty(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName());
            } else {
                kafkaConsumerProperties.setProperty(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
            }
        }

        if (!kafkaConsumerProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            if (keySchema != null) {
                kafkaConsumerProperties.setProperty(
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName());
            } else {
                kafkaConsumerProperties.setProperty(
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
            }
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
    public static final Function<String, String> DIRECT_MAPPING = Function.identity();

    //
    // For the benefit of our python integration
    //

    @SuppressWarnings("unused")
    public static IntPredicate partitionFilterFromArray(final int[] partitions) {
        Arrays.sort(partitions);
        return (final int p) -> Arrays.binarySearch(partitions, p) >= 0;
    }

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

    @SuppressWarnings("unused")
    public static Function<String, String> fieldNameMappingFromParallelArrays(
            final String[] fieldNames,
            final String[] columnNames) {
        if (fieldNames.length != columnNames.length) {
            throw new IllegalArgumentException("lengths of array arguments do not match");
        }
        final Map<String, String> map = new HashMap(fieldNames.length);
        for (int i = 0; i < fieldNames.length; ++i) {
            map.put(fieldNames[i], columnNames[i]);
        }
        return (final String fieldName) -> map.getOrDefault(fieldName, fieldName);
    }
}
