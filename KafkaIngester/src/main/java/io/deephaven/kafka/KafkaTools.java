package io.deephaven.kafka;

import gnu.trove.map.hash.TIntLongHashMap;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
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

    public static final String KAFKA_PARTITION_COLUMN_NAME_PROPERTY = "deephaven.partition.column.name";
    public static final String KAFKA_PARTITION_COLUMN_NAME_DEFAULT = "KafkaPartition";
    public static final String OFFSET_COLUMN_NAME_PROPERTY = "deephaven.offset.column.name";
    public static final String OFFSET_COLUMN_NAME_DEFAULT = "KafkaOffset";
    public static final String TIMESTAMP_COLUMN_NAME_PROPERTY = "deephaven.timestamp.column.name";
    public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "KafkaTimestamp";
    public static final String KEY_COLUMN_NAME_PROPERTY = "deephaven.key.column.name";
    public static final String KEY_COLUMN_NAME_DEFAULT = "KafkaKey";
    public static final String VALUE_COLUMN_NAME_PROPERTY = "deephaven.value.column.name";
    public static final String VALUE_COLUMN_NAME_DEFAULT = "KafkaValue";
    public static final String KEY_COLUMN_TYPE_PROPERTY = "deephaven.key.column.type";
    public static final String VALUE_COLUMN_TYPE_PROPERTY = "deephaven.value.column.type";
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
            throw new IllegalArgumentException("The schema is not a toplevel record definition.");
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
        final ColumnDefinition<?> partitionColumn = ColumnDefinition.ofInt(KAFKA_PARTITION_COLUMN_NAME_DEFAULT);
        allColumns[c++] = (partitionFilter == ALL_PARTITIONS)
                ? partitionColumn
                : partitionColumn.withPartitioning()
                ;
        allColumns[c++] = ColumnDefinition.ofLong(OFFSET_COLUMN_NAME_DEFAULT);
        allColumns[c++] = ColumnDefinition.fromGenericType(TIMESTAMP_COLUMN_NAME_DEFAULT, DBDateTime.class);
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
                KAFKA_PARTITION_COLUMN_NAME_DEFAULT,
                OFFSET_COLUMN_NAME_DEFAULT,
                TIMESTAMP_COLUMN_NAME_DEFAULT,
                null,
                keyColumnsMap,
                valueColumnsMap);

        if (!kafkaConsumerProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            if (keySchema != null) {
                kafkaConsumerProperties.setProperty(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            } else {
                kafkaConsumerProperties.setProperty(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
            }
        }

        if (!kafkaConsumerProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            if (valueSchema != null) {
                kafkaConsumerProperties.setProperty(
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
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
     * The types of key and value are either specified in the properties as "key.type" and "value.type",
     * or deduced from the serializer classes for key and value in the provided Properties object.
     * The names for the key and value columns can be provided in the properties as "key.column.name" and "value.column.name",
     * and otherwise default to "key" and "value".
     * object for the Kafka Consumer initialization; if the Properties object provided does not contain
     * keys for key deserializer or value deserializer, they are assumed to be of String type and the corresponding
     * property for the respective deserializer are added.
     *
     * @param consumerProperties       Properties to configure this table and also to be passed to create the KafkaConsumer.
     * @param topic                    Kafka topic name.
     * @param partitionFilter          A predicate returning true for the partitions to consume.
     * @param partitionToInitialOffset A function specifying the desired initial offset for each partition consumed.
     * @return                         The resulting live table.
     */
    public static Table consumeToTable(
            @NotNull final Properties consumerProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final IntToLongFunction partitionToInitialOffset) {
        final int nCols = 5;
        final ColumnDefinition<?>[] columns = new ColumnDefinition[nCols];
        int c = 0;
        c += getCommonCols(columns, 0, consumerProperties, partitionFilter == ALL_PARTITIONS);
        columns[c++] = getKeyOrValueCol(
                consumerProperties,
                KEY_COLUMN_TYPE_PROPERTY,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                KEY_COLUMN_NAME_PROPERTY,
                KEY_COLUMN_NAME_DEFAULT,
                true);
        columns[c++] = getKeyOrValueCol(
                consumerProperties,
                VALUE_COLUMN_TYPE_PROPERTY,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                VALUE_COLUMN_NAME_PROPERTY,
                VALUE_COLUMN_NAME_DEFAULT,
                false);
        Assert.eq(nCols, "nCols", c, "c");
        final TableDefinition tableDefinition = new TableDefinition(withoutNulls(columns));
        final DynamicTableWriter tableWriter = new DynamicTableWriter(tableDefinition);
        final Function<ColumnDefinition<?>, String> orNull = (final ColumnDefinition<?> colDef) -> {
            if (colDef == null) {
                return null;
            }
            return colDef.getName();
        };
        final ConsumerRecordToTableWriterAdapter adapter = SimpleConsumerRecordToTableWriterAdapter.make(
                tableWriter,
                orNull.apply(columns[0]),
                orNull.apply(columns[1]),
                orNull.apply(columns[2]),
                orNull.apply(columns[3]),
                orNull.apply(columns[4]));

        final KafkaIngester ingester = new KafkaIngester(
                log,
                consumerProperties,
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

    private static ColumnDefinition<?>[] withoutNulls(final ColumnDefinition<?>[] columns) {
        int nNulls = 0;
        for (final ColumnDefinition<?> col : columns) {
            if (col == null) {
                ++nNulls;
            }
        }
        if (nNulls == 0) {
            return columns;
        }
        final ColumnDefinition<?>[] result = new ColumnDefinition<?>[columns.length - nNulls];
        int ir = 0;
        for (final ColumnDefinition<?> col : columns) {
            if (col == null) {
                continue;
            }
            result[ir++] = col;
        }
        return result;
    }

    private static void getCommonCol(
            @NotNull final ColumnDefinition[] columnsToSet,
            final int outOffset,
            @NotNull final Properties consumerProperties,
            @NotNull final String columnNameProperty,
            @NotNull final String columnNameDefault,
            @NotNull Function<String, ColumnDefinition<?>> builder) {
        if (consumerProperties.containsKey(columnNameProperty)) {
            final String partitionColumnName = consumerProperties.getProperty(columnNameProperty);
            if (partitionColumnName == null || partitionColumnName.equals("")) {
                columnsToSet[outOffset] = null;
            } else {
                columnsToSet[outOffset] = builder.apply(partitionColumnName);
            }
            consumerProperties.remove(columnNameProperty);
        } else {
            columnsToSet[outOffset] = builder.apply(columnNameDefault);
        }
    }

    private static int getCommonCols(
            @NotNull final ColumnDefinition<?>[] columnsToSet,
            final int outOffset,
            @NotNull final Properties consumerProperties,
            final boolean withPartitions) {
        int c = outOffset;

        getCommonCol(
                columnsToSet,
                c,
                consumerProperties,
                KAFKA_PARTITION_COLUMN_NAME_PROPERTY,
                KAFKA_PARTITION_COLUMN_NAME_DEFAULT,
                ColumnDefinition::ofInt);
        if (columnsToSet[c] != null && withPartitions) {
            columnsToSet[c] = columnsToSet[c].withPartitioning();
        }
        ++c;
        getCommonCol(
                columnsToSet,
                c++,
                consumerProperties,
                OFFSET_COLUMN_NAME_PROPERTY,
                OFFSET_COLUMN_NAME_DEFAULT,
                ColumnDefinition::ofLong);
        getCommonCol(
                columnsToSet,
                c++,
                consumerProperties,
                TIMESTAMP_COLUMN_NAME_PROPERTY,
                TIMESTAMP_COLUMN_NAME_DEFAULT,
                (final String colName) -> ColumnDefinition.fromGenericType(colName, DBDateTime.class));
        return c;
    }

    private static ColumnDefinition<?> getKeyOrValueCol(
            @NotNull final Properties properties,
            @NotNull final String typeProperty,
            @NotNull final String deserializerProperty,
            @NotNull final String nameProperty,
            @NotNull final String nameDefault,
            final boolean allowEmpty) {
        final String columnName;
        if (properties.containsKey(nameProperty)) {
            columnName = properties.getProperty(nameProperty);
            if (columnName == null || columnName.equals("")) {
                if (allowEmpty) {
                    return null;
                }
                throw new IllegalArgumentException("Property for " + nameDefault + " can't be empty.");
            }
        } else {
            columnName = nameDefault;
        }

        if (properties.containsKey(typeProperty)) {
            final String typeAsString = properties.getProperty(typeProperty);
            switch (typeAsString) {
                case "int":
                    properties.setProperty(deserializerProperty, INT_DESERIALIZER);
                    return ColumnDefinition.ofInt(columnName);
                case "long":
                    properties.setProperty(deserializerProperty, LONG_DESERIALIZER);
                    return ColumnDefinition.ofLong(columnName);
                case "double":
                    properties.setProperty(deserializerProperty, DOUBLE_DESERIALIZER);
                    return ColumnDefinition.ofDouble(columnName);
                case "byte[]":
                    properties.setProperty(deserializerProperty, BYTE_ARRAY_DESERIALIZER);
                    return ColumnDefinition.fromGenericType(columnName, byte[].class, byte.class);
                case "String":
                case "string":
                    properties.setProperty(deserializerProperty, STRING_DESERIALIZER);
                    return ColumnDefinition.ofString(columnName);
                default:
                    throw new IllegalArgumentException(
                            "Property " + typeProperty + " value " + typeAsString + " not supported");
            }
        } else if (!properties.containsKey(deserializerProperty)) {
            properties.setProperty(deserializerProperty, STRING_DESERIALIZER);
            return ColumnDefinition.ofString(columnName);
        }
        final String deserializer = properties.getProperty(deserializerProperty);
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
        if (STRING_DESERIALIZER.equals(deserializer)) {
            return ColumnDefinition.ofString(columnName);
        }
        throw new IllegalArgumentException(
                "Deserializer type " + deserializer + " for " + deserializerProperty + " not supported.");
    }

    public static Table consumeToTable(
            @NotNull final Properties kafkaConsumerProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter) {
        return consumeToTable(kafkaConsumerProperties, topic, partitionFilter, ALL_PARTITIONS_DONT_SEEK);
    }

    public static Table consumeToTable(
            @NotNull final Properties kafkaConsumerProperties,
            @NotNull final String topic) {
        return consumeToTable(kafkaConsumerProperties, topic, ALL_PARTITIONS);
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
