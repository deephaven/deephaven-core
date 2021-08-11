package io.deephaven.kafka;

import gnu.trove.map.hash.TIntLongHashMap;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.kafka.ingest.*;
import io.deephaven.stream.StreamToTableAdapter;
import org.apache.avro.Schema;
import org.apache.commons.codec.Charsets;
import org.apache.commons.lang3.mutable.MutableInt;
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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.*;

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
    public static final String NESTED_FIELD_NAME_SEPARATOR = ".";

    private static final Logger log = LoggerFactory.getLogger(KafkaTools .class);

    private static final int CHUNK_SIZE = 2048;

    // offsets in the consumeToTable output
    private static final int KAFKA_PARTITION_COLUMN_INDEX = 0;
    private static final int OFFSET_COLUMN_INDEX = 1;
    private static final int TIMESTAMP_COLUMN_INDEX = 2;

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

    private static void pushColumnTypesFromAvroField(
        final List<ColumnDefinition<?>> columnsOut,
        final Map<String, String> mappedOut,
        final String prefix,
        final Schema.Field field,
        final Function<String, String> fieldNameMapping) {
            final Schema fieldSchema = field.schema();
            final String fieldName = field.name();
            final String mappedName = fieldNameMapping.apply(prefix + fieldName);
            final Schema.Type fieldType = fieldSchema.getType();
            pushColumnTypesFromAvroField(
                    columnsOut, mappedOut, prefix, field, fieldName, fieldSchema, mappedName, fieldType, fieldNameMapping);

    }

    private static void pushColumnTypesFromAvroField(
            final List<ColumnDefinition<?>> columnsOut,
            final Map<String, String> mappedOut,
            final String prefix,
            final Schema.Field field,
            final String fieldName,
            final Schema fieldSchema,
            final String mappedName,
            final Schema.Type fieldType,
            final Function<String, String> fieldNameMapping) {
        switch (fieldType) {
            case BOOLEAN:
                columnsOut.add(ColumnDefinition.ofBoolean(mappedName));
                break;
            case INT:
                columnsOut.add(ColumnDefinition.ofInt(mappedName));
                break;
            case LONG:
                columnsOut.add(ColumnDefinition.ofLong(mappedName));
                break;
            case FLOAT:
                columnsOut.add(ColumnDefinition.ofFloat(mappedName));
                break;
            case DOUBLE:
                columnsOut.add(ColumnDefinition.ofDouble(mappedName));
                break;
            case STRING:
                columnsOut.add(ColumnDefinition.ofString(mappedName));
                break;
            case UNION:
                final List<Schema> unionTypes = fieldSchema.getTypes();
                final int unionSize = unionTypes.size();
                if (unionSize == 0) {
                    throw new IllegalArgumentException("empty union " + fieldName);
                }
                if (unionSize != 2) {
                    throw new UnsupportedOperationException("Union " + fieldName + " with more than 2 fields not supported");
                }
                final Schema.Type unionType0 = unionTypes.get(0).getType();
                final Schema.Type unionType1 = unionTypes.get(1).getType();
                if (unionType1 == Schema.Type.NULL) {
                    pushColumnTypesFromAvroField(
                            columnsOut, mappedOut, prefix, field, fieldName, fieldSchema, mappedName, unionType0, fieldNameMapping);
                    return;
                }
                else if (unionType0 == Schema.Type.NULL) {
                    pushColumnTypesFromAvroField(
                            columnsOut, mappedOut, prefix, field, fieldName, fieldSchema, mappedName, unionType1, fieldNameMapping);
                    return;
                }
                throw new UnsupportedOperationException("Union " + fieldName + " not supported; only unions with NULL are supported at this time.");
            case RECORD:
                // Linearize any nesting.
                for (final Schema.Field nestedField : field.schema().getFields()) {
                    pushColumnTypesFromAvroField(
                            columnsOut, mappedOut,
                            prefix + fieldName + NESTED_FIELD_NAME_SEPARATOR,
                            nestedField,
                            fieldNameMapping);
                }
                return;
            case MAP:
            case ENUM:
            case NULL:
            case ARRAY:
            case BYTES:
            case FIXED:
            default:
                throw new UnsupportedOperationException("Type " + fieldType + " not supported for field " + fieldName);
        }
        if (mappedOut != null) {
            mappedOut.put(fieldName, mappedName);
        }
    }

    public static ColumnDefinition<?>[] avroSchemaToColumnDefinitions(
            final Map<String, String> mappedOut, final Schema schema, final Function<String, String> fieldNameMapping) {
        if (schema.isUnion()) {
            throw new UnsupportedOperationException("Union of records schemas are not supported");
        }
        final Schema.Type type = schema.getType();
        if (type != Schema.Type.RECORD) {
            throw new IllegalArgumentException("The schema is not a toplevel record definition.");
        }
        final List<Schema.Field> fields = schema.getFields();
        final ArrayList<ColumnDefinition<?>> columns = new ArrayList<>(fields.size());
        for (final Schema.Field field : fields) {
            pushColumnTypesFromAvroField(columns, mappedOut, "", field, fieldNameMapping);

        }
        return columns.toArray(new ColumnDefinition<?>[columns.size()]);
    }

    public static ColumnDefinition<?>[] avroSchemaToColumnDefinitions(final Schema schema, final Function<String, String> fieldNameMapping) {
        return avroSchemaToColumnDefinitions(null, schema, fieldNameMapping);
    }

    public static ColumnDefinition<?>[] avroSchemaToColumnDefinitions(final Schema schema) {
        return avroSchemaToColumnDefinitions(schema, DIRECT_MAPPING);
    }

    /**
     * Consume from Kafka to a Deephaven live table using avro schemas.
     *
     * @param kafkaConsumerProperties
     * @param topic
     * @param partitionFilter
     * @param partitionToInitialOffset
     * @param keySchema                Avro schema for the key, or null if no key expected
     * @param keyFieldNameMapping      Mapping of key schema field names to deephaven table column names, or null if no key expected
     * @param valueSchema              Avro schema for the value
     * @param valueFieldNameMapping    Mapping of key schema field names to deephaven table column names
     * @return                         The live table where kafka events are ingested
     */
    public static Table consumeToTable(
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

        final KeyOrValueProcessor keyProcessor;
        final KeyOrValueProcessor valueProcessor;

        final List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        final ColumnDefinition<?> partitionColumn = ColumnDefinition.ofInt(KAFKA_PARTITION_COLUMN_NAME_DEFAULT);
        columnDefinitions.add(partitionFilter == ALL_PARTITIONS ? partitionColumn : partitionColumn.withPartitioning());
        columnDefinitions.add(ColumnDefinition.ofLong(OFFSET_COLUMN_NAME_DEFAULT));
        columnDefinitions.add(ColumnDefinition.fromGenericType(TIMESTAMP_COLUMN_NAME_DEFAULT, DBDateTime.class));

        if (!kafkaConsumerProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            if (keySchema != null) {
                kafkaConsumerProperties.setProperty(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
                columnDefinitions.addAll(Arrays.asList(keyColumns));
            } else {
                kafkaConsumerProperties.setProperty(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
            }
        } else if (keySchema != null) {
            columnDefinitions.addAll(Arrays.asList(keyColumns));
        }

        if (!kafkaConsumerProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            if (valueSchema != null) {
                kafkaConsumerProperties.setProperty(
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
                columnDefinitions.addAll(Arrays.asList(valueColumns));
            } else {
                kafkaConsumerProperties.setProperty(
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
            }
        } else if (valueSchema != null) {
            columnDefinitions.addAll(Arrays.asList(valueColumns));
        }

        final TableDefinition tableDefinition = new TableDefinition(columnDefinitions);

        final StreamPublisherImpl streamPublisher = new StreamPublisherImpl();
        final StreamToTableAdapter streamToTableAdapter = new StreamToTableAdapter(tableDefinition, streamPublisher, LiveTableMonitor.DEFAULT);
        streamPublisher.setChunkFactory(() -> streamToTableAdapter.makeChunksForDefinition(CHUNK_SIZE), streamToTableAdapter::chunkTypeForIndex);


        if (keySchema == null) {
            keyProcessor = null; // TODO: if we have a primitive type, it goes here: https://github.com/deephaven/deephaven-core/issues/1025
        } else {
            keyProcessor = GenericRecordChunkAdapter.make(tableDefinition, streamToTableAdapter::chunkTypeForIndex, keyColumnsMap, true);
        }

        if (valueSchema == null) {
            valueProcessor = null; // TODO: if we have a primitive type, it goes here: https://github.com/deephaven/deephaven-core/issues/1025
        } else {
            valueProcessor = GenericRecordChunkAdapter.make(tableDefinition, streamToTableAdapter::chunkTypeForIndex, valueColumnsMap, true);
        }

        final ConsumerRecordToStreamPublisherAdapter adapter = SimpleConsumerRecordToStreamPublisherAdapter.make(streamPublisher,
                KAFKA_PARTITION_COLUMN_INDEX,
                OFFSET_COLUMN_INDEX,
                TIMESTAMP_COLUMN_INDEX,
                keyProcessor, valueProcessor,
                -1, // TODO A RAW STRING WOULD GO HERE: https://github.com/deephaven/deephaven-core/issues/1025
                -1 // TODO A RAW STRING WOULD GO HERE: https://github.com/deephaven/deephaven-core/issues/1025
                );

        final KafkaIngester ingester = new KafkaIngester(
                log,
                kafkaConsumerProperties,
                topic,
                partitionFilter,
                (int partition) -> (List<? extends ConsumerRecord<?, ?>> records) -> {
                    try {
                        adapter.consumeRecords(records);
                    } catch (IOException ex) {
                        throw new UncheckedDeephavenException(ex);
                    }
                },
                partitionToInitialOffset
        );
        ingester.start();

        return streamToTableAdapter.table();
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

        final StreamPublisherImpl streamPublisher = new StreamPublisherImpl();

        final StreamToTableAdapter streamToTableAdapter = new StreamToTableAdapter(tableDefinition, streamPublisher, LiveTableMonitor.DEFAULT);
        streamPublisher.setChunkFactory(() -> streamToTableAdapter.makeChunksForDefinition(CHUNK_SIZE), streamToTableAdapter::chunkTypeForIndex);

        final MutableInt colIdx = new MutableInt();
        final ToIntFunction<ColumnDefinition<?>> orNull = (final ColumnDefinition<?> colDef) -> {
            if (colDef == null) {
                return -1;
            }
            return colIdx.getAndIncrement();
        };
        final ConsumerRecordToStreamPublisherAdapter adapter = SimpleConsumerRecordToStreamPublisherAdapter.make(
                streamPublisher,
                orNull.applyAsInt(columns[0]),
                orNull.applyAsInt(columns[1]),
                orNull.applyAsInt(columns[2]),
                orNull.applyAsInt(columns[3]),
                orNull.applyAsInt(columns[4]));

        final KafkaIngester ingester = new KafkaIngester(
                log,
                consumerProperties,
                topic,
                partitionFilter,
                (int partition) -> (List<? extends ConsumerRecord<?, ?>> records) -> {
                    try {
                        adapter.consumeRecords(records);
                    } catch (IOException ex) {
                        throw new UncheckedDeephavenException(ex);
                    }
                },
                partitionToInitialOffset
        );
        ingester.start();

        return streamToTableAdapter.table();
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
        return columnDefinitionFromDeserializer(properties, deserializerProperty, columnName);
    }

    @NotNull
    private static ColumnDefinition<? extends Serializable> columnDefinitionFromDeserializer(@NotNull Properties properties, @NotNull String deserializerProperty, String columnName) {
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
