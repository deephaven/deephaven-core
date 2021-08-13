/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

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
        final List<ColumnDefinition> columnsOut,
        final Map<String, String> mappedOut,
        final String prefix,
        final Schema.Field field,
        final Function<String, String> fieldNameMapping) {
            final Schema fieldSchema = field.schema();
            final String fieldName = field.name();
            final String mappedName = fieldNameMapping.apply(prefix + fieldName);
            if (mappedName == null) {
                // allow the user to specify fields to skip by providing a mapping to null.
                return;
            }
            final Schema.Type fieldType = fieldSchema.getType();
            pushColumnTypesFromAvroField(
                    columnsOut, mappedOut, prefix, field, fieldName, fieldSchema, mappedName, fieldType, fieldNameMapping);

    }

    private static void pushColumnTypesFromAvroField(
            final List<ColumnDefinition> columnsOut,
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

    public static void avroSchemaToColumnDefinitions(
            final List<ColumnDefinition> columns,
            final Map<String, String> mappedOut,
            final Schema schema,
            final Function<String, String> fieldNameMapping
    ) {
        if (schema.isUnion()) {
            throw new UnsupportedOperationException("Union of records schemas are not supported");
        }
        final Schema.Type type = schema.getType();
        if (type != Schema.Type.RECORD) {
            throw new IllegalArgumentException("The schema is not a toplevel record definition.");
        }
        final List<Schema.Field> fields = schema.getFields();
        for (final Schema.Field field : fields) {
            pushColumnTypesFromAvroField(columns, mappedOut, "", field, fieldNameMapping);

        }
    }

    public static void avroSchemaToColumnDefinitions(
            final List<ColumnDefinition> columns,
            final Schema schema,
            final Function<String, String> fieldNameMapping
    ) {
        avroSchemaToColumnDefinitions(columns, null, schema, fieldNameMapping);
    }

    public static void avroSchemaToColumnDefinitions(
            final List<ColumnDefinition> columns,
            final Schema schema
    ) {
        avroSchemaToColumnDefinitions(columns, schema, DIRECT_MAPPING);
    }

    public enum KeyOrValue {
        KEY, VALUE
    }

    public enum DataFormat {
        IGNORE, SIMPLE, AVRO, JSON
    }

    public static abstract class KeyOrValueSpec {
        public abstract DataFormat dataFormat();

        public static final class Ignore extends KeyOrValueSpec {
            @Override public DataFormat dataFormat() { return DataFormat.IGNORE; }
        }

        private static final Ignore IGNORE = new Ignore();

        public static final class Avro extends KeyOrValueSpec {
            public final Schema schema;
            public final Function<String, String> fieldNameMapping;
            private Avro(final Schema schema, final Function<String, String> fieldNameMapping) {
                this.schema = schema;
                this.fieldNameMapping = fieldNameMapping;
            }
            @Override public DataFormat dataFormat() { return DataFormat.AVRO; }
        }

        public static final class Simple extends KeyOrValueSpec {
            public final String columnName;
            public final Class<?> dataType;
            private Simple(final String columnName, final Class<?> dataType) {
                this.columnName = columnName;
                this.dataType = dataType;
            }
            @Override public DataFormat dataFormat() { return DataFormat.SIMPLE; }
        }

        public static final class Json extends KeyOrValueSpec {
            public final ColumnDefinition<?>[] columnDefinitions;
            public final Map<String, String> columnNameToJsonFieldName;
            private Json(
                    final ColumnDefinition<?>[] columnDefinitions,
                    final Map<String, String> columnNameToJsonFieldName
            ) {
                this.columnDefinitions = columnDefinitions;
                this.columnNameToJsonFieldName = columnNameToJsonFieldName;
            }
            @Override public DataFormat dataFormat() { return DataFormat.JSON; }
        }
    }

    public KeyOrValueSpec ignoreSpec() {
        return KeyOrValueSpec.IGNORE;
    }

    public KeyOrValueSpec jsonSpec(
            final ColumnDefinition<?>[] columnDefinitions,
            final Map<String, String> columnNameToJsonFieldName
    ) {
        return new KeyOrValueSpec.Json(columnDefinitions, columnNameToJsonFieldName);
    }

    public KeyOrValueSpec jsonSpec(final ColumnDefinition<?>[] columnDefinitions) {
        return jsonSpec(columnDefinitions, null);
    }

    public KeyOrValueSpec avroSpec(final Schema schema, final Function<String, String> fieldNameMapping) {
        return new KeyOrValueSpec.Avro(schema, fieldNameMapping);
    }

    public KeyOrValueSpec avroSpec(final Schema schema) {
        return new KeyOrValueSpec.Avro(schema, Function.identity());
    }

    public KeyOrValueSpec simpleSpec(final String columnName, final Class<?> dataType) {
        return new KeyOrValueSpec.Simple(columnName, dataType);
    }

    public KeyOrValueSpec simpleSpec(final String columnName) {
        return new KeyOrValueSpec.Simple(columnName, null);
    }

    public KeyOrValueSpec simpleSpec() {
        return new KeyOrValueSpec.Simple(null, null);
    }

    public static Table consumeToTable(
            @NotNull final Properties kafkaConsumerProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final IntToLongFunction partitionToInitialOffset,
            @NotNull final KeyOrValueSpec keySpec,
            @NotNull final KeyOrValueSpec valueSpec) {
        if (keySpec.dataFormat() == DataFormat.IGNORE && keySpec.dataFormat() == DataFormat.IGNORE) {
            throw new IllegalArgumentException(
                    "can't ignore both key and value: keySpec and valueSpec can't both be ignore specs");
        }

        final ColumnDefinition<?>[] commonColumns = new ColumnDefinition<?>[3];
        getCommonCols(commonColumns, 0, kafkaConsumerProperties, partitionFilter == ALL_PARTITIONS);
        final List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        int[] commonColumnIndices = new int[3];
        int nextColumnIndex = 0;
        for (int i = 0; i < 3; ++i) {
            if (commonColumns[i] != null) {
                commonColumnIndices[i] = nextColumnIndex++;
                columnDefinitions.add(commonColumns[i]);
            } else {
                commonColumnIndices[i] = -1;
            }
        }

        final MutableInt nextColumnIndexMut = new MutableInt(nextColumnIndex);
        final KeyOrValueIngestData keyIngestData =
                getIngestData(KeyOrValue.KEY, kafkaConsumerProperties, columnDefinitions, nextColumnIndexMut, keySpec);
        final KeyOrValueIngestData valueIngestData =
                getIngestData(KeyOrValue.VALUE, kafkaConsumerProperties, columnDefinitions, nextColumnIndexMut, valueSpec);

        final TableDefinition tableDefinition = new TableDefinition(columnDefinitions);

        final StreamPublisherImpl streamPublisher = new StreamPublisherImpl();
        final StreamToTableAdapter streamToTableAdapter = new StreamToTableAdapter(tableDefinition, streamPublisher, LiveTableMonitor.DEFAULT);
        streamPublisher.setChunkFactory(() -> streamToTableAdapter.makeChunksForDefinition(CHUNK_SIZE), streamToTableAdapter::chunkTypeForIndex);


        final KeyOrValueProcessor keyProcessor = getProcessor(keySpec, tableDefinition, streamToTableAdapter, keyIngestData);
        final KeyOrValueProcessor valueProcessor = getProcessor(valueSpec, tableDefinition, streamToTableAdapter, valueIngestData);

        final ConsumerRecordToStreamPublisherAdapter adapter = KafkaStreamPublisher.make(streamPublisher,
                commonColumnIndices[0],
                commonColumnIndices[1],
                commonColumnIndices[2],
                keyProcessor, valueProcessor,
                Function.identity(),
                Function.identity(),
                keyIngestData == null ? -1 : keyIngestData.simpleColumnIndex,
                valueIngestData == null ? -1 : valueIngestData.simpleColumnIndex
        );

        final KafkaIngester ingester = new KafkaIngester(
                log,
                kafkaConsumerProperties,
                topic,
                partitionFilter,
                (int partition) -> new SimpleKafkaStreamConsumer(adapter, streamToTableAdapter),
                partitionToInitialOffset
        );
        ingester.start();

        return streamToTableAdapter.table();
    }

    private static KeyOrValueProcessor getProcessor(
            final KeyOrValueSpec spec,
            final TableDefinition tableDef,
            final StreamToTableAdapter streamToTableAdapter,
            final KeyOrValueIngestData data
    ) {
        switch (spec.dataFormat()) {
            case IGNORE:
            case SIMPLE:
                return null;
            case AVRO:
                return GenericRecordChunkAdapter.make(
                        tableDef, streamToTableAdapter::chunkTypeForIndex, data.fieldNamesToColumnNamesMap, true);
            case JSON:
                return JsonNodeChunkAdapter.make(
                        tableDef, streamToTableAdapter::chunkTypeForIndex, data.fieldNamesToColumnNamesMap, true);
            default:
                throw new IllegalStateException("Unknown KeyOrvalueSpec value" + spec.dataFormat());
        }
    }

    private static class KeyOrValueIngestData {
        public Map<String, String> fieldNamesToColumnNamesMap;
        public int simpleColumnIndex = -1;
    }

    private static void setIfNotSet(final Properties prop, final String key, final String value) {
        if (prop.containsKey(key)) {
            return;
        }
        prop.setProperty(key, value);
    }

    private static void setDeserIfNotSet(final Properties prop, final KeyOrValue keyOrValue, final String value) {
        final String propKey = (keyOrValue == KeyOrValue.KEY)
                ? ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
                : ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
                ;
        setIfNotSet(prop, propKey, value);
    }

    private static KeyOrValueIngestData getIngestData(
            final KeyOrValue keyOrValue,
            final Properties kafkaConsumerProperties,
            final List<ColumnDefinition> columnDefinitions,
            final MutableInt nextColumnIndexMut,
            final KeyOrValueSpec keyOrValueSpec
    ) {
        if (keyOrValueSpec.dataFormat() == DataFormat.IGNORE) {
            return null;
        }
        final KeyOrValueIngestData data = new KeyOrValueIngestData();
        switch (keyOrValueSpec.dataFormat()) {
            case AVRO:
                setDeserIfNotSet(kafkaConsumerProperties, keyOrValue, KafkaAvroDeserializer.class.getName());
                final KeyOrValueSpec.Avro avroSpec = (KeyOrValueSpec.Avro) keyOrValueSpec;
                data.fieldNamesToColumnNamesMap = new HashMap<>();
                avroSchemaToColumnDefinitions(
                        columnDefinitions, data.fieldNamesToColumnNamesMap, avroSpec.schema, avroSpec.fieldNameMapping);
                break;
            case JSON:
                setDeserIfNotSet(kafkaConsumerProperties, keyOrValue, STRING_DESERIALIZER);
                final KeyOrValueSpec.Json jsonSpec = (KeyOrValueSpec.Json) keyOrValueSpec;
                columnDefinitions.addAll(Arrays.asList(jsonSpec.columnDefinitions));
                data.fieldNamesToColumnNamesMap = new HashMap<>();
                for (final ColumnDefinition<?> colDef : jsonSpec.columnDefinitions) {
                    final String colName = colDef.getName();
                    final String fieldName = (jsonSpec.columnNameToJsonFieldName == null)
                            ? colName
                            : jsonSpec.columnNameToJsonFieldName.getOrDefault(colName, colName);
                    data.fieldNamesToColumnNamesMap.put(colName, fieldName);
                }
                break;
            case SIMPLE:
                setDeserIfNotSet(kafkaConsumerProperties, keyOrValue, STRING_DESERIALIZER);
                data.simpleColumnIndex = nextColumnIndexMut.getAndAdd(1);
                final KeyOrValueSpec.Simple simpleSpec = (KeyOrValueSpec.Simple) keyOrValueSpec;
                final ColumnDefinition colDef;
                if (simpleSpec.dataType == null) {
                    colDef = getKeyOrValueCol(keyOrValue, kafkaConsumerProperties, simpleSpec.columnName, false);
                } else {
                    colDef = ColumnDefinition.fromGenericType(simpleSpec.columnName, simpleSpec.dataType);
                }
                columnDefinitions.add(colDef);
                break;
            default:
                throw new IllegalStateException("Unhandled spec type:" + keyOrValueSpec.dataFormat());
        }
        return data;
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

        final ColumnDefinition<?>[] commonColumns = new ColumnDefinition<?>[3];
        getCommonCols(commonColumns, 0, kafkaConsumerProperties, partitionFilter == ALL_PARTITIONS);
        final List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        int[] commonColumnIndices = new int[3];
        int nextColumnIndex = 0;
        for (int i = 0; i < 3; ++i) {
            if (commonColumns[i] != null) {
                commonColumnIndices[i] = nextColumnIndex++;
                columnDefinitions.add(commonColumns[i]);
            } else {
                commonColumnIndices[i] = -1;
            }
        }

        final Map<String, String> keyColumnsMap;
        if (keySchema != null) {
            keyColumnsMap = new HashMap<>();
            avroSchemaToColumnDefinitions(columnDefinitions, keyColumnsMap, keySchema, keyFieldNameMapping);
        } else {
            keyColumnsMap = Collections.emptyMap();
        }

        final Map<String, String> valueColumnsMap;
        if (valueSchema != null) {
            valueColumnsMap = new HashMap<>();
            avroSchemaToColumnDefinitions(columnDefinitions, valueColumnsMap, valueSchema, valueFieldNameMapping);
        } else {
            valueColumnsMap = Collections.emptyMap();
        }

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

        final TableDefinition tableDefinition = new TableDefinition(columnDefinitions);

        final StreamPublisherImpl streamPublisher = new StreamPublisherImpl();
        final StreamToTableAdapter streamToTableAdapter = new StreamToTableAdapter(tableDefinition, streamPublisher, LiveTableMonitor.DEFAULT);
        streamPublisher.setChunkFactory(() -> streamToTableAdapter.makeChunksForDefinition(CHUNK_SIZE), streamToTableAdapter::chunkTypeForIndex);


        final KeyOrValueProcessor keyProcessor;
        if (keySchema == null) {
            keyProcessor = null; // TODO: if we have a primitive type, it goes here: https://github.com/deephaven/deephaven-core/issues/1025
        } else {
            keyProcessor = GenericRecordChunkAdapter.make(tableDefinition, streamToTableAdapter::chunkTypeForIndex, keyColumnsMap, true);
        }

        final KeyOrValueProcessor valueProcessor;
        if (valueSchema == null) {
            valueProcessor = null; // TODO: if we have a primitive type, it goes here: https://github.com/deephaven/deephaven-core/issues/1025
        } else {
            valueProcessor = GenericRecordChunkAdapter.make(tableDefinition, streamToTableAdapter::chunkTypeForIndex, valueColumnsMap, true);
        }

        final ConsumerRecordToStreamPublisherAdapter adapter = KafkaStreamPublisher.make(streamPublisher,
                commonColumnIndices[0],
                commonColumnIndices[1],
                commonColumnIndices[2],
                keyProcessor, valueProcessor,
                Function.identity(),
                Function.identity(),
                -1, // TODO A RAW STRING WOULD GO HERE: https://github.com/deephaven/deephaven-core/issues/1025
                -1 // TODO A RAW STRING WOULD GO HERE: https://github.com/deephaven/deephaven-core/issues/1025
                );

        final KafkaIngester ingester = new KafkaIngester(
                log,
                kafkaConsumerProperties,
                topic,
                partitionFilter,
                (int partition) -> new SimpleKafkaStreamConsumer(adapter, streamToTableAdapter),
                partitionToInitialOffset
        );
        ingester.start();

        return streamToTableAdapter.table();
    }

    public static Table consumeJsonToTable(
            @NotNull final Properties consumerProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final IntToLongFunction partitionToInitialOffset,
            final ColumnDefinition<?>[] valueColumns) {
        return consumeJsonToTable(consumerProperties, topic, partitionFilter, partitionToInitialOffset, valueColumns, null);
    }

    public static Table consumeJsonToTable(
            @NotNull final Properties consumerProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final IntToLongFunction partitionToInitialOffset,
            final ColumnDefinition<?>[] valueColumns,
            final Map<String, String> columnNameToJsonField) {
        final int nCols = 3 + valueColumns.length;
        final ColumnDefinition<?>[] commonColumns = new ColumnDefinition[nCols];
        int c = 0;
        c += getCommonCols(commonColumns, 0, consumerProperties, partitionFilter == ALL_PARTITIONS);
        final int[] commonColsIndices = new int[3];
        int nextColumnIndex = 0;
        for (int i = 0; i < 3; ++i) {
            if (commonColumns[i] != null) {
                commonColsIndices[i] = nextColumnIndex++;
            } else {
                commonColsIndices[i] = -1;
            }
        }
        System.arraycopy(valueColumns, 0, commonColumns, c, valueColumns.length);
        final Map<String, String> valueColumnsMap = new HashMap<>(valueColumns.length);
        for (final ColumnDefinition<?> colDef : valueColumns) {
            final String colName = colDef.getName();
            final String fieldName = (columnNameToJsonField == null) ? colName : columnNameToJsonField.getOrDefault(colName, colName);
            valueColumnsMap.put(colName, fieldName);
        }

        final TableDefinition tableDefinition = new TableDefinition(commonColumns);

        final StreamPublisherImpl streamPublisher = new StreamPublisherImpl();
        final StreamToTableAdapter streamToTableAdapter = new StreamToTableAdapter(tableDefinition, streamPublisher, LiveTableMonitor.DEFAULT);
        streamPublisher.setChunkFactory(() -> streamToTableAdapter.makeChunksForDefinition(CHUNK_SIZE), streamToTableAdapter::chunkTypeForIndex);

        final KeyOrValueProcessor keyProcessor = null; // TODO: Support key as both json or generic.
        final KeyOrValueProcessor valueProcessor = JsonNodeChunkAdapter.make(tableDefinition, streamToTableAdapter::chunkTypeForIndex, valueColumnsMap, true);

        final Function<Object, Object> toObjectChunkMapper = (final Object in) -> {
            final String json;
            try {
                json = (String) in;
            } catch (ClassCastException ex) {
                throw new UncheckedDeephavenException("Could not convert input to json string", ex);
            }
            return JsonNodeUtil.makeJsonNode(json);
        };
        final ConsumerRecordToStreamPublisherAdapter adapter = KafkaStreamPublisher.make(
                streamPublisher,
                commonColsIndices[0],
                commonColsIndices[1],
                commonColsIndices[2],
                keyProcessor, valueProcessor,
                toObjectChunkMapper,
                toObjectChunkMapper,
                -1, // TODO A RAW STRING WOULD GO HERE: https://github.com/deephaven/deephaven-core/issues/1025
                -1 // TODO A RAW STRING WOULD GO HERE: https://github.com/deephaven/deephaven-core/issues/1025
        );

        if (!consumerProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
        }

        if (!consumerProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
        }

        final KafkaIngester ingester = new KafkaIngester(
                log,
                consumerProperties,
                topic,
                partitionFilter,
                (int partition) -> new SimpleKafkaStreamConsumer(adapter, streamToTableAdapter),
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
                KeyOrValue.KEY,
                consumerProperties,
                null,
                true);
        columns[c++] = getKeyOrValueCol(
                KeyOrValue.VALUE,
                consumerProperties,
                null,
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
        final ConsumerRecordToStreamPublisherAdapter adapter = KafkaStreamPublisher.make(
                streamPublisher,
                Function.identity(),
                Function.identity(),
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
                (int partition) -> new SimpleKafkaStreamConsumer(adapter, streamToTableAdapter),
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
            @NotNull final KeyOrValue keyOrValue,
            @NotNull final Properties properties,
            final String columnNameArg,
            final boolean allowEmpty
    ) {
        final String typeProperty;
        final String deserializerProperty;
        final String nameProperty;
        final String nameDefault;
        switch(keyOrValue) {
            case KEY:
                typeProperty = KEY_COLUMN_TYPE_PROPERTY;
                deserializerProperty = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
                nameProperty = KEY_COLUMN_NAME_PROPERTY;
                nameDefault =KEY_COLUMN_NAME_DEFAULT;
                break;
            case VALUE:
                typeProperty = VALUE_COLUMN_TYPE_PROPERTY;
                deserializerProperty = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
                nameProperty = VALUE_COLUMN_NAME_PROPERTY;
                nameDefault = VALUE_COLUMN_NAME_DEFAULT;
                break;
            default:
                throw new IllegalStateException("Unrecognized KeyOrValue value " + keyOrValue);
        }

        final String columnName;
        if (columnNameArg != null) {
            columnName = columnNameArg;
        } else if (properties.containsKey(nameProperty)) {
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

    private static class SimpleKafkaStreamConsumer implements KafkaStreamConsumer {
        private final ConsumerRecordToStreamPublisherAdapter adapter;
        private final StreamToTableAdapter streamToTableAdapter;

        public SimpleKafkaStreamConsumer(ConsumerRecordToStreamPublisherAdapter adapter, StreamToTableAdapter streamToTableAdapter) {
            this.adapter = adapter;
            this.streamToTableAdapter = streamToTableAdapter;
        }

        @Override
        public void accept(List<? extends ConsumerRecord<?, ?>> consumerRecords) {
            try {
                adapter.consumeRecords(consumerRecords);
            } catch (Exception e) {
                acceptFailure(e);
            }
        }

        @Override
        public void acceptFailure(@NotNull Exception cause) {
            streamToTableAdapter.acceptFailure(cause);
        }
    }
}
