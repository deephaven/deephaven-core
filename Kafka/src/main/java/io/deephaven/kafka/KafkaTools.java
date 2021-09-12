/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka;

import gnu.trove.map.hash.TIntLongHashMap;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.live.LiveTableRefreshCombiner;
import io.deephaven.db.tables.live.LiveTableRegistrar;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.LocalTableMap;
import io.deephaven.db.v2.StreamTableTools;
import io.deephaven.db.v2.TableMap;
import io.deephaven.db.v2.TransformableTableMap;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.kafka.ingest.*;
import io.deephaven.stream.StreamToTableAdapter;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
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

import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.HttpClients;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
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
    public static final String SCHEMA_SERVER_PROPERTY = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
    public static final String SHORT_DESERIALIZER = ShortDeserializer.class.getName();
    public static final String INT_DESERIALIZER = IntegerDeserializer.class.getName();
    public static final String LONG_DESERIALIZER = LongDeserializer.class.getName();
    public static final String FLOAT_DESERIALIZER = FloatDeserializer.class.getName();
    public static final String DOUBLE_DESERIALIZER = DoubleDeserializer.class.getName();
    public static final String BYTE_ARRAY_DESERIALIZER = ByteArrayDeserializer.class.getName();
    public static final String STRING_DESERIALIZER = StringDeserializer.class.getName();
    public static final String BYTE_BUFFER_DESERIALIZER = ByteBufferDeserializer.class.getName();
    public static final String DESERIALIZER_FOR_IGNORE = BYTE_BUFFER_DESERIALIZER;
    public static final String NESTED_FIELD_NAME_SEPARATOR = ".";
    public static final String AVRO_LATEST_VERSION = "latest";

    private static final Logger log = LoggerFactory.getLogger(KafkaTools.class);

    private static final int CHUNK_SIZE = 2048;

    /**
     * Fetch an Avro schema from a Confluent compatible Schema Server.
     *
     * @param schemaServerUrl The schema server URL
     * @param resourceName The resource name that the schema is known as in the schema server
     * @param version The version to fetch, or the string "latest" for the latest version.
     * @return An Avro schema.
     */
    public static Schema getAvroSchema(final String schemaServerUrl, final String resourceName, final String version) {
        String action = "setup http client";
        try (final CloseableHttpClient client = HttpClients.custom().build()) {
            final String requestStr =
                    schemaServerUrl + "/subjects/" + resourceName + "/versions/" + version + "/schema";
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
     * Fetch the latest version of an Avro schema from a Confluent compatible Schema Server.
     *
     * @param schemaServerUrl The schema server URL
     * @param resourceName The resource name that the schema is known as in the schema server
     * @return An Avro schema.
     */
    @SuppressWarnings("unused")
    public static Schema getAvroSchema(final String schemaServerUrl, final String resourceName) {
        return getAvroSchema(schemaServerUrl, resourceName, AVRO_LATEST_VERSION);
    }

    private static void pushColumnTypesFromAvroField(
            final List<ColumnDefinition<?>> columnsOut,
            final Map<String, String> mappedOut,
            final String prefix,
            final Schema.Field field,
            final Function<String, String> fieldNameToColumnName) {
        final Schema fieldSchema = field.schema();
        final String fieldName = field.name();
        final String mappedName = fieldNameToColumnName.apply(prefix + fieldName);
        if (mappedName == null) {
            // allow the user to specify fields to skip by providing a mapping to null.
            return;
        }
        final Schema.Type fieldType = fieldSchema.getType();
        pushColumnTypesFromAvroField(
                columnsOut, mappedOut, prefix, field, fieldName, fieldSchema, mappedName, fieldType,
                fieldNameToColumnName);

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
            final Function<String, String> fieldNameToColumnName) {
        switch (fieldType) {
            case BOOLEAN:
                columnsOut.add(ColumnDefinition.ofBoolean(mappedName));
                break;
            case INT:
                columnsOut.add(ColumnDefinition.ofInt(mappedName));
                break;
            case LONG:
                final LogicalType logicalType = fieldSchema.getLogicalType();
                if (LogicalTypes.timestampMicros().equals(logicalType) ||
                        LogicalTypes.timestampMillis().equals(logicalType)) {
                    columnsOut.add(ColumnDefinition.ofTime(mappedName));
                } else {
                    columnsOut.add(ColumnDefinition.ofLong(mappedName));
                }
                break;
            case FLOAT:
                columnsOut.add(ColumnDefinition.ofFloat(mappedName));
                break;
            case DOUBLE:
                columnsOut.add(ColumnDefinition.ofDouble(mappedName));
                break;
            case ENUM:
            case STRING:
                columnsOut.add(ColumnDefinition.ofString(mappedName));
                break;
            case UNION:
                final Schema effectiveSchema = Utils.getEffectiveSchema(fieldName, fieldSchema);
                pushColumnTypesFromAvroField(
                        columnsOut, mappedOut, prefix, field, fieldName, effectiveSchema, mappedName,
                        effectiveSchema.getType(),
                        fieldNameToColumnName);
                return;
            case RECORD:
                // Linearize any nesting.
                for (final Schema.Field nestedField : field.schema().getFields()) {
                    pushColumnTypesFromAvroField(
                            columnsOut, mappedOut,
                            prefix + fieldName + NESTED_FIELD_NAME_SEPARATOR,
                            nestedField,
                            fieldNameToColumnName);
                }
                return;
            case MAP:
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
            final List<ColumnDefinition<?>> columns,
            final Map<String, String> mappedOut,
            final Schema schema,
            final Function<String, String> fieldNameToColumnName) {
        if (schema.isUnion()) {
            throw new UnsupportedOperationException("Union of records schemas are not supported");
        }
        final Schema.Type type = schema.getType();
        if (type != Schema.Type.RECORD) {
            throw new IllegalArgumentException("The schema is not a toplevel record definition.");
        }
        final List<Schema.Field> fields = schema.getFields();
        for (final Schema.Field field : fields) {
            pushColumnTypesFromAvroField(columns, mappedOut, "", field, fieldNameToColumnName);

        }
    }

    /**
     * Convert an Avro schema to a list of column definitions.
     *
     * @param columns Column definitions for output; should be empty on entry.
     * @param schema Avro schema
     * @param fieldNameToColumnName An optional mapping to specify selection and naming of columns from Avro fields, or
     *        null for map all fields using field name for column name.
     */
    public static void avroSchemaToColumnDefinitions(
            final List<ColumnDefinition<?>> columns,
            final Schema schema,
            final Function<String, String> fieldNameToColumnName) {
        avroSchemaToColumnDefinitions(columns, null, schema, fieldNameToColumnName);
    }

    /**
     * Convert an Avro schema to a list of column definitions, mapping every avro field to a column of the same name.
     *
     * @param columns Column definitions for output; should be empty on entry.
     * @param schema Avro schema
     */
    public static void avroSchemaToColumnDefinitions(
            final List<ColumnDefinition<?>> columns,
            final Schema schema) {
        avroSchemaToColumnDefinitions(columns, schema, DIRECT_MAPPING);
    }

    /**
     * Enum to specify operations that may apply to either of Kafka KEY or VALUE fields.
     */
    public enum KeyOrValue {
        KEY, VALUE
    }

    /**
     * Enum to specify the expected processing (format) for Kafka KEY or VALUE fields.
     */
    public enum DataFormat {
        IGNORE, SIMPLE, AVRO, JSON
    }

    /**
     * Class to specify conversion of Kafka KEY or VALUE fields to table columns.
     */
    public static abstract class KeyOrValueSpec {
        /**
         * Data format for this Spec.
         * 
         * @return Data format for this Spec
         */
        public abstract DataFormat dataFormat();

        private static final class Ignore extends KeyOrValueSpec {
            @Override
            public DataFormat dataFormat() {
                return DataFormat.IGNORE;
            }
        }

        private static final Ignore IGNORE = new Ignore();

        /**
         * Avro spec.
         */
        public static final class Avro extends KeyOrValueSpec {
            public final Schema schema;
            public final String schemaName;
            public final String schemaVersion;
            public final Function<String, String> fieldNameToColumnName;

            private Avro(final Schema schema, final Function<String, String> fieldNameToColumnName) {
                this.schema = schema;
                this.schemaName = null;
                this.schemaVersion = null;
                this.fieldNameToColumnName = fieldNameToColumnName;
            }

            private Avro(final String schemaName,
                    final String schemaVersion,
                    final Function<String, String> fieldNameToColumnName) {
                this.schema = null;
                this.schemaName = schemaName;
                this.schemaVersion = schemaVersion;
                this.fieldNameToColumnName = fieldNameToColumnName;
            }

            @Override
            public DataFormat dataFormat() {
                return DataFormat.AVRO;
            }
        }

        /**
         * Single spec for unidimensional (basic Kafka encoded for one type) fields.
         */
        public static final class Simple extends KeyOrValueSpec {
            public final String columnName;
            public final Class<?> dataType;

            private Simple(final String columnName, final Class<?> dataType) {
                this.columnName = columnName;
                this.dataType = dataType;
            }

            @Override
            public DataFormat dataFormat() {
                return DataFormat.SIMPLE;
            }
        }

        /**
         * The names for the key or value columns can be provided in the properties as "key.column.name" or
         * "value.column.name", and otherwise default to "key" or "value". The types for key or value are either
         * specified in the properties as "key.type" or "value.type", or deduced from the serializer classes for key or
         * value in the provided Properties object.
         */
        private static final Simple FROM_PROPERTIES = new Simple(null, null);

        public static final class Json extends KeyOrValueSpec {
            public final ColumnDefinition<?>[] columnDefinitions;
            public final Map<String, String> fieldNameToColumnName;

            private Json(
                    final ColumnDefinition<?>[] columnDefinitions,
                    final Map<String, String> fieldNameToColumnName) {
                this.columnDefinitions = columnDefinitions;
                this.fieldNameToColumnName = fieldNameToColumnName;
            }

            @Override
            public DataFormat dataFormat() {
                return DataFormat.JSON;
            }
        }
    }

    /**
     * Type enumeration for the result {@link Table} returned by stream consumers.
     */
    public enum TableType {
        /**
         * <p>
         * Consume all partitions into a single interleaved stream table, which will present only newly-available rows
         * to downstream operations and visualizations.
         * <p>
         * See {@link Table#STREAM_TABLE_ATTRIBUTE} for a detailed explanation of stream table semantics, and
         * {@link io.deephaven.db.v2.StreamTableTools} for related tooling.
         */
        Stream(false, false),
        /**
         * Consume all partitions into a single interleaved in-memory append-only table.
         */
        Append(true, false),
        /**
         * <p>
         * As in {@link #Stream}, but each partition is mapped to a distinct stream table.
         * <p>
         * The resulting per-partition tables are aggregated into a single {@link TableMap} keyed by {@link Integer}
         * partition, which is then presented as a {@link Table} proxy via
         * {@link TransformableTableMap#asTable(boolean, boolean, boolean) asTable} with {@code strictKeys=true},
         * {@code allowCoalesce=true}, and {@code sanityCheckJoins=true}.
         * <p>
         * See {@link TransformableTableMap#asTableMap()} to explicitly work with the underlying {@link TableMap} and
         * {@link TransformableTableMap#asTable(boolean, boolean, boolean)} for alternative proxy options.
         */
        StreamMap(false, true),
        /**
         * <p>
         * As in {@link #Append}, but each partition is mapped to a distinct in-memory append-only table.
         * <p>
         * The resulting per-partition tables are aggregated into a single {@link TableMap} keyed by {@link Integer}
         * partition, which is then presented as a {@link Table} proxy via
         * {@link TransformableTableMap#asTable(boolean, boolean, boolean) asTable} with {@code strictKeys=true},
         * {@code allowCoalesce=true}, and {@code sanityCheckJoins=true}.
         * <p>
         * See {@link TransformableTableMap#asTableMap()} to explicitly work with the underlying {@link TableMap} and
         * {@link TransformableTableMap#asTable(boolean, boolean, boolean)} for alternative proxy options.
         */
        AppendMap(true, true);

        private final boolean isAppend;
        private final boolean isMap;

        TableType(final boolean isAppend, final boolean isMap) {
            this.isAppend = isAppend;
            this.isMap = isMap;
        }

    }

    /**
     * Map "Python-friendly" table type name to a {@link TableType}.
     *
     * @param typeName The friendly name
     * @return The mapped {@link TableType}
     */
    public static TableType friendlyNameToTableType(@NotNull final String typeName) {
        // @formatter:off
        switch (typeName) {
            case "stream"    : return TableType.Stream;
            case "append"    : return TableType.Append;
            case "stream_map": return TableType.StreamMap;
            case "append_map": return TableType.AppendMap;
            default             : return null;
        }
        // @formatter:on
    }

    /**
     * Spec to explicitly ask
     * {@link #consumeToTable(Properties, String, IntPredicate, IntToLongFunction, KeyOrValueSpec, KeyOrValueSpec, TableType)
     * consumeToTable} to ignore either key or value.
     */
    @SuppressWarnings("unused")
    public static KeyOrValueSpec ignoreSpec() {
        return KeyOrValueSpec.IGNORE;
    }

    /**
     * A JSON spec from a set of column definitions.
     *
     * @param columnDefinitions An array of column definitions for specifying the table to be created.
     * @param fieldNameToColumnName A mapping from JSON field names to column names provided in the definition. Fields
     *        not included will be ignored.
     * @return A JSON spec for the given inputs.
     */
    @SuppressWarnings("unused")
    public static KeyOrValueSpec jsonSpec(
            final ColumnDefinition<?>[] columnDefinitions,
            final Map<String, String> fieldNameToColumnName) {
        return new KeyOrValueSpec.Json(columnDefinitions, fieldNameToColumnName);
    }

    /**
     * A JSON spec from a set of column definitions.
     *
     * @param columnDefinitions An array of column definitions for specifying the table to be created. The column names
     *        should map one to JSON fields expected; is not necessary to include all fields from the expected JSON, any
     *        fields not included would be ignored.
     *
     * @return A JSON spec for the given inputs.
     */
    @SuppressWarnings("unused")
    public static KeyOrValueSpec jsonSpec(final ColumnDefinition<?>[] columnDefinitions) {
        return jsonSpec(columnDefinitions, null);
    }

    /**
     * Avro spec from an Avro schmea.
     *
     * @param schema An Avro schema.
     * @param fieldNameToColumnName A mapping specifying which Avro fields to include and what column name to use for
     *        them
     * @return A spec corresponding to the schema provided.
     */
    @SuppressWarnings("unused")
    public static KeyOrValueSpec avroSpec(final Schema schema, final Function<String, String> fieldNameToColumnName) {
        return new KeyOrValueSpec.Avro(schema, fieldNameToColumnName);
    }

    /**
     * Avro spec from an Avro schmea. All fields in the schema are mapped to columns of the same name.
     *
     * @param schema An Avro schema.
     * @return A spec corresponding to the schema provided.
     */
    @SuppressWarnings("unused")
    public static KeyOrValueSpec avroSpec(final Schema schema) {
        return new KeyOrValueSpec.Avro(schema, Function.identity());
    }

    /**
     * Avro spec from fetching an Avro schema from a Confluent compatible Schema Server. The Properties used to
     * initialize Kafka should contain the URL for the Schema Server to use under the "schema.registry.url" property.
     *
     * @param schemaName The registered name for the schema on Schema Server
     * @param schemaVersion The version to fetch
     * @param fieldNameToColumnName A mapping specifying which Avro fields to include and what column name to use for
     *        them
     * @return A spec corresponding to the schema provided.
     */
    @SuppressWarnings("unused")
    public static KeyOrValueSpec avroSpec(final String schemaName,
            final String schemaVersion,
            final Function<String, String> fieldNameToColumnName) {
        return new KeyOrValueSpec.Avro(schemaName, schemaVersion, fieldNameToColumnName);
    }

    /**
     * Avro spec from fetching an Avro schema from a Confluent compatible Schema Server. The Properties used to
     * initialize Kafka should contain the URL for the Schema Server to use under the "schema.registry.url" property.
     * The version fetched would be latest.
     *
     * @param schemaName The registered name for the schema on Schema Server
     * @param fieldNameToColumnName A mapping specifying which Avro fields to include and what column name to use for
     *        them
     * @return A spec corresponding to the schema provided.
     */
    @SuppressWarnings("unused")
    public static KeyOrValueSpec avroSpec(final String schemaName,
            final Function<String, String> fieldNameToColumnName) {
        return new KeyOrValueSpec.Avro(schemaName, AVRO_LATEST_VERSION, fieldNameToColumnName);
    }

    /**
     * Avro spec from fetching an Avro schema from a Confluent compatible Schema Server. The Properties used to
     * initialize Kafka should contain the URL for the Schema Server to use under the "schema.registry.url" property.
     * All fields in the schema are mapped to columns of the same name.
     *
     * @param schemaName The registered name for the schema on Schema Server
     * @param schemaVersion The version to fetch
     * @return A spec corresponding to the schema provided
     */
    @SuppressWarnings("unused")
    public static KeyOrValueSpec avroSpec(final String schemaName, final String schemaVersion) {
        return new KeyOrValueSpec.Avro(schemaName, schemaVersion, Function.identity());
    }

    /**
     * Avro spec from fetching an Avro schema from a Confluent compatible Schema Server The Properties used to
     * initialize Kafka should contain the URL for the Schema Server to use under the "schema.registry.url" property.
     * The version fetched is latest All fields in the schema are mapped to columns of the same name
     *
     * @param schemaName The registered name for the schema on Schema Server.
     * @return A spec corresponding to the schema provided.
     */
    @SuppressWarnings("unused")
    public static KeyOrValueSpec avroSpec(final String schemaName) {
        return new KeyOrValueSpec.Avro(schemaName, AVRO_LATEST_VERSION, Function.identity());
    }

    @SuppressWarnings("unused")
    public static KeyOrValueSpec simpleSpec(final String columnName, final Class<?> dataType) {
        return new KeyOrValueSpec.Simple(columnName, dataType);
    }

    /**
     * The types for key or value are either specified in the properties as "key.type" or "value.type", or deduced from
     * the serializer classes for key or value in the provided Properties object.
     */
    @SuppressWarnings("unused")
    public static KeyOrValueSpec simpleSpec(final String columnName) {
        return new KeyOrValueSpec.Simple(columnName, null);
    }

    /**
     * Consume from Kafka to a Deephaven table.
     *
     * @param kafkaConsumerProperties Properties to configure this table and also to be passed to create the
     *        KafkaConsumer
     * @param topic Kafka topic name
     * @param partitionFilter A predicate returning true for the partitions to consume
     * @param partitionToInitialOffset A function specifying the desired initial offset for each partition consumed
     * @param keySpec Conversion specification for Kafka record keys
     * @param valueSpec Conversion specification for Kafka record values
     * @param resultType {@link TableType} specifying the type of the expected result
     * @return The result table containing Kafka stream data formatted according to {@code resultType}
     */
    @SuppressWarnings("unused")
    public static Table consumeToTable(
            @NotNull final Properties kafkaConsumerProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final IntToLongFunction partitionToInitialOffset,
            @NotNull final KeyOrValueSpec keySpec,
            @NotNull final KeyOrValueSpec valueSpec,
            @NotNull final TableType resultType) {
        final boolean ignoreKey = keySpec.dataFormat() == DataFormat.IGNORE;
        final boolean ignoreValue = valueSpec.dataFormat() == DataFormat.IGNORE;
        if (ignoreKey && ignoreValue) {
            throw new IllegalArgumentException(
                    "can't ignore both key and value: keySpec and valueSpec can't both be ignore specs");
        }
        if (ignoreKey) {
            setDeserIfNotSet(kafkaConsumerProperties, KeyOrValue.KEY, DESERIALIZER_FOR_IGNORE);
        }
        if (ignoreValue) {
            setDeserIfNotSet(kafkaConsumerProperties, KeyOrValue.VALUE, DESERIALIZER_FOR_IGNORE);
        }

        final ColumnDefinition<?>[] commonColumns = new ColumnDefinition<?>[3];
        getCommonCols(commonColumns, 0, kafkaConsumerProperties);
        final List<ColumnDefinition<?>> columnDefinitions = new ArrayList<>();
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
                getIngestData(KeyOrValue.VALUE, kafkaConsumerProperties, columnDefinitions, nextColumnIndexMut,
                        valueSpec);

        final TableDefinition tableDefinition = new TableDefinition(columnDefinitions);

        final StreamTableMap streamTableMap = resultType.isMap ? new StreamTableMap(tableDefinition) : null;
        final LiveTableRegistrar liveTableRegistrar =
                streamTableMap == null ? LiveTableMonitor.DEFAULT : streamTableMap.refreshCombiner;

        final Supplier<Pair<StreamToTableAdapter, ConsumerRecordToStreamPublisherAdapter>> adapterFactory = () -> {
            final StreamPublisherImpl streamPublisher = new StreamPublisherImpl();
            final StreamToTableAdapter streamToTableAdapter =
                    new StreamToTableAdapter(tableDefinition, streamPublisher, liveTableRegistrar);
            streamPublisher.setChunkFactory(() -> streamToTableAdapter.makeChunksForDefinition(CHUNK_SIZE),
                    streamToTableAdapter::chunkTypeForIndex);

            final KeyOrValueProcessor keyProcessor =
                    getProcessor(keySpec, tableDefinition, streamToTableAdapter, keyIngestData);
            final KeyOrValueProcessor valueProcessor =
                    getProcessor(valueSpec, tableDefinition, streamToTableAdapter, valueIngestData);

            return new Pair<>(
                    streamToTableAdapter,
                    KafkaStreamPublisher.make(
                            streamPublisher,
                            commonColumnIndices[0],
                            commonColumnIndices[1],
                            commonColumnIndices[2],
                            keyProcessor,
                            valueProcessor,
                            keyIngestData == null ? -1 : keyIngestData.simpleColumnIndex,
                            valueIngestData == null ? -1 : valueIngestData.simpleColumnIndex,
                            keyIngestData == null ? Function.identity() : keyIngestData.toObjectChunkMapper,
                            valueIngestData == null ? Function.identity() : valueIngestData.toObjectChunkMapper));
        };

        final UnaryOperator<Table> tableConversion =
                resultType.isAppend ? StreamTableTools::streamToAppendOnlyTable : UnaryOperator.identity();
        final Table result;
        final IntFunction<KafkaStreamConsumer> partitionToConsumer;
        if (resultType.isMap) {
            result = streamTableMap.asTable(true, true, true);
            partitionToConsumer = (final int partition) -> {
                final Pair<StreamToTableAdapter, ConsumerRecordToStreamPublisherAdapter> partitionAdapterPair =
                        adapterFactory.get();
                final Table partitionTable = tableConversion.apply(partitionAdapterPair.getFirst().table());
                streamTableMap.enqueueUpdate(() -> Assert.eqNull(streamTableMap.put(partition, partitionTable),
                        "streamTableMap.put(partition, partitionTable)"));
                return new SimpleKafkaStreamConsumer(partitionAdapterPair.getSecond(), partitionAdapterPair.getFirst());
            };
        } else {
            final Pair<StreamToTableAdapter, ConsumerRecordToStreamPublisherAdapter> singleAdapterPair =
                    adapterFactory.get();
            result = tableConversion.apply(singleAdapterPair.getFirst().table());
            partitionToConsumer = (final int partition) -> new SimpleKafkaStreamConsumer(singleAdapterPair.getSecond(),
                    singleAdapterPair.getFirst());
        }

        final KafkaIngester ingester = new KafkaIngester(
                log,
                kafkaConsumerProperties,
                topic,
                partitionFilter,
                partitionToConsumer,
                partitionToInitialOffset);
        ingester.start();

        return result;
    }

    private static class StreamTableMap extends LocalTableMap implements LiveTable {

        private final LiveTableRefreshCombiner refreshCombiner = new LiveTableRefreshCombiner();
        private final Queue<Runnable> deferredUpdates = new ConcurrentLinkedQueue<>();

        private StreamTableMap(@NotNull final TableDefinition constituentDefinition) {
            super(null, constituentDefinition);
            refreshCombiner.addTable(this); // Results in managing the refreshCombiner
            LiveTableMonitor.DEFAULT.addTable(refreshCombiner);
        }

        @Override
        public void refresh() {
            Runnable deferredUpdate;
            while ((deferredUpdate = deferredUpdates.poll()) != null) {
                deferredUpdate.run();
            }
        }

        private void enqueueUpdate(@NotNull final Runnable deferredUpdate) {
            deferredUpdates.add(deferredUpdate);
        }
    }

    private static KeyOrValueProcessor getProcessor(
            final KeyOrValueSpec spec,
            final TableDefinition tableDef,
            final StreamToTableAdapter streamToTableAdapter,
            final KeyOrValueIngestData data) {
        switch (spec.dataFormat()) {
            case IGNORE:
            case SIMPLE:
                return null;
            case AVRO:
                return GenericRecordChunkAdapter.make(
                        tableDef, streamToTableAdapter::chunkTypeForIndex, data.fieldNameToColumnName,
                        (Schema) data.extra, true);
            case JSON:
                return JsonNodeChunkAdapter.make(
                        tableDef, streamToTableAdapter::chunkTypeForIndex, data.fieldNameToColumnName, true);
            default:
                throw new IllegalStateException("Unknown KeyOrvalueSpec value" + spec.dataFormat());
        }
    }

    private static class KeyOrValueIngestData {
        public Map<String, String> fieldNameToColumnName;
        public int simpleColumnIndex = -1;
        public Function<Object, Object> toObjectChunkMapper = Function.identity();
        public Object extra;
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
                : ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
        setIfNotSet(prop, propKey, value);
    }

    private static KeyOrValueIngestData getIngestData(
            final KeyOrValue keyOrValue,
            final Properties kafkaConsumerProperties,
            final List<ColumnDefinition<?>> columnDefinitions,
            final MutableInt nextColumnIndexMut,
            final KeyOrValueSpec keyOrValueSpec) {
        if (keyOrValueSpec.dataFormat() == DataFormat.IGNORE) {
            return null;
        }
        final KeyOrValueIngestData data = new KeyOrValueIngestData();
        switch (keyOrValueSpec.dataFormat()) {
            case AVRO:
                setDeserIfNotSet(kafkaConsumerProperties, keyOrValue, KafkaAvroDeserializer.class.getName());
                final KeyOrValueSpec.Avro avroSpec = (KeyOrValueSpec.Avro) keyOrValueSpec;
                data.fieldNameToColumnName = new HashMap<>();
                final Schema schema;
                if (avroSpec.schema != null) {
                    schema = avroSpec.schema;
                } else {
                    if (!kafkaConsumerProperties.containsKey(SCHEMA_SERVER_PROPERTY)) {
                        throw new IllegalArgumentException(
                                "Avro schema name specified and schema server url propeorty " +
                                        SCHEMA_SERVER_PROPERTY + " not found.");
                    }
                    final String schemaServiceUrl = kafkaConsumerProperties.getProperty(SCHEMA_SERVER_PROPERTY);
                    schema = getAvroSchema(schemaServiceUrl, avroSpec.schemaName, avroSpec.schemaVersion);
                }
                avroSchemaToColumnDefinitions(
                        columnDefinitions, data.fieldNameToColumnName, schema, avroSpec.fieldNameToColumnName);
                data.extra = schema;
                break;
            case JSON:
                setDeserIfNotSet(kafkaConsumerProperties, keyOrValue, STRING_DESERIALIZER);
                data.toObjectChunkMapper = jsonToObjectChunkMapper;
                final KeyOrValueSpec.Json jsonSpec = (KeyOrValueSpec.Json) keyOrValueSpec;
                columnDefinitions.addAll(Arrays.asList(jsonSpec.columnDefinitions));
                // Populate out field to column name mapping from two potential sources.
                data.fieldNameToColumnName = new HashMap<>(jsonSpec.columnDefinitions.length);
                final Set<String> coveredColumns = new HashSet<>(jsonSpec.columnDefinitions.length);
                if (jsonSpec.fieldNameToColumnName != null) {
                    for (final Map.Entry<String, String> entry : jsonSpec.fieldNameToColumnName.entrySet()) {
                        final String colName = entry.getValue();
                        data.fieldNameToColumnName.put(entry.getKey(), colName);
                        coveredColumns.add(colName);
                    }
                }
                for (final ColumnDefinition<?> colDef : jsonSpec.columnDefinitions) {
                    final String colName = colDef.getName();
                    if (!coveredColumns.contains(colName)) {
                        data.fieldNameToColumnName.put(colName, colName);
                    }
                }
                break;
            case SIMPLE:
                data.simpleColumnIndex = nextColumnIndexMut.getAndAdd(1);
                final KeyOrValueSpec.Simple simpleSpec = (KeyOrValueSpec.Simple) keyOrValueSpec;
                final ColumnDefinition<?> colDef;
                if (simpleSpec.dataType == null) {
                    colDef = getKeyOrValueCol(keyOrValue, kafkaConsumerProperties, simpleSpec.columnName, false);
                } else {
                    colDef = ColumnDefinition.fromGenericType(simpleSpec.columnName, simpleSpec.dataType);
                }
                final String propKey = (keyOrValue == KeyOrValue.KEY)
                        ? ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
                        : ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
                if (!kafkaConsumerProperties.containsKey(propKey)) {
                    final Class<?> dataType = colDef.getDataType();
                    if (dataType == short.class) {
                        kafkaConsumerProperties.setProperty(propKey, SHORT_DESERIALIZER);
                    } else if (dataType == int.class) {
                        kafkaConsumerProperties.setProperty(propKey, INT_DESERIALIZER);
                    } else if (dataType == long.class) {
                        kafkaConsumerProperties.setProperty(propKey, LONG_DESERIALIZER);
                    } else if (dataType == float.class) {
                        kafkaConsumerProperties.setProperty(propKey, FLOAT_DESERIALIZER);
                    } else if (dataType == double.class) {
                        kafkaConsumerProperties.setProperty(propKey, DOUBLE_DESERIALIZER);
                    } else if (dataType == String.class) {
                        kafkaConsumerProperties.setProperty(propKey, STRING_DESERIALIZER);
                    } else {
                        throw new UncheckedDeephavenException(
                                "Deserializer for " + keyOrValue + " not set in kafka consumer properties " +
                                        "and can't automatically set it for type " + dataType);
                    }
                }
                setDeserIfNotSet(kafkaConsumerProperties, keyOrValue, STRING_DESERIALIZER);
                columnDefinitions.add(colDef);
                break;
            default:
                throw new IllegalStateException("Unhandled spec type:" + keyOrValueSpec.dataFormat());
        }
        return data;
    }

    private static final Function<Object, Object> jsonToObjectChunkMapper = (final Object in) -> {
        final String json;
        try {
            json = (String) in;
        } catch (ClassCastException ex) {
            throw new UncheckedDeephavenException("Could not convert input to json string", ex);
        }
        return JsonNodeUtil.makeJsonNode(json);
    };

    private static void getCommonCol(
            @NotNull final ColumnDefinition<?>[] columnsToSet,
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
            @NotNull final Properties consumerProperties) {
        int c = outOffset;

        getCommonCol(
                columnsToSet,
                c,
                consumerProperties,
                KAFKA_PARTITION_COLUMN_NAME_PROPERTY,
                KAFKA_PARTITION_COLUMN_NAME_DEFAULT,
                ColumnDefinition::ofInt);
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
            final boolean allowEmpty) {
        final String typeProperty;
        final String deserializerProperty;
        final String nameProperty;
        final String nameDefault;
        switch (keyOrValue) {
            case KEY:
                typeProperty = KEY_COLUMN_TYPE_PROPERTY;
                deserializerProperty = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
                nameProperty = KEY_COLUMN_NAME_PROPERTY;
                nameDefault = KEY_COLUMN_NAME_DEFAULT;
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
                case "short":
                    properties.setProperty(deserializerProperty, SHORT_DESERIALIZER);
                    return ColumnDefinition.ofShort(columnName);
                case "int":
                    properties.setProperty(deserializerProperty, INT_DESERIALIZER);
                    return ColumnDefinition.ofInt(columnName);
                case "long":
                    properties.setProperty(deserializerProperty, LONG_DESERIALIZER);
                    return ColumnDefinition.ofLong(columnName);
                case "float":
                    properties.setProperty(deserializerProperty, FLOAT_DESERIALIZER);
                    return ColumnDefinition.ofDouble(columnName);
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
    private static ColumnDefinition<? extends Serializable> columnDefinitionFromDeserializer(
            @NotNull Properties properties, @NotNull String deserializerProperty, String columnName) {
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

    @SuppressWarnings("unused")
    public static final long SEEK_TO_BEGINNING = KafkaIngester.SEEK_TO_BEGINNING;
    @SuppressWarnings("unused")
    public static final long DONT_SEEK = KafkaIngester.DONT_SEEK;
    @SuppressWarnings("unused")
    public static final long SEEK_TO_END = KafkaIngester.SEEK_TO_END;
    @SuppressWarnings("unused")
    public static final IntPredicate ALL_PARTITIONS = KafkaIngester.ALL_PARTITIONS;
    @SuppressWarnings("unused")
    public static final IntToLongFunction ALL_PARTITIONS_SEEK_TO_BEGINNING =
            KafkaIngester.ALL_PARTITIONS_SEEK_TO_BEGINNING;
    @SuppressWarnings("unused")
    public static final IntToLongFunction ALL_PARTITIONS_DONT_SEEK = KafkaIngester.ALL_PARTITIONS_DONT_SEEK;
    @SuppressWarnings("unused")
    public static final IntToLongFunction ALL_PARTITIONS_SEEK_TO_END = KafkaIngester.ALL_PARTITIONS_SEEK_TO_END;
    @SuppressWarnings("unused")
    public static final Function<String, String> DIRECT_MAPPING = Function.identity();
    @SuppressWarnings("unused")
    public static final KeyOrValueSpec FROM_PROPERTIES = KeyOrValueSpec.FROM_PROPERTIES;
    @SuppressWarnings("unused")
    public static final KeyOrValueSpec IGNORE = KeyOrValueSpec.IGNORE;

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

    private static class SimpleKafkaStreamConsumer implements KafkaStreamConsumer {
        private final ConsumerRecordToStreamPublisherAdapter adapter;
        private final StreamToTableAdapter streamToTableAdapter;

        public SimpleKafkaStreamConsumer(ConsumerRecordToStreamPublisherAdapter adapter,
                StreamToTableAdapter streamToTableAdapter) {
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
