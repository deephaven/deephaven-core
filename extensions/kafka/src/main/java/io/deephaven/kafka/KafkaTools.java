//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import gnu.trove.map.hash.TIntLongHashMap;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.annotations.SingletonStyle;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessManager;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.ConstituentDependency;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ring.RingTableTools;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.kafka.AvroImpl.AvroConsume;
import io.deephaven.kafka.AvroImpl.AvroProduce;
import io.deephaven.kafka.IgnoreImpl.IgnoreConsume;
import io.deephaven.kafka.IgnoreImpl.IgnoreProduce;
import io.deephaven.kafka.JsonImpl.JsonConsume;
import io.deephaven.kafka.JsonImpl.JsonProduce;
import io.deephaven.kafka.KafkaTools.Produce.KeyOrValueSpec;
import io.deephaven.kafka.KafkaTools.StreamConsumerRegistrarProvider.PerPartition;
import io.deephaven.kafka.KafkaTools.StreamConsumerRegistrarProvider.Single;
import io.deephaven.kafka.KafkaTools.TableType.Append;
import io.deephaven.kafka.KafkaTools.TableType.Blink;
import io.deephaven.kafka.KafkaTools.TableType.Ring;
import io.deephaven.kafka.KafkaTools.TableType.Visitor;
import io.deephaven.kafka.ProtobufImpl.ProtobufConsumeImpl;
import io.deephaven.kafka.RawImpl.RawConsume;
import io.deephaven.kafka.RawImpl.RawProduce;
import io.deephaven.kafka.SimpleImpl.SimpleConsume;
import io.deephaven.kafka.SimpleImpl.SimpleProduce;
import io.deephaven.kafka.ingest.ConsumerRecordToStreamPublisherAdapter;
import io.deephaven.kafka.ingest.KafkaIngester;
import io.deephaven.kafka.ingest.KafkaRecordConsumer;
import io.deephaven.kafka.ingest.KafkaStreamPublisher;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.protobuf.ProtobufConsumeOptions;
import io.deephaven.kafka.publish.KafkaPublisherException;
import io.deephaven.kafka.publish.KeyOrValueSerializer;
import io.deephaven.kafka.publish.PublishToKafka;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.protobuf.ProtobufDescriptorParserOptions;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.annotations.ScriptApi;
import org.apache.avro.Schema;
import io.deephaven.util.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.IntToLongFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.deephaven.kafka.ingest.KafkaStreamPublisher.NULL_COLUMN_INDEX;

public class KafkaTools {

    public static final String KAFKA_PARTITION_COLUMN_NAME_PROPERTY = "deephaven.partition.column.name";
    public static final String KAFKA_PARTITION_COLUMN_NAME_DEFAULT = "KafkaPartition";
    public static final String OFFSET_COLUMN_NAME_PROPERTY = "deephaven.offset.column.name";
    public static final String OFFSET_COLUMN_NAME_DEFAULT = "KafkaOffset";
    public static final String TIMESTAMP_COLUMN_NAME_PROPERTY = "deephaven.timestamp.column.name";
    public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "KafkaTimestamp";
    public static final String RECEIVE_TIME_COLUMN_NAME_PROPERTY = "deephaven.receivetime.column.name";
    public static final String RECEIVE_TIME_COLUMN_NAME_DEFAULT = null;
    public static final String KEY_BYTES_COLUMN_NAME_PROPERTY = "deephaven.keybytes.column.name";
    public static final String KEY_BYTES_COLUMN_NAME_DEFAULT = null;
    public static final String VALUE_BYTES_COLUMN_NAME_PROPERTY = "deephaven.valuebytes.column.name";
    public static final String VALUE_BYTES_COLUMN_NAME_DEFAULT = null;
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
    public static final String AVRO_DESERIALIZER = KafkaAvroDeserializer.class.getName();
    public static final String DESERIALIZER_FOR_IGNORE = BYTE_BUFFER_DESERIALIZER;
    public static final String SHORT_SERIALIZER = ShortSerializer.class.getName();
    public static final String INT_SERIALIZER = IntegerSerializer.class.getName();
    public static final String LONG_SERIALIZER = LongSerializer.class.getName();
    public static final String FLOAT_SERIALIZER = FloatSerializer.class.getName();
    public static final String DOUBLE_SERIALIZER = DoubleSerializer.class.getName();
    public static final String BYTE_ARRAY_SERIALIZER = ByteArraySerializer.class.getName();
    public static final String STRING_SERIALIZER = StringSerializer.class.getName();
    public static final String BYTE_BUFFER_SERIALIZER = ByteBufferSerializer.class.getName();
    public static final String AVRO_SERIALIZER = KafkaAvroSerializer.class.getName();
    public static final String SERIALIZER_FOR_IGNORE = BYTE_BUFFER_SERIALIZER;
    public static final String NESTED_FIELD_NAME_SEPARATOR = ".";
    public static final String NESTED_FIELD_COLUMN_NAME_SEPARATOR = "__";
    public static final String AVRO_LATEST_VERSION = "latest";

    private static final Logger log = LoggerFactory.getLogger(KafkaTools.class);

    /**
     * Create an Avro schema object for a String containing a JSON encoded Avro schema definition.
     *
     * @param avroSchemaAsJsonString The JSON Avro schema definition
     * @return an Avro schema object
     */
    public static Schema getAvroSchema(final String avroSchemaAsJsonString) {
        return new Schema.Parser().parse(avroSchemaAsJsonString);
    }

    public static Schema columnDefinitionsToAvroSchema(
            final Table t,
            final String schemaName,
            final String namespace,
            final Properties colProps,
            final Predicate<String> includeOnly,
            final Predicate<String> exclude,
            final MutableObject<Properties> colPropsOut) {
        return AvroImpl.columnDefinitionsToAvroSchema(t, schemaName, namespace, colProps, includeOnly, exclude,
                colPropsOut);
    }

    public static void avroSchemaToColumnDefinitions(
            final List<ColumnDefinition<?>> columnsOut,
            final Map<String, String> fieldPathToColumnNameOut,
            final Schema schema,
            final Function<String, String> requestedFieldPathToColumnName) {
        avroSchemaToColumnDefinitions(columnsOut, fieldPathToColumnNameOut, schema, requestedFieldPathToColumnName,
                false);
    }

    public static void avroSchemaToColumnDefinitions(
            final List<ColumnDefinition<?>> columnsOut,
            final Map<String, String> fieldPathToColumnNameOut,
            final Schema schema,
            final Function<String, String> requestedFieldPathToColumnName,
            final boolean useUTF8Strings) {
        AvroImpl.avroSchemaToColumnDefinitions(columnsOut, fieldPathToColumnNameOut, schema,
                requestedFieldPathToColumnName, useUTF8Strings);
    }

    /**
     * Convert an Avro schema to a list of column definitions.
     *
     * @param columnsOut Column definitions for output; should be empty on entry.
     * @param schema Avro schema
     * @param requestedFieldPathToColumnName An optional mapping to specify selection and naming of columns from Avro
     *        fields, or null for map all fields using field path for column name.
     */
    public static void avroSchemaToColumnDefinitions(
            final List<ColumnDefinition<?>> columnsOut,
            final Schema schema,
            final Function<String, String> requestedFieldPathToColumnName) {
        avroSchemaToColumnDefinitions(columnsOut, null, schema, requestedFieldPathToColumnName);
    }

    /**
     * Convert an Avro schema to a list of column definitions, mapping every avro field to a column of the same name.
     *
     * @param columnsOut Column definitions for output; should be empty on entry.
     * @param schema Avro schema
     */
    public static void avroSchemaToColumnDefinitions(
            final List<ColumnDefinition<?>> columnsOut,
            final Schema schema) {
        avroSchemaToColumnDefinitions(columnsOut, schema, DIRECT_MAPPING);
    }

    /**
     * Enum to specify operations that may apply to either of Kafka KEY or VALUE fields.
     */
    public enum KeyOrValue {
        KEY, VALUE
    }

    private interface SchemaProviderProvider {
        Optional<SchemaProvider> getSchemaProvider();
    }

    /**
     * Determines the initial offset to seek to for a given KafkaConsumer and TopicPartition.
     */
    @FunctionalInterface
    public interface InitialOffsetLookup {

        /**
         * Creates a implementation based solely on the {@link TopicPartition#partition() topic partition}.
         */
        static InitialOffsetLookup adapt(IntToLongFunction intToLongFunction) {
            return new IntToLongLookupAdapter(intToLongFunction);
        }

        /**
         * Returns the initial offset that the {@code consumer} should start from for a given {@code topicPartition}.
         *
         * <ul>
         * <li>{@value SEEK_TO_BEGINNING}, seek to the beginning</li>
         * <li>{@value DONT_SEEK}, don't seek</li>
         * <li>{@value SEEK_TO_END}, seek to the end</li>
         * </ul>
         * 
         * @param consumer the consumer
         * @param topicPartition the topic partition
         * @return the initial
         * @see #SEEK_TO_BEGINNING
         * @see #DONT_SEEK
         * @see #SEEK_TO_END
         */
        long getInitialOffset(KafkaConsumer<?, ?> consumer, TopicPartition topicPartition);
    }

    /**
     * A callback which is invoked from the consumer loop, enabling clients to inject logic to be invoked by the Kafka
     * consumer thread.
     */
    public interface ConsumerLoopCallback {
        /**
         * Called before the consumer is polled for records.
         *
         * @param consumer the KafkaConsumer that will be polled for records
         */
        void beforePoll(KafkaConsumer<?, ?> consumer);

        /**
         * Called after the consumer is polled for records and they have been published to the downstream
         * KafkaRecordConsumer.
         *
         * @param consumer the KafkaConsumer that has been polled for records
         * @param more true if more records should be read, false if the consumer should be shut down due to error
         */
        void afterPoll(KafkaConsumer<?, ?> consumer, boolean more);
    }

    public static class Consume {

        /**
         * Class to specify conversion of Kafka KEY or VALUE fields to table columns.
         */
        public static abstract class KeyOrValueSpec implements SchemaProviderProvider {

            protected abstract Deserializer<?> getDeserializer(
                    KeyOrValue keyOrValue,
                    SchemaRegistryClient schemaRegistryClient,
                    Map<String, ?> configs);

            protected abstract KeyOrValueIngestData getIngestData(
                    KeyOrValue keyOrValue,
                    SchemaRegistryClient schemaRegistryClient,
                    Map<String, ?> configs,
                    MutableInt nextColumnIndexMut,
                    List<ColumnDefinition<?>> columnDefinitionsOut);

            protected abstract KeyOrValueProcessor getProcessor(
                    TableDefinition tableDef,
                    KeyOrValueIngestData data);
        }

        private static final KeyOrValueSpec FROM_PROPERTIES = new SimpleConsume(null, null);

        public static final KeyOrValueSpec IGNORE = new IgnoreConsume();

        /**
         * Spec to explicitly ask one of the "consume" methods to ignore either key or value.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec ignoreSpec() {
            return IGNORE;
        }

        private static boolean isIgnore(KeyOrValueSpec keyOrValueSpec) {
            return keyOrValueSpec == IGNORE;
        }

        /**
         * A JSON spec from a set of column definitions, with a specific mapping of JSON nodes to columns and a custom
         * {@link ObjectMapper}. JSON nodes can be specified as a string field name, or as a JSON Pointer string (see
         * RFC 6901, ISSN: 2070-1721).
         *
         * @param columnDefinitions An array of column definitions for specifying the table to be created
         * @param fieldToColumnName A mapping from JSON field names or JSON Pointer strings to column names provided in
         *        the definition. For each field key, if it starts with '/' it is assumed to be a JSON Pointer (e.g.,
         *        {@code "/parent/nested"} represents a pointer to the nested field {@code "nested"} inside the toplevel
         *        field {@code "parent"}). Fields not included will be ignored
         * @param objectMapper A custom {@link ObjectMapper} to use for deserializing JSON. May be null.
         * @return A JSON spec for the given inputs
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec jsonSpec(
                @NotNull final ColumnDefinition<?>[] columnDefinitions,
                @Nullable final Map<String, String> fieldToColumnName,
                @Nullable final ObjectMapper objectMapper) {
            return new JsonConsume(columnDefinitions, fieldToColumnName, objectMapper);
        }

        /**
         * A JSON spec from a set of column definitions, with a specific mapping of JSON nodes to columns. JSON nodes
         * can be specified as a string field name, or as a JSON Pointer string (see RFC 6901, ISSN: 2070-1721).
         *
         * @param columnDefinitions An array of column definitions for specifying the table to be created
         * @param fieldToColumnName A mapping from JSON field names or JSON Pointer strings to column names provided in
         *        the definition. For each field key, if it starts with '/' it is assumed to be a JSON Pointer (e.g.,
         *        {@code "/parent/nested"} represents a pointer to the nested field {@code "nested"} inside the toplevel
         *        field {@code "parent"}). Fields not included will be ignored
         * @return A JSON spec for the given inputs
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec jsonSpec(
                @NotNull final ColumnDefinition<?>[] columnDefinitions,
                @Nullable final Map<String, String> fieldToColumnName) {
            return new JsonConsume(columnDefinitions, fieldToColumnName, null);
        }

        /**
         * A JSON spec from a set of column definitions using a custom {@link ObjectMapper}.
         *
         * @param columnDefinitions An array of column definitions for specifying the table to be created. The column
         *        names should map one to JSON fields expected; is not necessary to include all fields from the expected
         *        JSON, any fields not included would be ignored.
         * @param objectMapper A custom {@link ObjectMapper} to use for deserializing JSON. May be null.
         * @return A JSON spec for the given inputs.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec jsonSpec(
                @NotNull final ColumnDefinition<?>[] columnDefinitions,
                @Nullable final ObjectMapper objectMapper) {
            return jsonSpec(columnDefinitions, null, objectMapper);
        }

        /**
         * A JSON spec from a set of column definitions.
         *
         * @param columnDefinitions An array of column definitions for specifying the table to be created. The column
         *        names should map one to JSON fields expected; is not necessary to include all fields from the expected
         *        JSON, any fields not included would be ignored.
         * @return A JSON spec for the given inputs.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec jsonSpec(@NotNull final ColumnDefinition<?>[] columnDefinitions) {
            return jsonSpec(columnDefinitions, null, null);
        }

        /**
         * Avro spec from an Avro schema.
         *
         * @param schema An Avro schema.
         * @param fieldNameToColumnName A mapping specifying which Avro fields to include and what column name to use
         *        for them; fields mapped to null are excluded.
         * @return A spec corresponding to the schema provided.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec avroSpec(final Schema schema,
                final Function<String, String> fieldNameToColumnName) {
            return new AvroConsume(schema, fieldNameToColumnName);
        }

        /**
         * Avro spec from an Avro schema. All fields in the schema are mapped to columns of the same name.
         *
         * @param schema An Avro schema.
         * @return A spec corresponding to the schema provided.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec avroSpec(final Schema schema) {
            return new AvroConsume(schema, DIRECT_MAPPING);
        }

        /**
         * Avro spec from fetching an Avro schema from a Confluent compatible Schema Server. The Properties used to
         * initialize Kafka should contain the URL for the Schema Server to use under the "schema.registry.url"
         * property.
         *
         * @param schemaName The registered name for the schema on Schema Server
         * @param schemaVersion The version to fetch
         * @param fieldNameToColumnName A mapping specifying which Avro fields to include and what column name to use
         *        for them; fields mapped to null are excluded.
         * @return A spec corresponding to the schema provided.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec avroSpec(final String schemaName,
                final String schemaVersion,
                final Function<String, String> fieldNameToColumnName) {
            return new AvroConsume(schemaName, schemaVersion, fieldNameToColumnName);
        }

        /**
         * Avro spec from fetching an Avro schema from a Confluent compatible Schema Server. The Properties used to
         * initialize Kafka should contain the URL for the Schema Server to use under the "schema.registry.url"
         * property.
         *
         * @param schemaName The registered name for the schema on Schema Server
         * @param schemaVersion The version to fetch
         * @param fieldNameToColumnName A mapping specifying which Avro fields to include and what column name to use
         *        for them; fields mapped to null are excluded.
         * @param useUTF8Strings If true, String fields will be not be converted to Java Strings.
         * @return A spec corresponding to the schema provided.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec avroSpec(final String schemaName,
                final String schemaVersion,
                final Function<String, String> fieldNameToColumnName,
                final boolean useUTF8Strings) {
            return new AvroConsume(schemaName, schemaVersion, fieldNameToColumnName, useUTF8Strings);
        }

        /**
         * Avro spec from fetching an Avro schema from a Confluent compatible Schema Server. The Properties used to
         * initialize Kafka should contain the URL for the Schema Server to use under the "schema.registry.url"
         * property. The version fetched would be latest.
         *
         * @param schemaName The registered name for the schema on Schema Server
         * @param fieldNameToColumnName A mapping specifying which Avro fields to include and what column name to use
         *        for them; fields mapped to null are excluded.
         * @return A spec corresponding to the schema provided.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec avroSpec(final String schemaName,
                final Function<String, String> fieldNameToColumnName) {
            return new AvroConsume(schemaName, AVRO_LATEST_VERSION, fieldNameToColumnName);
        }

        /**
         * Avro spec from fetching an Avro schema from a Confluent compatible Schema Server. The Properties used to
         * initialize Kafka should contain the URL for the Schema Server to use under the "schema.registry.url"
         * property. All fields in the schema are mapped to columns of the same name.
         *
         * @param schemaName The registered name for the schema on Schema Server
         * @param schemaVersion The version to fetch
         * @return A spec corresponding to the schema provided
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec avroSpec(final String schemaName, final String schemaVersion) {
            return new AvroConsume(schemaName, schemaVersion, DIRECT_MAPPING);
        }

        /**
         * Avro spec from fetching an Avro schema from a Confluent compatible Schema Server The Properties used to
         * initialize Kafka should contain the URL for the Schema Server to use under the "schema.registry.url"
         * property. The version fetched is latest All fields in the schema are mapped to columns of the same name
         *
         * @param schemaName The registered name for the schema on Schema Server.
         * @return A spec corresponding to the schema provided.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec avroSpec(final String schemaName) {
            return new AvroConsume(schemaName, AVRO_LATEST_VERSION, DIRECT_MAPPING);
        }

        /**
         * The kafka protobuf specs. This will fetch the {@link com.google.protobuf.Descriptors.Descriptor protobuf
         * descriptor} based on the {@link ProtobufConsumeOptions#descriptorProvider()} and create the
         * {@link com.google.protobuf.Message message} parsing functions according to
         * {@link io.deephaven.protobuf.ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions)}.
         * These functions will be adapted to handle schema changes.
         *
         * @param options the options
         * @return the key or value spec
         * @see io.deephaven.protobuf.ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions)
         *      parsing
         */
        public static KeyOrValueSpec protobufSpec(ProtobufConsumeOptions options) {
            return new ProtobufConsumeImpl(options);
        }

        /**
         * If {@code columnName} is set, that column name will be used. Otherwise, the names for the key or value
         * columns can be provided in the properties as {@value KEY_COLUMN_NAME_PROPERTY} or
         * {@value VALUE_COLUMN_NAME_PROPERTY}, and otherwise default to {@value KEY_COLUMN_NAME_DEFAULT} or
         * {@value VALUE_COLUMN_NAME_DEFAULT}. If {@code dataType} is set, that type will be used for the column type.
         * Otherwise, the types for key or value are either specified in the properties as
         * {@value KEY_COLUMN_TYPE_PROPERTY} or {@value VALUE_COLUMN_TYPE_PROPERTY} or deduced from the serializer
         * classes for {@value ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} or
         * {@value ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} in the provided Properties object.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec simpleSpec(final String columnName, final Class<?> dataType) {
            return new SimpleConsume(columnName, dataType);
        }

        /**
         * If {@code columnName} is set, that column name will be used. Otherwise, the names for the key or value
         * columns can be provided in the properties as {@value KEY_COLUMN_NAME_PROPERTY} or
         * {@value VALUE_COLUMN_NAME_PROPERTY}, and otherwise default to {@value KEY_COLUMN_NAME_DEFAULT} or
         * {@value VALUE_COLUMN_NAME_DEFAULT}. The types for key or value are either specified in the properties as
         * {@value KEY_COLUMN_TYPE_PROPERTY} or {@value VALUE_COLUMN_TYPE_PROPERTY} or deduced from the serializer
         * classes for {@value ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} or
         * {@value ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} in the provided Properties object.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec simpleSpec(final String columnName) {
            return new SimpleConsume(columnName, null);
        }

        @SuppressWarnings("unused")
        public static KeyOrValueSpec rawSpec(ColumnHeader<?> header, Class<? extends Deserializer<?>> deserializer) {
            return new RawConsume(ColumnDefinition.from(header), deserializer);
        }

        /**
         * Creates a kafka key or value spec implementation from an {@link ObjectProcessor}.
         *
         * <p>
         * The respective column definitions are derived from the combination of {@code columnNames} and
         * {@link ObjectProcessor#outputTypes()}.
         *
         * @param deserializer the deserializer
         * @param processor the object processor
         * @param columnNames the column names
         * @return the Kafka key or value spec
         * @param <T> the object type
         */
        public static <T> KeyOrValueSpec objectProcessorSpec(
                Deserializer<? extends T> deserializer,
                ObjectProcessor<? super T> processor,
                List<String> columnNames) {
            return new KeyOrValueSpecObjectProcessorImpl<>(deserializer, processor, columnNames);
        }

        /**
         * Creates a kafka key or value spec implementation from a byte-array {@link ObjectProcessor}.
         *
         * <p>
         * Equivalent to {@code objectProcessorSpec(new ByteArrayDeserializer(), processor, columnNames)}.
         *
         * @param processor the byte-array object processor
         * @param columnNames the column names
         * @return the Kafka key or value spec
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec objectProcessorSpec(ObjectProcessor<? super byte[]> processor,
                List<String> columnNames) {
            return objectProcessorSpec(new ByteArrayDeserializer(), processor, columnNames);
        }
    }

    public static class Produce {
        /**
         * Class to specify conversion of table columns to Kafka KEY or VALUE fields.
         */
        public static abstract class KeyOrValueSpec implements SchemaProviderProvider {

            abstract Serializer<?> getSerializer(SchemaRegistryClient schemaRegistryClient, TableDefinition definition);

            abstract String[] getColumnNames(@NotNull Table t, SchemaRegistryClient schemaRegistryClient);

            abstract KeyOrValueSerializer<?> getKeyOrValueSerializer(@NotNull Table t, @NotNull String[] columnNames);
        }

        public static final KeyOrValueSpec IGNORE = new IgnoreProduce();

        /**
         * Spec to explicitly ask one of the "consume" methods to ignore either key or value.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec ignoreSpec() {
            return IGNORE;
        }

        static boolean isIgnore(KeyOrValueSpec keyOrValueSpec) {
            return keyOrValueSpec == IGNORE;
        }

        /**
         * A simple spec for sending one column as either key or value in a Kafka message.
         *
         * @param columnName The name of the column to include.
         * @return A simple spec for the given input.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec simpleSpec(final String columnName) {
            return new SimpleProduce(columnName);
        }

        public static KeyOrValueSpec rawSpec(final String columnName, final Class<? extends Serializer<?>> serializer) {
            return new RawProduce(columnName, serializer);
        }

        /**
         * A JSON spec from a set of column names
         *
         * @param includeColumns An array with an entry for each column intended to be included in the JSON output. If
         *        null, include all columns except those specified in {@code excludeColumns}. If {@code includeColumns}
         *        is not null, {@code excludeColumns} should be null.
         * @param excludeColumns A set specifying column names to omit; can only be used when {@code columnNames} is
         *        null. In this case all table columns except for the ones in {@code excludeColumns} will be included.
         * @param columnToFieldMapping A map from column name to JSON field name to use for that column. Any column
         *        names implied by earlier arguments not included as a key in the map will be mapped to JSON fields of
         *        the same name. If null, map all columns to fields of the same name.
         * @param nestedObjectDelimiter if nested JSON fields are desired, the field separator that is used for the
         *        fieldNames parameter, or null for no nesting. For instance, if a particular column should be mapped to
         *        JSON field {@code X} nested inside field {@code Y}, the corresponding field name value for the column
         *        key in the {@code columnToFieldMapping} map can be the string {@code "X__Y"}, in which case the value
         *        for {@code nestedObjectDelimiter} should be {code "_"}
         * @param outputNulls If false, omit fields with a null value.
         * @param timestampFieldName If not null, include a field of the given name with a publication timestamp.
         * @return A JSON spec for the given inputs.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec jsonSpec(
                final String[] includeColumns,
                final Predicate<String> excludeColumns,
                final Map<String, String> columnToFieldMapping,
                final String nestedObjectDelimiter,
                final boolean outputNulls,
                final String timestampFieldName) {
            if (includeColumns != null && excludeColumns != null) {
                throw new IllegalArgumentException(
                        "Both includeColumns (=" + includeColumns +
                                ") and excludeColumns (=" + excludeColumns + ") are not null, " +
                                "at least one of them should be null.");
            }
            return new JsonProduce(
                    includeColumns,
                    excludeColumns,
                    columnToFieldMapping,
                    nestedObjectDelimiter,
                    outputNulls,
                    timestampFieldName);
        }

        /**
         * A JSON spec from a set of column names. Shorthand for
         * {@code jsonSpec(columNames, excludeColumns, columnToFieldMapping, null, false, null)}
         *
         * @param includeColumns An array with an entry for each column intended to be included in the JSON output. If
         *        null, include all columns except those specified in {@code excludeColumns}. If {@code includeColumns}
         *        is not null, {@code excludeColumns} should be null.
         * @param excludeColumns A predicate specifying column names to omit; can only be used when {@code columnNames}
         *        is null. In this case all table columns except for the ones in {@code excludeColumns} will be
         *        included.
         * @param columnToFieldMapping A map from column name to JSON field name to use for that column. Any column
         *        names implied by earlier arguments not included as a key in the map will be mapped to JSON fields of
         *        the same name. If null, map all columns to fields of the same name.
         * @return A JSON spec for the given inputs.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec jsonSpec(
                final String[] includeColumns,
                final Predicate<String> excludeColumns,
                final Map<String, String> columnToFieldMapping) {
            return jsonSpec(includeColumns, excludeColumns, columnToFieldMapping, null, false, null);
        }

        /**
         * Avro spec to generate Avro messages from an Avro schema.
         *
         * @param schema An Avro schema. The message will implement this schema; all fields will be populated from some
         *        table column via explicit or implicit mapping.
         * @param fieldToColumnMapping A map from Avro schema field name to column name. Any field names not included as
         *        a key in the map will be mapped to columns with the same name (unless those columns are filtered out).
         *        If null, map all fields to columns of the same name (except for columns filtered out).
         * @param timestampFieldName If not null, include a field of the given name with a publication timestamp. The
         *        field with the given name should exist in the provided schema, and be of logical type timestamp
         *        micros.
         * @param includeOnlyColumns If not null, filter out any columns tested false in this predicate.
         * @param excludeColumns If not null, filter out any columns tested true in this predicate.
         * @return A spec corresponding to the schema provided.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec avroSpec(
                final Schema schema,
                final Map<String, String> fieldToColumnMapping,
                final String timestampFieldName,
                final Predicate<String> includeOnlyColumns,
                final Predicate<String> excludeColumns) {
            return new AvroProduce(schema, null, null, fieldToColumnMapping,
                    timestampFieldName, includeOnlyColumns, excludeColumns, false, null, null);
        }

        /**
         * Avro spec from fetching an Avro schema from a Confluent compatible Schema Server. The Properties used to
         * initialize Kafka should contain the URL for the Schema Server to use under the "schema.registry.url"
         * property.
         *
         * @param schemaName The registered name for the schema on Schema Server
         * @param schemaVersion The version to fetch. Pass the constant {@code AVRO_LATEST_VERSION} for latest
         * @param fieldToColumnMapping A map from Avro schema field name to column name. Any field names not included as
         *        a key in the map will be mapped to columns with the same name. If null, map all fields to columns of
         *        the same name.
         * @param timestampFieldName If not null, include a field of the given name with a publication timestamp. The
         *        field with the given name should exist in the provided schema, and be of logical type timestamp
         *        micros.
         * @param includeOnlyColumns If not null, filter out any columns tested false in this predicate.
         * @param excludeColumns If not null, filter out any columns tested true in this predicate.
         * @param publishSchema If true, instead of loading a schema already defined in schema server, define a new Avro
         *        schema based on the selected columns for this table and publish it to schema server. When publishing,
         *        if a schema version is provided and the version generated doesn't match, an exception results.
         * @param schemaNamespace When publishSchema is true, the namespace for the generated schema to be restered in
         *        schema server. When publishSchema is false, null should be passed.
         * @param columnProperties When publisSchema is true, a {@code Properties} object can be provided, specifying
         *        String properties implying particular Avro type mappings for them. In particular, column {@code X} of
         *        {@code BigDecimal} type should specify string properties {@code "x.precision"} and {@code "x.scale"}.
         * @return A spec corresponding to the schema provided.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec avroSpec(
                final String schemaName,
                final String schemaVersion,
                final Map<String, String> fieldToColumnMapping,
                final String timestampFieldName,
                final Predicate<String> includeOnlyColumns,
                final Predicate<String> excludeColumns,
                final boolean publishSchema,
                final String schemaNamespace,
                final Properties columnProperties) {
            return new AvroProduce(
                    null, schemaName, schemaVersion, fieldToColumnMapping,
                    timestampFieldName, includeOnlyColumns, excludeColumns, publishSchema,
                    schemaNamespace, columnProperties);
        }
    }

    /**
     * Type for the result {@link Table} returned by kafka consumers.
     */
    public interface TableType {

        static Blink blink() {
            return Blink.of();
        }

        static Append append() {
            return Append.of();
        }

        static Ring ring(int capacity) {
            return Ring.of(capacity);
        }

        <T> T walk(Visitor<T> visitor);

        interface Visitor<T> {
            T visit(Blink blink);

            T visit(Append append);

            T visit(Ring ring);
        }

        /**
         * <p>
         * Consume data into an in-memory blink table, which will present only newly-available rows to downstream
         * operations and visualizations.
         * <p>
         * See {@link Table#BLINK_TABLE_ATTRIBUTE} for a detailed explanation of blink table semantics, and
         * {@link BlinkTableTools} for related tooling.
         */
        @Immutable
        @SingletonStyle
        abstract class Blink implements TableType {

            public static Blink of() {
                return ImmutableBlink.of();
            }

            @Override
            public final <T> T walk(Visitor<T> visitor) {
                return visitor.visit(this);
            }
        }

        /**
         * Consume data into an in-memory append-only table.
         *
         * @see BlinkTableTools#blinkToAppendOnly(Table)
         */
        @Immutable
        @SingletonStyle
        abstract class Append implements TableType {

            public static Append of() {
                return ImmutableAppend.of();
            }

            @Override
            public final <T> T walk(Visitor<T> visitor) {
                return visitor.visit(this);
            }
        }

        /**
         * Consume data into an in-memory ring table.
         *
         * @see RingTableTools#of(Table, int)
         */
        @Immutable
        @SimpleStyle
        abstract class Ring implements TableType {

            public static Ring of(int capacity) {
                return ImmutableRing.of(capacity);
            }

            @Parameter
            public abstract int capacity();

            @Override
            public final <T> T walk(Visitor<T> visitor) {
                return visitor.visit(this);
            }
        }
    }

    /**
     * Map "Python-friendly" table type name to a {@link TableType}. Supported values are:
     * <ol>
     * <li>{@code "blink"}</li>
     * <li>{@code "stream"} (deprecated; use {@code "blink"})</li>
     * <li>{@code "append"}</li>
     * <li>{@code "ring:<capacity>"} where capacity is a integer number specifying the maximum number of trailing rows
     * to include in the result</li>
     * </ol>
     *
     * @param typeName The friendly name
     * @return The mapped {@link TableType}
     */
    @ScriptApi
    public static TableType friendlyNameToTableType(@NotNull final String typeName) {
        final String[] split = typeName.split(":");
        switch (split[0].trim()) {
            case "blink":
            case "stream": // TODO (https://github.com/deephaven/deephaven-core/issues/3853): Delete this
                if (split.length != 1) {
                    throw unexpectedType(typeName, null);
                }
                return TableType.blink();
            case "append":
                if (split.length != 1) {
                    throw unexpectedType(typeName, null);
                }
                return TableType.append();
            case "ring":
                if (split.length != 2) {
                    throw unexpectedType(typeName, null);
                }
                try {
                    return TableType.ring(Integer.parseInt(split[1].trim()));
                } catch (NumberFormatException e) {
                    throw unexpectedType(typeName, e);
                }
            default:
                throw unexpectedType(typeName, null);
        }
    }

    private static IllegalArgumentException unexpectedType(@NotNull final String typeName, @Nullable Exception cause) {
        return new IllegalArgumentException("Unexpected type format \"" + typeName
                + "\", expected \"blink\", \"append\", or \"ring:<capacity>\"", cause);
    }

    /**
     * Consume from Kafka to a Deephaven {@link Table}.
     *
     * @param kafkaProperties Properties to configure the result and also to be passed to create the KafkaConsumer
     * @param topic Kafka topic name
     * @param partitionFilter A predicate returning true for the partitions to consume. The convenience constant
     *        {@code ALL_PARTITIONS} is defined to facilitate requesting all partitions.
     * @param partitionToInitialOffset A function specifying the desired initial offset for each partition consumed
     * @param keySpec Conversion specification for Kafka record keys
     * @param valueSpec Conversion specification for Kafka record values
     * @param tableType {@link TableType} specifying the type of the expected result
     * @return The result {@link Table} containing Kafka stream data formatted according to {@code tableType}
     */
    @SuppressWarnings("unused")
    public static Table consumeToTable(
            @NotNull final Properties kafkaProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final IntToLongFunction partitionToInitialOffset,
            @NotNull final Consume.KeyOrValueSpec keySpec,
            @NotNull final Consume.KeyOrValueSpec valueSpec,
            @NotNull final TableType tableType) {
        final MutableObject<Table> resultHolder = new MutableObject<>();
        final ExecutionContext enclosingExecutionContext = ExecutionContext.getContext();
        final LivenessManager enclosingLivenessManager = LivenessScopeStack.peek();

        final SingleConsumerRegistrar registrar =
                (final TableDefinition tableDefinition, final StreamPublisher streamPublisher) -> {
                    try (final SafeCloseable ignored1 = enclosingExecutionContext.open();
                            final SafeCloseable ignored2 = LivenessScopeStack.open()) {
                        // StreamToBlinkTableAdapter registers itself in its constructor
                        // noinspection resource
                        final StreamToBlinkTableAdapter streamToBlinkTableAdapter = new StreamToBlinkTableAdapter(
                                tableDefinition,
                                streamPublisher,
                                enclosingExecutionContext.getUpdateGraph(),
                                "Kafka-" + topic + '-' + partitionFilter);
                        final Table blinkTable = streamToBlinkTableAdapter.table();
                        final Table result = tableType.walk(new BlinkTableOperation(blinkTable));
                        enclosingLivenessManager.manage(result);
                        resultHolder.setValue(result);
                    }
                };

        consume(kafkaProperties, topic, partitionFilter,
                InitialOffsetLookup.adapt(partitionToInitialOffset), keySpec, valueSpec,
                StreamConsumerRegistrarProvider.single(registrar), null);
        return resultHolder.getValue();
    }

    /**
     * Consume from Kafka to a Deephaven {@link PartitionedTable} containing one constituent {@link Table} per
     * partition.
     *
     * @param kafkaProperties Properties to configure the result and also to be passed to create the KafkaConsumer
     * @param topic Kafka topic name
     * @param partitionFilter A predicate returning true for the partitions to consume. The convenience constant
     *        {@code ALL_PARTITIONS} is defined to facilitate requesting all partitions.
     * @param partitionToInitialOffset A function specifying the desired initial offset for each partition consumed
     * @param keySpec Conversion specification for Kafka record keys
     * @param valueSpec Conversion specification for Kafka record values
     * @param tableType {@link TableType} specifying the type of the expected result's constituent tables
     * @return The result {@link PartitionedTable} containing Kafka stream data formatted according to {@code tableType}
     */
    @SuppressWarnings("unused")
    public static PartitionedTable consumeToPartitionedTable(
            @NotNull final Properties kafkaProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final IntToLongFunction partitionToInitialOffset,
            @NotNull final Consume.KeyOrValueSpec keySpec,
            @NotNull final Consume.KeyOrValueSpec valueSpec,
            @NotNull final TableType tableType) {
        final AtomicReference<StreamPartitionedQueryTable> resultHolder = new AtomicReference<>();
        final ExecutionContext enclosingExecutionContext = ExecutionContext.getContext();
        final LivenessManager enclosingLivenessManager = LivenessScopeStack.peek();

        final PerPartitionConsumerRegistrar registrar =
                (final TableDefinition tableDefinition, final TopicPartition topicPartition,
                        final StreamPublisher streamPublisher) -> {
                    try (final SafeCloseable ignored1 = enclosingExecutionContext.open();
                            final SafeCloseable ignored2 = LivenessScopeStack.open()) {
                        StreamPartitionedQueryTable result = resultHolder.get();
                        if (result == null) {
                            synchronized (resultHolder) {
                                result = resultHolder.get();
                                if (result == null) {
                                    result = new StreamPartitionedQueryTable(tableDefinition);
                                    enclosingLivenessManager.manage(result);
                                    resultHolder.set(result);
                                }
                            }
                        }
                        // StreamToBlinkTableAdapter registers itself in its constructor
                        // noinspection resource
                        final StreamToBlinkTableAdapter streamToBlinkTableAdapter = new StreamToBlinkTableAdapter(
                                tableDefinition,
                                streamPublisher,
                                result.getRegistrar(),
                                "Kafka-" + topic + '-' + topicPartition.partition());
                        final Table blinkTable = streamToBlinkTableAdapter.table();
                        final Table derivedTable = tableType.walk(new BlinkTableOperation(blinkTable));
                        result.enqueueAdd(topicPartition.partition(), derivedTable);
                    }
                };

        consume(kafkaProperties, topic, partitionFilter,
                InitialOffsetLookup.adapt(partitionToInitialOffset), keySpec, valueSpec,
                StreamConsumerRegistrarProvider.perPartition(registrar), null);
        return resultHolder.get().toPartitionedTable();
    }

    @FunctionalInterface
    public interface SingleConsumerRegistrar {

        /**
         * Provide a single {@link StreamConsumer} to {@code streamPublisher} via its
         * {@link StreamPublisher#register(StreamConsumer) register} method.
         *
         * @param tableDefinition The {@link TableDefinition} for the stream to be consumed
         * @param streamPublisher The {@link StreamPublisher} to {@link StreamPublisher#register(StreamConsumer)
         *        register} a {@link StreamConsumer} for
         */
        void register(
                @NotNull TableDefinition tableDefinition,
                @NotNull StreamPublisher streamPublisher);
    }

    @FunctionalInterface
    public interface PerPartitionConsumerRegistrar {

        /**
         * Provide a per-partition {@link StreamConsumer} to {@code streamPublisher} via its
         * {@link StreamPublisher#register(StreamConsumer) register} method.
         *
         * @param tableDefinition The {@link TableDefinition} for the stream to be consumed
         * @param topicPartition The {@link TopicPartition} to be consumed
         * @param streamPublisher The {@link StreamPublisher} to {@link StreamPublisher#register(StreamConsumer)
         *        register} a {@link StreamConsumer} for
         */
        void register(
                @NotNull TableDefinition tableDefinition,
                @NotNull TopicPartition topicPartition,
                @NotNull StreamPublisher streamPublisher);
    }

    /**
     * Marker interface for {@link StreamConsumer} registrar provider objects.
     */
    public interface StreamConsumerRegistrarProvider {

        /**
         * @param registrar The internal registrar method for {@link StreamConsumer} instances
         * @return A StreamConsumerRegistrarProvider that registers a single consumer for all selected partitions
         */
        static Single single(@NotNull final SingleConsumerRegistrar registrar) {
            return Single.of(registrar);
        }

        /**
         * @param registrar The internal registrar method for {@link StreamConsumer} instances
         * @return A StreamConsumerRegistrarProvider that registers a new consumer for each selected partition
         */
        static PerPartition perPartition(@NotNull final PerPartitionConsumerRegistrar registrar) {
            return PerPartition.of(registrar);
        }

        <T> T walk(Visitor<T> visitor);

        interface Visitor<T> {
            T visit(@NotNull Single single);

            T visit(@NotNull PerPartition perPartition);
        }

        @Immutable
        @SimpleStyle
        abstract class Single implements StreamConsumerRegistrarProvider {

            public static Single of(@NotNull final SingleConsumerRegistrar registrar) {
                return ImmutableSingle.of(registrar);
            }

            @Parameter
            public abstract SingleConsumerRegistrar registrar();

            @Override
            public final <T> T walk(@NotNull final Visitor<T> visitor) {
                return visitor.visit(this);
            }
        }

        @Immutable
        @SimpleStyle
        abstract class PerPartition implements StreamConsumerRegistrarProvider {

            public static PerPartition of(@NotNull final PerPartitionConsumerRegistrar registrar) {
                return ImmutablePerPartition.of(registrar);
            }

            @Parameter
            public abstract PerPartitionConsumerRegistrar registrar();

            @Override
            public final <T> T walk(@NotNull final Visitor<T> visitor) {
                return visitor.visit(this);
            }
        }
    }

    private static class KafkaRecordConsumerFactoryCreator
            implements StreamConsumerRegistrarProvider.Visitor<Function<TopicPartition, KafkaRecordConsumer>> {

        private final KafkaStreamPublisher.Parameters publisherParameters;
        private final Supplier<KafkaIngester> ingesterSupplier;

        private KafkaRecordConsumerFactoryCreator(
                @NotNull final KafkaStreamPublisher.Parameters publisherParameters,
                @NotNull final Supplier<KafkaIngester> ingesterSupplier) {
            this.publisherParameters = publisherParameters;
            this.ingesterSupplier = ingesterSupplier;
        }

        @Override
        public Function<TopicPartition, KafkaRecordConsumer> visit(@NotNull final Single single) {
            final ConsumerRecordToStreamPublisherAdapter adapter = KafkaStreamPublisher.make(
                    publisherParameters,
                    () -> ingesterSupplier.get().shutdown());
            single.registrar().register(publisherParameters.getTableDefinition(), adapter);
            return (final TopicPartition tp) -> new SimpleKafkaRecordConsumer(adapter);
        }

        @Override
        public Function<TopicPartition, KafkaRecordConsumer> visit(@NotNull final PerPartition perPartition) {
            return (final TopicPartition tp) -> {
                final ConsumerRecordToStreamPublisherAdapter adapter = KafkaStreamPublisher.make(
                        publisherParameters,
                        () -> ingesterSupplier.get().shutdownPartition(tp.partition()));
                perPartition.registrar().register(publisherParameters.getTableDefinition(), tp, adapter);
                return new SimpleKafkaRecordConsumer(adapter);
            };
        }
    }

    /**
     * Consume from Kafka to {@link StreamConsumer stream consumers} supplied by {@code streamConsumerRegistrar}.
     *
     * @param kafkaProperties Properties to configure this table and also to be passed to create the KafkaConsumer
     * @param topic Kafka topic name
     * @param partitionFilter A predicate returning true for the partitions to consume. The convenience constant
     *        {@code ALL_PARTITIONS} is defined to facilitate requesting all partitions.
     * @param partitionToInitialOffset A function specifying the desired initial offset for each partition consumed
     * @param keySpec Conversion specification for Kafka record keys
     * @param valueSpec Conversion specification for Kafka record values
     * @param streamConsumerRegistrarProvider A provider for a function to
     *        {@link StreamPublisher#register(StreamConsumer) register} {@link StreamConsumer} instances. The registered
     *        stream consumers must accept {@link ChunkType chunk types} that correspond to
     *        {@link StreamChunkUtils#chunkTypeForColumnIndex(TableDefinition, int)} for the supplied
     *        {@link TableDefinition}. See {@link StreamConsumerRegistrarProvider#single(SingleConsumerRegistrar)
     *        single} and {@link StreamConsumerRegistrarProvider#perPartition(PerPartitionConsumerRegistrar)
     *        per-partition}.
     * @param consumerLoopCallback callback to inject logic into the ingester's consumer loop
     */
    public static void consume(
            @NotNull final Properties kafkaProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final InitialOffsetLookup partitionToInitialOffset,
            @NotNull final Consume.KeyOrValueSpec keySpec,
            @NotNull final Consume.KeyOrValueSpec valueSpec,
            @NotNull final StreamConsumerRegistrarProvider streamConsumerRegistrarProvider,
            @Nullable final ConsumerLoopCallback consumerLoopCallback) {
        if (Consume.isIgnore(keySpec) && Consume.isIgnore(valueSpec)) {
            throw new IllegalArgumentException(
                    "can't ignore both key and value: keySpec and valueSpec can't both be ignore specs");
        }

        final Map<String, ?> configs = asStringMap(kafkaProperties);
        final SchemaRegistryClient schemaRegistryClient =
                schemaRegistryClient(keySpec, valueSpec, configs).orElse(null);

        final Deserializer<?> keyDeser = keySpec.getDeserializer(KeyOrValue.KEY, schemaRegistryClient, configs);
        keyDeser.configure(configs, true);

        final Deserializer<?> valueDeser = valueSpec.getDeserializer(KeyOrValue.VALUE, schemaRegistryClient, configs);
        valueDeser.configure(configs, false);

        final KafkaStreamPublisher.Parameters.Builder publisherParametersBuilder =
                KafkaStreamPublisher.Parameters.builder();

        final MutableInt nextColumnIndex = new MutableInt(0);
        final List<ColumnDefinition<?>> columnDefinitions = new ArrayList<>();

        Arrays.stream(CommonColumn.values())
                .forEach(cc -> {
                    final ColumnDefinition<?> commonColumnDefinition = cc.getDefinition(kafkaProperties);
                    if (commonColumnDefinition == null) {
                        return;
                    }
                    columnDefinitions.add(commonColumnDefinition);
                    cc.setColumnIndex.setColumnIndex(publisherParametersBuilder, nextColumnIndex.getAndIncrement());
                });

        final KeyOrValueIngestData keyIngestData = keySpec.getIngestData(KeyOrValue.KEY,
                schemaRegistryClient, configs, nextColumnIndex, columnDefinitions);
        final KeyOrValueIngestData valueIngestData = valueSpec.getIngestData(KeyOrValue.VALUE,
                schemaRegistryClient, configs, nextColumnIndex, columnDefinitions);

        final TableDefinition tableDefinition = TableDefinition.of(columnDefinitions);
        publisherParametersBuilder.setTableDefinition(tableDefinition);

        if (keyIngestData != null) {
            publisherParametersBuilder
                    .setKeyProcessor(keySpec.getProcessor(tableDefinition, keyIngestData))
                    .setSimpleKeyColumnIndex(keyIngestData.simpleColumnIndex)
                    .setKeyToChunkObjectMapper(keyIngestData.toObjectChunkMapper);
        }
        if (valueIngestData != null) {
            publisherParametersBuilder
                    .setValueProcessor(valueSpec.getProcessor(tableDefinition, valueIngestData))
                    .setSimpleValueColumnIndex(valueIngestData.simpleColumnIndex)
                    .setValueToChunkObjectMapper(valueIngestData.toObjectChunkMapper);
        }

        final KafkaStreamPublisher.Parameters publisherParameters = publisherParametersBuilder.build();
        final MutableObject<KafkaIngester> kafkaIngesterHolder = new MutableObject<>();

        final Function<TopicPartition, KafkaRecordConsumer> kafkaRecordConsumerFactory =
                streamConsumerRegistrarProvider.walk(
                        new KafkaRecordConsumerFactoryCreator(publisherParameters, kafkaIngesterHolder::getValue));

        final KafkaIngester ingester = new KafkaIngester(
                log,
                kafkaProperties,
                topic,
                partitionFilter,
                kafkaRecordConsumerFactory,
                partitionToInitialOffset,
                keyDeser,
                valueDeser,
                consumerLoopCallback);
        kafkaIngesterHolder.setValue(ingester);
        ingester.start();
    }

    private static Optional<SchemaRegistryClient> schemaRegistryClient(SchemaProviderProvider key,
            SchemaProviderProvider value,
            Map<String, ?> configs) {
        final Map<String, SchemaProvider> providers = new HashMap<>();
        key.getSchemaProvider().ifPresent(p -> providers.put(p.schemaType(), p));
        value.getSchemaProvider().ifPresent(p -> providers.putIfAbsent(p.schemaType(), p));
        if (providers.isEmpty()) {
            return Optional.empty();
        }
        for (SchemaProvider schemaProvider : providers.values()) {
            schemaProvider.configure(configs);
        }
        return Optional.of(newSchemaRegistryClient(configs, List.copyOf(providers.values())));
    }

    static SchemaRegistryClient newSchemaRegistryClient(Map<String, ?> configs, List<SchemaProvider> providers) {
        // Note: choosing to not use the constructor with doLog which is a newer API; this is in support of downstream
        // users _potentially_ being able to replace kafka jars with previous versions.
        final AbstractKafkaSchemaSerDeConfig config = new AbstractKafkaSchemaSerDeConfig(
                AbstractKafkaSchemaSerDeConfig.baseConfigDef(),
                configs);
        // Note: choosing to not use SchemaRegistryClientFactory.newClient which is a newer API; this is in support of
        // downstream users _potentially_ being able to replace kafka jars with previous versions.
        return new CachedSchemaRegistryClient(
                config.getSchemaRegistryUrls(),
                config.getMaxSchemasPerSubject(),
                List.copyOf(providers),
                config.originalsWithPrefix(""),
                config.requestHeaders());
    }

    private static class BlinkTableOperation implements Visitor<Table> {
        private final Table blinkTable;

        public BlinkTableOperation(Table blinkTable) {
            this.blinkTable = Objects.requireNonNull(blinkTable);
        }

        @Override
        public Table visit(Blink blink) {
            return blinkTable;
        }

        @Override
        public Table visit(Append append) {
            return BlinkTableTools.blinkToAppendOnly(blinkTable);
        }

        @Override
        public Table visit(Ring ring) {
            return RingTableTools.of(blinkTable, ring.capacity());
        }
    }

    /**
     * Produce a Kafka stream from a Deephaven table.
     * <p>
     * Note that {@code table} must only change in ways that are meaningful when turned into a stream of events over
     * Kafka.
     * <p>
     * Two primary use cases are considered:
     * <ol>
     * <li><b>A stream of changes (puts and removes) to a key-value data set.</b> In order to handle this efficiently
     * and allow for correct reconstruction of the state at a consumer, it is assumed that the input data is the result
     * of a Deephaven aggregation, e.g. {@link Table#aggAllBy}, {@link Table#aggBy}, or {@link Table#lastBy}. This means
     * that key columns (as specified by {@code keySpec}) must not be modified, and no rows should be shifted if there
     * are any key columns. Note that specifying {@code lastByKeyColumns=true} can make it easy to satisfy this
     * constraint if the input data is not already aggregated.</li>
     * <li><b>A stream of independent log records.</b> In this case, the input table should either be a
     * {@link Table#BLINK_TABLE_ATTRIBUTE blink table} or should only ever add rows (regardless of whether the
     * {@link Table#ADD_ONLY_TABLE_ATTRIBUTE attribute} is specified).</li>
     * </ol>
     * <p>
     * If other use cases are identified, a publication mode or extensible listener framework may be introduced at a
     * later date.
     *
     * @param table The table used as a source of data to be sent to Kafka.
     * @param kafkaProperties Properties to be passed to create the associated KafkaProducer.
     * @param topic Kafka topic name
     * @param keySpec Conversion specification for Kafka record keys from table column data.
     * @param valueSpec Conversion specification for Kafka record values from table column data.
     * @param lastByKeyColumns Whether to publish only the last record for each unique key. Ignored when {@code keySpec}
     *        is {@code IGNORE}. Otherwise, if {@code lastByKeycolumns == true} this method will internally perform a
     *        {@link Table#lastBy(String...) lastBy} aggregation on {@code table} grouped by the input columns of
     *        {@code keySpec} and publish to Kafka from the result.
     * @return a callback to stop producing and shut down the associated table listener; note a caller should keep a
     *         reference to this return value to ensure liveliness.
     * @see #produceFromTable(KafkaPublishOptions)
     */
    @SuppressWarnings("unused")
    public static Runnable produceFromTable(
            @NotNull final Table table,
            @NotNull final Properties kafkaProperties,
            @NotNull final String topic,
            @NotNull final Produce.KeyOrValueSpec keySpec,
            @NotNull final Produce.KeyOrValueSpec valueSpec,
            final boolean lastByKeyColumns) {
        return produceFromTable(KafkaPublishOptions.builder()
                .table(table)
                .topic(topic)
                .config(kafkaProperties)
                .keySpec(keySpec)
                .valueSpec(valueSpec)
                .lastBy(lastByKeyColumns && !Produce.isIgnore(keySpec))
                .publishInitial(true)
                .build());
    }

    /**
     * Produce a Kafka stream from a Deephaven table.
     *
     * <p>
     * Note that {@code table} must only change in ways that are meaningful when turned into a stream of events over
     * Kafka.
     * <p>
     * Two primary use cases are considered:
     * <ol>
     * <li><b>A stream of changes (puts and removes) to a key-value data set.</b> In order to handle this efficiently
     * and allow for correct reconstruction of the state at a consumer, it is assumed that the input data is the result
     * of a Deephaven aggregation, e.g. {@link Table#aggAllBy}, {@link Table#aggBy}, or {@link Table#lastBy}. This means
     * that key columns (as specified by {@code keySpec}) must not be modified, and no rows should be shifted if there
     * are any key columns. Note that specifying {@code lastByKeyColumns=true} can make it easy to satisfy this
     * constraint if the input data is not already aggregated.</li>
     * <li><b>A stream of independent log records.</b> In this case, the input table should either be a
     * {@link Table#BLINK_TABLE_ATTRIBUTE blink table} or should only ever add rows (regardless of whether the
     * {@link Table#ADD_ONLY_TABLE_ATTRIBUTE attribute} is specified).</li>
     * </ol>
     * <p>
     * If other use cases are identified, a publication mode or extensible listener framework may be introduced at a
     * later date.
     *
     * @param options the options
     * @return a callback to stop producing and shut down the associated table listener; note a caller should keep a
     *         reference to this return value to ensure liveliness.
     */
    public static Runnable produceFromTable(KafkaPublishOptions options) {
        final Table table = options.table();
        try {
            QueryTable.checkInitiateOperation(table);
        } catch (IllegalStateException e) {
            throw new KafkaPublisherException(
                    "Calling thread must hold an exclusive or shared UpdateGraph lock to publish live sources", e);
        }
        final Map<String, ?> config = asStringMap(options.config());
        final KeyOrValueSpec keySpec = options.keySpec();
        final KeyOrValueSpec valueSpec = options.valueSpec();
        final SchemaRegistryClient schemaRegistryClient = schemaRegistryClient(keySpec, valueSpec, config).orElse(null);

        final TableDefinition tableDefinition = table.getDefinition();
        final Serializer<?> keySpecSerializer = keySpec.getSerializer(schemaRegistryClient, tableDefinition);
        keySpecSerializer.configure(config, true);

        final Serializer<?> valueSpecSerializer = valueSpec.getSerializer(schemaRegistryClient, tableDefinition);
        valueSpecSerializer.configure(config, false);

        final String[] keyColumns = keySpec.getColumnNames(table, schemaRegistryClient);
        final String[] valueColumns = valueSpec.getColumnNames(table, schemaRegistryClient);

        final LivenessScope publisherScope = new LivenessScope(true);
        try (final SafeCloseable ignored = LivenessScopeStack.open(publisherScope, false)) {
            final Table effectiveTable = options.lastBy()
                    ? table.lastBy(keyColumns)
                    : table.coalesce();
            final KeyOrValueSerializer<?> keySerializer = keySpec.getKeyOrValueSerializer(effectiveTable, keyColumns);
            final KeyOrValueSerializer<?> valueSerializer =
                    valueSpec.getKeyOrValueSerializer(effectiveTable, valueColumns);
            // PublishToKafka is a LivenessArtifact; it will be kept reachable and alive by the publisherScope, since
            // it is constructed with enforceStrongReachability=true.
            new PublishToKafka(
                    options.config(),
                    effectiveTable,
                    options.topic(),
                    options.partition().isEmpty() ? null : options.partition().getAsInt(),
                    keyColumns,
                    keySpecSerializer,
                    keySerializer,
                    valueColumns,
                    valueSpecSerializer,
                    valueSerializer,
                    options.topicColumn().orElse(null),
                    options.partitionColumn().orElse(null),
                    options.timestampColumn().orElse(null),
                    options.publishInitial());
        }
        return publisherScope::release;
    }

    /**
     * @implNote The constructor publishes {@code this} (indirectly) to the {@link UpdateGraph} and cannot be
     *           subclassed.
     */
    private static final class StreamPartitionedQueryTable extends QueryTable implements Runnable {

        private static final String PARTITION_COLUMN_NAME = "Partition";
        private static final String CONSTITUENT_COLUMN_NAME = "Table";

        private final TableDefinition constituentDefinition;

        private final WritableColumnSource<Integer> partitionColumn;
        private final WritableColumnSource<Table> constituentColumn;

        @ReferentialIntegrity
        private final UpdateSourceCombiner refreshCombiner;

        private volatile long lastAddedPartitionRowKey = -1L; // NULL_ROW_KEY

        private StreamPartitionedQueryTable(@NotNull final TableDefinition constituentDefinition) {
            super(RowSetFactory.empty().toTracking(), makeSources());

            setFlat();
            setRefreshing(true);

            this.constituentDefinition = constituentDefinition;

            partitionColumn = (WritableColumnSource<Integer>) getColumnSource(PARTITION_COLUMN_NAME, int.class);
            constituentColumn = (WritableColumnSource<Table>) getColumnSource(CONSTITUENT_COLUMN_NAME, Table.class);

            /*
             * We use an UpdateSourceCombiner to drive both the StreamPartitionedQueryTable and its constituents, with
             * the StreamPartitionedQueryTable added first. This to ensure that newly-added constituents cannot process
             * their initial update until after they have been added to the StreamPartitionedQueryTable. The
             * StreamPartitionedQueryTable creates the combiner, and constituent-building code is responsible for using
             * it, accessed via getRegistrar(). We could remove the combiner and use the "raw" UpdateGraph plus a
             * ConstituentDependency if we demanded a guarantee that new constituents would only be constructed in such
             * a way that they were unable to fire before being added to the partitioned table. Without that guarantee,
             * we would risk data loss for blink or ring tables.
             */
            refreshCombiner = new UpdateSourceCombiner(getUpdateGraph());
            manage(refreshCombiner);
            refreshCombiner.addSource(this);

            /*
             * We use a ConstituentDependency to ensure that our notification is not delivered before all of our
             * constituents have become satisfied on this cycle. For "raw" (blink) constituents, the
             * UpdateSourceCombiner trivially guarantees this. However, since constituents may be transformed to ring or
             * append-only tables before they are added to the StreamPartitionedQueryTable, we must test constituent
             * satisfaction before allowing the table to become satisfied on a given cycle.
             */
            ConstituentDependency.install(this, refreshCombiner);

            // Begin update processing
            refreshCombiner.install();
        }

        /**
         * @return The {@link UpdateSourceRegistrar} to be used for all constituent roots.
         */
        public UpdateSourceRegistrar getRegistrar() {
            return refreshCombiner;
        }

        @Override
        public void run() {
            final WritableRowSet rowSet = getRowSet().writableCast();

            final long newLastRowKey = lastAddedPartitionRowKey;
            final long oldLastRowKey = rowSet.lastRowKey();

            if (newLastRowKey != oldLastRowKey) {
                final RowSet added = RowSetFactory.fromRange(oldLastRowKey + 1, newLastRowKey);
                rowSet.insert(added);
                notifyListeners(new TableUpdateImpl(added,
                        RowSetFactory.empty(), RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            }
        }

        private synchronized void enqueueAdd(final int partition, @NotNull final Table partitionTable) {
            manage(partitionTable);

            final long partitionRowKey = lastAddedPartitionRowKey + 1;

            partitionColumn.ensureCapacity(partitionRowKey + 1);
            partitionColumn.set(partitionRowKey, partition);

            constituentColumn.ensureCapacity(partitionRowKey + 1);
            constituentColumn.set(partitionRowKey, partitionTable);

            lastAddedPartitionRowKey = partitionRowKey;
        }

        private PartitionedTable toPartitionedTable() {
            return new PartitionedTableImpl(this,
                    Set.of(PARTITION_COLUMN_NAME),
                    true, // keys are unique
                    CONSTITUENT_COLUMN_NAME,
                    constituentDefinition,
                    true, // constituent changes are permitted
                    false); // validation not needed
        }

        private static Map<String, ColumnSource<?>> makeSources() {
            final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>(2);
            sources.put(PARTITION_COLUMN_NAME, ArrayBackedColumnSource.getMemoryColumnSource(int.class, null));
            sources.put(CONSTITUENT_COLUMN_NAME, ArrayBackedColumnSource.getMemoryColumnSource(Table.class, null));
            return sources;
        }
    }

    public static class KeyOrValueIngestData {
        public Map<String, String> fieldPathToColumnName;
        public int simpleColumnIndex = NULL_COLUMN_INDEX;
        public Function<Object, Object> toObjectChunkMapper = Function.identity();
        public Object extra;
    }

    private interface SetColumnIndex {
        void setColumnIndex(KafkaStreamPublisher.Parameters.Builder builder, int columnIndex);
    }

    private enum CommonColumn {
        // @formatter:off
        KafkaPartition(
                KAFKA_PARTITION_COLUMN_NAME_PROPERTY,
                KAFKA_PARTITION_COLUMN_NAME_DEFAULT,
                ColumnDefinition::ofInt,
                KafkaStreamPublisher.Parameters.Builder::setKafkaPartitionColumnIndex),
        Offset(
                OFFSET_COLUMN_NAME_PROPERTY,
                OFFSET_COLUMN_NAME_DEFAULT,
                ColumnDefinition::ofLong,
                KafkaStreamPublisher.Parameters.Builder::setOffsetColumnIndex),
        Timestamp(
                TIMESTAMP_COLUMN_NAME_PROPERTY,
                TIMESTAMP_COLUMN_NAME_DEFAULT,
                ColumnDefinition::ofTime,
                KafkaStreamPublisher.Parameters.Builder::setTimestampColumnIndex),

        ReceiveTime(
                RECEIVE_TIME_COLUMN_NAME_PROPERTY,
                RECEIVE_TIME_COLUMN_NAME_DEFAULT,
                ColumnDefinition::ofTime,
                KafkaStreamPublisher.Parameters.Builder::setReceiveTimeColumnIndex),

        KeyBytes(
                KEY_BYTES_COLUMN_NAME_PROPERTY,
                KEY_BYTES_COLUMN_NAME_DEFAULT,
                ColumnDefinition::ofInt,
                KafkaStreamPublisher.Parameters.Builder::setKeyBytesColumnIndex),

        ValueBytes(
                VALUE_BYTES_COLUMN_NAME_PROPERTY,
                VALUE_BYTES_COLUMN_NAME_DEFAULT,
                ColumnDefinition::ofInt,
                KafkaStreamPublisher.Parameters.Builder::setValueBytesColumnIndex);
        // @formatter:on

        private final String nameProperty;
        private final String nameDefault;
        private final Function<String, ColumnDefinition<?>> definitionFactory;
        private final SetColumnIndex setColumnIndex;

        CommonColumn(@NotNull final String nameProperty,
                @Nullable final String nameDefault,
                @NotNull final Function<String, ColumnDefinition<?>> definitionFactory,
                @NotNull final SetColumnIndex setColumnIndex) {
            this.nameProperty = nameProperty;
            this.nameDefault = nameDefault;
            this.definitionFactory = definitionFactory;
            this.setColumnIndex = setColumnIndex;
        }

        private ColumnDefinition<?> getDefinition(@NotNull final Properties consumerProperties) {
            final ColumnDefinition<?> result;
            if (consumerProperties.containsKey(nameProperty)) {
                final String commonColumnName = consumerProperties.getProperty(nameProperty);
                if (commonColumnName == null || commonColumnName.equals("")) {
                    result = null;
                } else {
                    result = definitionFactory.apply(commonColumnName);
                }
            } else if (nameDefault == null) {
                result = null;
            } else {
                result = definitionFactory.apply(nameDefault);
            }
            return result;
        }
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
    public static final Function<String, String> DIRECT_MAPPING =
            fieldName -> fieldName.replace(NESTED_FIELD_NAME_SEPARATOR, NESTED_FIELD_COLUMN_NAME_SEPARATOR);

    /**
     * The names for the key or value columns can be provided in the properties as {@value KEY_COLUMN_NAME_PROPERTY} or
     * {@value VALUE_COLUMN_NAME_PROPERTY}, and otherwise default to {@value KEY_COLUMN_NAME_DEFAULT} or
     * {@value VALUE_COLUMN_NAME_DEFAULT}. The types for key or value are either specified in the properties as
     * {@value KEY_COLUMN_TYPE_PROPERTY} or {@value VALUE_COLUMN_TYPE_PROPERTY} or deduced from the serializer classes
     * for {@value ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} or
     * {@value ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} in the provided Properties object.
     */
    @SuppressWarnings("unused")
    public static final Consume.KeyOrValueSpec FROM_PROPERTIES = Consume.FROM_PROPERTIES;

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
    public static Predicate<String> predicateFromSet(final Set<String> set) {
        return (set == null) ? null : set::contains;
    }

    private static class SimpleKafkaRecordConsumer implements KafkaRecordConsumer {

        private final ConsumerRecordToStreamPublisherAdapter adapter;

        private SimpleKafkaRecordConsumer(@NotNull final ConsumerRecordToStreamPublisherAdapter adapter) {
            this.adapter = adapter;
        }

        @Override
        public long consume(final long receiveTime,
                @NotNull final List<? extends ConsumerRecord<?, ?>> consumerRecords) {
            try {
                return adapter.consumeRecords(receiveTime, consumerRecords);
            } catch (Exception e) {
                acceptFailure(e);
                return 0;
            }
        }

        @Override
        public void acceptFailure(@NotNull final Throwable cause) {
            adapter.propagateFailure(cause);
        }
    }

    public static Set<String> topics(@NotNull final Properties kafkaProperties) {
        try (final Admin admin = Admin.create(kafkaProperties)) {
            final ListTopicsResult result = admin.listTopics();
            return result.names().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to list Kafka Topics for " + kafkaProperties, e);
        }
    }

    public static String[] listTopics(@NotNull final Properties kafkaProperties) {
        final Set<String> topics = topics(kafkaProperties);
        final String[] r = new String[topics.size()];
        return topics.toArray(r);
    }

    static Map<String, ?> asStringMap(Map<?, ?> map) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            final Object key = entry.getKey();
            if (!(key instanceof String)) {
                throw new UncheckedDeephavenException(String.format(
                        "key must be a string, is key.getClass().getName()=%s, key.toString()=%s",
                        key.getClass().getName(),
                        key));
            }
        }
        // noinspection unchecked
        return (Map<String, ?>) map;
    }

    private static class IntToLongLookupAdapter implements InitialOffsetLookup {
        private final IntToLongFunction function;

        IntToLongLookupAdapter(IntToLongFunction function) {
            this.function = Objects.requireNonNull(function);
        }

        @Override
        public long getInitialOffset(final KafkaConsumer<?, ?> consumer, final TopicPartition topicPartition) {
            return function.applyAsLong(topicPartition.partition());
        }
    }
}
