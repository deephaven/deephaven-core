/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.KafkaTools.Produce;
import io.deephaven.kafka.ingest.GenericRecordChunkAdapter;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.publish.GenericRecordKeyOrValueSerializer;
import io.deephaven.kafka.publish.KeyOrValueSerializer;
import io.deephaven.stream.StreamChunkUtils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.deephaven.kafka.KafkaTools.AVRO_LATEST_VERSION;
import static io.deephaven.kafka.KafkaTools.NESTED_FIELD_NAME_SEPARATOR;
import static io.deephaven.kafka.KafkaTools.avroSchemaToColumnDefinitions;

class AvroImpl {

    static final class AvroConsume extends Consume.KeyOrValueSpec {
        private static final Pattern NESTED_FIELD_NAME_SEPARATOR_PATTERN =
                Pattern.compile(Pattern.quote(NESTED_FIELD_NAME_SEPARATOR));

        private final Schema schema;
        private final String schemaName;
        private final String schemaVersion;
        /** fields mapped to null are skipped. */
        private final Function<String, String> fieldPathToColumnName;

        AvroConsume(final Schema schema, final Function<String, String> fieldPathToColumnName) {
            this.schema = schema;
            this.schemaName = null;
            this.schemaVersion = null;
            this.fieldPathToColumnName = fieldPathToColumnName;
        }

        AvroConsume(final String schemaName,
                final String schemaVersion,
                final Function<String, String> fieldPathToColumnName) {
            this.schema = null;
            this.schemaName = schemaName;
            this.schemaVersion = schemaVersion;
            this.fieldPathToColumnName = fieldPathToColumnName;
        }

        @Override
        boolean isIgnore() {
            return false;
        }

        @Override
        public Optional<SchemaProvider> schemaProvider() {
            return Optional.of(new AvroSchemaProvider());
        }

        @Override
        Deserializer<?> deserializer(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs) {
            return new KafkaAvroDeserializer(Objects.requireNonNull(schemaRegistryClient));
        }

        @Override
        KeyOrValueIngestData ingestData(KeyOrValue keyOrValue,
                List<ColumnDefinition<?>> columnDefinitionsOut, MutableInt nextColumnIndexMut,
                SchemaRegistryClient schemaRegistryClient, Map<String, ?> configs) {
            KeyOrValueIngestData data = new KeyOrValueIngestData();
            data.fieldPathToColumnName = new HashMap<>();
            final Schema localSchema = schema != null
                    ? schema
                    : getAvroSchema(schemaRegistryClient, schemaName, schemaVersion);
            avroSchemaToColumnDefinitions(columnDefinitionsOut, data.fieldPathToColumnName, localSchema,
                    fieldPathToColumnName);
            data.extra = localSchema;
            return data;
        }

        @Override
        KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
            return GenericRecordChunkAdapter.make(
                    tableDef,
                    ci -> StreamChunkUtils.chunkTypeForColumnIndex(tableDef, ci),
                    data.fieldPathToColumnName,
                    NESTED_FIELD_NAME_SEPARATOR_PATTERN,
                    (Schema) data.extra,
                    true);
        }
    }

    static final class AvroProduce extends Produce.KeyOrValueSpec {
        private Schema schema;
        private final String schemaName;
        private final String schemaVersion;
        final Map<String, String> fieldToColumnMapping;
        private final String timestampFieldName;
        private final Predicate<String> includeOnlyColumns;
        private final Predicate<String> excludeColumns;
        private final boolean publishSchema;
        private final String schemaNamespace;
        private final MutableObject<Properties> columnProperties;

        AvroProduce(final Schema schema,
                final String schemaName,
                final String schemaVersion,
                final Map<String, String> fieldToColumnMapping,
                final String timestampFieldName,
                final Predicate<String> includeOnlyColumns,
                final Predicate<String> excludeColumns,
                final boolean publishSchema,
                final String schemaNamespace,
                final Properties columnProperties) {
            this.schema = schema;
            this.schemaName = schemaName;
            this.schemaVersion = schemaVersion;
            this.fieldToColumnMapping = fieldToColumnMapping;
            this.timestampFieldName = timestampFieldName;
            this.includeOnlyColumns = includeOnlyColumns;
            this.excludeColumns = excludeColumns;
            this.publishSchema = publishSchema;
            this.schemaNamespace = schemaNamespace;
            this.columnProperties = new MutableObject<>(columnProperties);
            if (publishSchema) {
                if (schemaVersion != null && !AVRO_LATEST_VERSION.equals(schemaVersion)) {
                    throw new IllegalArgumentException(
                            String.format("schemaVersion must be null or \"%s\" when publishSchema=true",
                                    AVRO_LATEST_VERSION));
                }
            }
        }

        @Override
        boolean isIgnore() {
            return false;
        }

        @Override
        public Optional<SchemaProvider> schemaProvider() {
            return Optional.of(new AvroSchemaProvider());
        }

        @Override
        Serializer<?> serializer(SchemaRegistryClient schemaRegistryClient, TableDefinition definition) {
            return new KafkaAvroSerializer(Objects.requireNonNull(schemaRegistryClient));
        }

        @Override
        String[] getColumnNames(final Table t, SchemaRegistryClient schemaRegistryClient) {
            ensureSchema(t, schemaRegistryClient);
            final List<Field> fields = schema.getFields();
            // ensure we got timestampFieldName right
            if (timestampFieldName != null) {
                boolean found = false;
                for (final Field field : fields) {
                    final String fieldName = field.name();
                    if (fieldName.equals(timestampFieldName)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new IllegalArgumentException(
                            "timestampFieldName=" + timestampFieldName +
                                    " is not a field name in the provided schema.");
                }
            }
            final int timestampFieldCount = ((timestampFieldName != null) ? 1 : 0);
            final List<String> columnNames = new ArrayList<>();
            for (final Field field : fields) {
                final String fieldName = field.name();
                if (timestampFieldName != null && fieldName.equals(timestampFieldName)) {
                    continue;
                }
                final String candidateColumnName;
                if (fieldToColumnMapping == null) {
                    candidateColumnName = fieldName;
                } else {
                    candidateColumnName = fieldToColumnMapping.getOrDefault(fieldName, fieldName);
                }
                if (excludeColumns != null && excludeColumns.test(candidateColumnName)) {
                    continue;
                }
                if (includeOnlyColumns != null && !includeOnlyColumns.test(candidateColumnName)) {
                    continue;
                }
                columnNames.add(candidateColumnName);
            }
            return columnNames.toArray(new String[columnNames.size()]);
        }

        @Override
        KeyOrValueSerializer<?> keyOrValueSerializer(@NotNull Table t, @NotNull String[] columnNames) {
            return new GenericRecordKeyOrValueSerializer(
                    t, schema, columnNames, timestampFieldName, columnProperties.getValue());
        }

        void ensureSchema(final Table t, SchemaRegistryClient schemaRegistryClient) {
            if (schema != null) {
                return;
            }
            if (publishSchema) {
                schema = KafkaTools.columnDefinitionsToAvroSchema(t,
                        schemaName, schemaNamespace, columnProperties.getValue(), includeOnlyColumns,
                        excludeColumns, columnProperties);
                try {
                    schemaRegistryClient.register(schemaName, new AvroSchema(schema));
                } catch (RestClientException | IOException e) {
                    throw new UncheckedDeephavenException(e);
                }
            } else {
                schema = getAvroSchema(schemaRegistryClient, schemaName, schemaVersion);
            }
        }
    }

    static Schema getAvroSchema(SchemaRegistryClient schemaClient, final String schemaName,
            final String schemaVersion) {
        try {
            final SchemaMetadata schemaMetadata;
            if (AVRO_LATEST_VERSION.equals(schemaVersion)) {
                schemaMetadata = schemaClient.getLatestSchemaMetadata(schemaName);
            } else {
                schemaMetadata = schemaClient.getSchemaMetadata(schemaName, Integer.parseInt(schemaVersion));
            }
            return (Schema) schemaClient.getSchemaById(schemaMetadata.getId()).rawSchema();
        } catch (RestClientException | IOException e) {
            throw new UncheckedDeephavenException(e);
        }
    }
}
