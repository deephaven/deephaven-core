/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.table.Table;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.DataFormat;
import io.deephaven.kafka.KafkaTools.Produce.KeyOrValueSpec;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang3.mutable.MutableObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.deephaven.kafka.KafkaTools.createSchemaRegistryClient;

class AvroImpl {

    /**
     * Avro spec.
     */
    static final class AvroConsume extends Consume.KeyOrValueSpec {
        final Schema schema;
        final String schemaName;
        final String schemaVersion;
        /** fields mapped to null are skipped. */
        final Function<String, String> fieldPathToColumnName;

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
        DataFormat dataFormat() {
            return DataFormat.AVRO;
        }
    }

    /**
     * Avro spec.
     */
    static final class AvroProduce extends KeyOrValueSpec {
        Schema schema;
        final String schemaName;
        final String schemaVersion;
        final Map<String, String> fieldToColumnMapping;
        final String timestampFieldName;
        final Predicate<String> includeOnlyColumns;
        final Predicate<String> excludeColumns;
        final boolean publishSchema;
        final String schemaNamespace;
        final MutableObject<Properties> columnProperties;

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
                if (schemaVersion != null && !KafkaTools.AVRO_LATEST_VERSION.equals(schemaVersion)) {
                    throw new IllegalArgumentException(
                            String.format("schemaVersion must be null or \"%s\" when publishSchema=true",
                                    KafkaTools.AVRO_LATEST_VERSION));
                }
            }
        }

        @Override
        DataFormat dataFormat() {
            return DataFormat.AVRO;
        }

        void ensureSchema(final Table t, final Properties kafkaProperties) {
            if (schema != null) {
                return;
            }
            if (publishSchema) {
                schema = KafkaTools.columnDefinitionsToAvroSchema(t,
                        schemaName, schemaNamespace, columnProperties.getValue(), includeOnlyColumns,
                        excludeColumns, columnProperties);
                try {
                    putAvroSchema(kafkaProperties, schemaName, schema);
                } catch (RestClientException | IOException e) {
                    throw new UncheckedDeephavenException(e);
                }
            } else {
                schema = KafkaTools.getAvroSchema(kafkaProperties, schemaName, schemaVersion);
            }
        }

        String[] getColumnNames(final Table t, final Properties kafkaProperties) {
            ensureSchema(t, kafkaProperties);
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
    }

    private static int putAvroSchema(Properties kafkaProperties, String schemaName, Schema schema)
            throws RestClientException, IOException {
        final SchemaRegistryClient registryClient = createSchemaRegistryClient(kafkaProperties);
        return registryClient.register(schemaName, new AvroSchema(schema));
    }
}
