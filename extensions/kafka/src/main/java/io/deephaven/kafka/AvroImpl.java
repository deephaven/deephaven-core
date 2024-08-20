//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
import io.deephaven.engine.util.BigDecimalUtils;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.KafkaTools.Produce;
import io.deephaven.kafka.ingest.GenericRecordChunkAdapter;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.publish.GenericRecordKeyOrValueSerializer;
import io.deephaven.kafka.publish.KeyOrValueSerializer;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.vector.ByteVector;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
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

class AvroImpl {

    private static final Type<Utf8> utf8Type = Type.find(Utf8.class);

    static final class AvroConsume extends Consume.KeyOrValueSpec {
        private static final Pattern NESTED_FIELD_NAME_SEPARATOR_PATTERN =
                Pattern.compile(Pattern.quote(NESTED_FIELD_NAME_SEPARATOR));

        private Schema schema;
        private final String schemaName;
        private final String schemaVersion;
        /** fields mapped to null are skipped. */
        private final Function<String, String> fieldPathToColumnName;

        private final boolean useUTF8Strings;

        AvroConsume(final Schema schema, final Function<String, String> fieldPathToColumnName) {
            this.schema = Objects.requireNonNull(schema);
            this.schemaName = null;
            this.schemaVersion = null;
            this.fieldPathToColumnName = fieldPathToColumnName;
            this.useUTF8Strings = false;
        }

        AvroConsume(final String schemaName,
                final String schemaVersion,
                final Function<String, String> fieldPathToColumnName) {
            this(schemaName, schemaVersion, fieldPathToColumnName, false);
        }

        AvroConsume(final String schemaName,
                final String schemaVersion,
                final Function<String, String> fieldPathToColumnName,
                final boolean useUTF8Strings) {
            this.schema = null;
            this.schemaName = schemaName;
            this.schemaVersion = schemaVersion;
            this.fieldPathToColumnName = fieldPathToColumnName;
            this.useUTF8Strings = useUTF8Strings;
        }

        @Override
        public Optional<SchemaProvider> getSchemaProvider() {
            return Optional.of(new AvroSchemaProvider());
        }

        @Override
        protected Deserializer<?> getDeserializer(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs) {
            ensureSchema(schemaRegistryClient);
            return new KafkaAvroDeserializerWithReaderSchema(schemaRegistryClient);
        }

        @Override
        protected KeyOrValueIngestData getIngestData(KeyOrValue keyOrValue,
                SchemaRegistryClient schemaRegistryClient, Map<String, ?> configs, MutableInt nextColumnIndexMut,
                List<ColumnDefinition<?>> columnDefinitionsOut) {
            ensureSchema(schemaRegistryClient);
            KeyOrValueIngestData data = new KeyOrValueIngestData();
            data.fieldPathToColumnName = new HashMap<>();
            avroSchemaToColumnDefinitions(columnDefinitionsOut, data.fieldPathToColumnName, schema,
                    fieldPathToColumnName, useUTF8Strings);
            data.extra = schema;
            return data;
        }

        @Override
        protected KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
            return GenericRecordChunkAdapter.make(
                    tableDef,
                    ci -> StreamChunkUtils.chunkTypeForColumnIndex(tableDef, ci),
                    data.fieldPathToColumnName,
                    NESTED_FIELD_NAME_SEPARATOR_PATTERN,
                    (Schema) data.extra,
                    true);
        }

        private void ensureSchema(SchemaRegistryClient schemaRegistryClient) {
            // This adds a little bit of stateful-ness to AvroConsume. Typically, this is not something we want /
            // encourage for implementations, but due to the getDeserializer / getProcessor dependency on the exact
            // same schema, we need to ensure we don't race if the user has set AVRO_LATEST_VERSION. Alternatively, we
            // could break the KeyOrValueSpec API and pass Deserializer as parameter to getIngestData.
            if (schema != null) {
                return;
            }
            schema = Objects.requireNonNull(getAvroSchema(schemaRegistryClient, schemaName, schemaVersion));
        }

        /**
         * Our getProcessor relies on a specific {@link Schema}; we need to ensure that Kafka layer adapts the on-wire
         * writer's schema to our reader's schema.
         */
        class KafkaAvroDeserializerWithReaderSchema extends KafkaAvroDeserializer {
            public KafkaAvroDeserializerWithReaderSchema(SchemaRegistryClient client) {
                super(client);
            }

            @Override
            public java.lang.Object deserialize(String topic, byte[] bytes) {
                return super.deserialize(topic, bytes, schema);
            }

            @Override
            public java.lang.Object deserialize(String topic, Headers headers, byte[] bytes) {
                return super.deserialize(topic, headers, bytes, schema);
            }
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
        public Optional<SchemaProvider> getSchemaProvider() {
            return Optional.of(new AvroSchemaProvider());
        }

        @Override
        Serializer<?> getSerializer(SchemaRegistryClient schemaRegistryClient, TableDefinition definition) {
            return new KafkaAvroSerializer(Objects.requireNonNull(schemaRegistryClient));
        }

        @Override
        String[] getColumnNames(@NotNull final Table t, SchemaRegistryClient schemaRegistryClient) {
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
                if (fieldName.equals(timestampFieldName)) {
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
        KeyOrValueSerializer<?> getKeyOrValueSerializer(@NotNull Table t, @NotNull String[] columnNames) {
            return new GenericRecordKeyOrValueSerializer(
                    t, schema, columnNames, timestampFieldName, columnProperties.getValue());
        }

        void ensureSchema(final Table t, SchemaRegistryClient schemaRegistryClient) {
            if (schema != null) {
                return;
            }
            if (publishSchema) {
                schema = columnDefinitionsToAvroSchema(t,
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

    static Schema columnDefinitionsToAvroSchema(Table t, String schemaName, String namespace, Properties colProps,
            Predicate<String> includeOnly, Predicate<String> exclude, MutableObject<Properties> colPropsOut) {
        SchemaBuilder.FieldAssembler<Schema> fass = SchemaBuilder.record(schemaName).namespace(namespace).fields();
        final List<ColumnDefinition<?>> colDefs = t.getDefinition().getColumns();
        colPropsOut.setValue(colProps);
        for (final ColumnDefinition<?> colDef : colDefs) {
            if (includeOnly != null && !includeOnly.test(colDef.getName())) {
                continue;
            }
            if (exclude != null && exclude.test(colDef.getName())) {
                continue;
            }
            fass = addFieldForColDef(t, fass, colDef, colPropsOut);
        }
        return fass.endRecord();
    }

    private static SchemaBuilder.FieldAssembler<Schema> addFieldForColDef(
            final Table t,
            final SchemaBuilder.FieldAssembler<Schema> fassIn,
            final ColumnDefinition<?> colDef,
            final MutableObject<Properties> colPropsMu) {
        final String logicalTypeName = "logicalType";
        final String dhTypeAttribute = "dhType";
        SchemaBuilder.FieldAssembler<Schema> fass = fassIn;
        final Class<?> type = colDef.getDataType();
        final String colName = colDef.getName();
        final SchemaBuilder.BaseFieldTypeBuilder<Schema> base = fass.name(colName).type().nullable();
        if (type == byte.class || type == char.class || type == short.class) {
            fass = base.intBuilder().prop(dhTypeAttribute, type.getName()).endInt().noDefault();
        } else if (type == int.class) {
            fass = base.intType().noDefault();
        } else if (type == long.class) {
            fass = base.longType().noDefault();
        } else if (type == float.class) {
            fass = base.floatType().noDefault();
        } else if (type == double.class) {
            fass = base.doubleType().noDefault();
        } else if (type == String.class) {
            fass = base.stringType().noDefault();
        } else if (type == Instant.class) {
            fass = base.longBuilder().prop(logicalTypeName, "timestamp-micros").endLong().noDefault();
        } else if (type == BigDecimal.class) {
            final BigDecimalUtils.PropertyNames propertyNames =
                    new BigDecimalUtils.PropertyNames(colName);
            BigDecimalUtils.PrecisionAndScale values =
                    BigDecimalUtils.getPrecisionAndScaleFromColumnProperties(propertyNames, colPropsMu.getValue(),
                            true);
            if (t.isRefreshing()) {
                validatePrecisionAndScaleForRefreshingTable(propertyNames, values);
            } else { // non refreshing table
                ensurePrecisionAndScaleForStaticTable(colPropsMu, t, propertyNames, values);
            }
            fass = base.bytesBuilder()
                    .prop(logicalTypeName, "decimal")
                    .prop("precision", values.precision)
                    .prop("scale", values.scale)
                    .endBytes()
                    .noDefault();
        } else {
            fass = base.bytesBuilder().prop(dhTypeAttribute, type.getName()).endBytes().noDefault();
        }
        return fass;
    }

    private static void validatePrecisionAndScaleForRefreshingTable(
            final BigDecimalUtils.PropertyNames names,
            final BigDecimalUtils.PrecisionAndScale values) {
        final String exBaseMsg = "Column " + names.columnName + " of type " + BigDecimal.class.getSimpleName() +
                " in a refreshing table implies both properties '" +
                names.precisionProperty + "' and '" + names.scaleProperty
                + "' should be defined; ";

        if (values.precision == BigDecimalUtils.INVALID_PRECISION_OR_SCALE
                && values.scale == BigDecimalUtils.INVALID_PRECISION_OR_SCALE) {
            throw new IllegalArgumentException(exBaseMsg + " missing both");
        }
        if (values.precision == BigDecimalUtils.INVALID_PRECISION_OR_SCALE) {
            throw new IllegalArgumentException(
                    exBaseMsg + " missing '" + names.precisionProperty + "'");
        }
        if (values.scale == BigDecimalUtils.INVALID_PRECISION_OR_SCALE) {
            throw new IllegalArgumentException(exBaseMsg + " missing '" + names.scaleProperty + "'");
        }
    }

    private static BigDecimalUtils.PrecisionAndScale ensurePrecisionAndScaleForStaticTable(
            final MutableObject<Properties> colPropsMu,
            final Table t,
            final BigDecimalUtils.PropertyNames names,
            final BigDecimalUtils.PrecisionAndScale valuesIn) {
        if (valuesIn.precision != BigDecimalUtils.INVALID_PRECISION_OR_SCALE
                && valuesIn.scale != BigDecimalUtils.INVALID_PRECISION_OR_SCALE) {
            return valuesIn;
        }
        final String exBaseMsg = "Column " + names.columnName + " of type " + BigDecimal.class.getSimpleName() +
                " in a non refreshing table implies either both properties '" +
                names.precisionProperty + "' and '" + names.scaleProperty
                + "' should be defined, or none of them;";
        if (valuesIn.precision != BigDecimalUtils.INVALID_PRECISION_OR_SCALE) {
            throw new IllegalArgumentException(
                    exBaseMsg + " only '" + names.precisionProperty + "' is defined, missing '"
                            + names.scaleProperty + "'");
        }
        if (valuesIn.scale != BigDecimalUtils.INVALID_PRECISION_OR_SCALE) {
            throw new IllegalArgumentException(
                    exBaseMsg + " only '" + names.scaleProperty + "' is defined, missing '"
                            + names.precisionProperty + "'");
        }
        // Both precision and scale are null; compute them ourselves.
        final BigDecimalUtils.PrecisionAndScale newValues =
                BigDecimalUtils.computePrecisionAndScale(t, names.columnName);
        final Properties toSet;
        final Properties colProps = colPropsMu.getValue();
        if (colProps == null) {
            toSet = new Properties();
            colPropsMu.setValue(toSet);
        } else {
            toSet = colProps;
        }
        BigDecimalUtils.setProperties(toSet, names, newValues);
        return newValues;
    }

    static void avroSchemaToColumnDefinitions(List<ColumnDefinition<?>> columnsOut,
            Map<String, String> fieldPathToColumnNameOut, Schema schema,
            Function<String, String> requestedFieldPathToColumnName,
            final boolean useUTF8Strings) {
        if (schema.isUnion()) {
            throw new UnsupportedOperationException("Schemas defined as a union of records are not supported");
        }
        final Schema.Type type = schema.getType();
        if (type != Schema.Type.RECORD) {
            throw new IllegalArgumentException("The schema is not a toplevel record definition.");
        }
        final List<Field> fields = schema.getFields();
        for (final Field field : fields) {
            pushColumnTypesFromAvroField(columnsOut, fieldPathToColumnNameOut, "", field,
                    requestedFieldPathToColumnName, useUTF8Strings);
        }
    }

    private static void pushColumnTypesFromAvroField(
            final List<ColumnDefinition<?>> columnsOut,
            final Map<String, String> fieldPathToColumnNameOut,
            final String fieldNamePrefix,
            final Field field,
            final Function<String, String> fieldPathToColumnName,
            final boolean useUTF8Strings) {
        final Schema fieldSchema = field.schema();
        final String fieldName = field.name();
        final String mappedNameForColumn = fieldPathToColumnName.apply(fieldNamePrefix + fieldName);
        if (mappedNameForColumn == null) {
            // allow the user to specify fields to skip by providing a mapping to null.
            return;
        }
        final Schema.Type fieldType = fieldSchema.getType();
        pushColumnTypesFromAvroField(
                columnsOut, fieldPathToColumnNameOut,
                fieldNamePrefix, fieldName,
                fieldSchema, mappedNameForColumn, fieldType, fieldPathToColumnName, useUTF8Strings);
    }

    private static void pushColumnTypesFromAvroField(
            final List<ColumnDefinition<?>> columnsOut,
            final Map<String, String> fieldPathToColumnNameOut,
            final String fieldNamePrefix,
            final String fieldName,
            final Schema fieldSchema,
            final String mappedNameForColumn,
            final Schema.Type fieldType,
            final Function<String, String> fieldPathToColumnName,
            final boolean useUTF8Strings) {
        switch (fieldType) {
            case BOOLEAN:
                columnsOut.add(ColumnDefinition.ofBoolean(mappedNameForColumn));
                break;
            // There is no "SHORT" in Avro.
            case INT:
                columnsOut.add(ColumnDefinition.ofInt(mappedNameForColumn));
                break;
            case LONG: {
                final LogicalType logicalType = getEffectiveLogicalType(fieldName, fieldSchema);
                if (LogicalTypes.timestampMicros().equals(logicalType) ||
                        LogicalTypes.timestampMillis().equals(logicalType)) {
                    columnsOut.add(ColumnDefinition.ofTime(mappedNameForColumn));
                } else {
                    columnsOut.add(ColumnDefinition.ofLong(mappedNameForColumn));
                }
                break;
            }
            case FLOAT:
                columnsOut.add(ColumnDefinition.ofFloat(mappedNameForColumn));
                break;
            case DOUBLE:
                columnsOut.add(ColumnDefinition.ofDouble(mappedNameForColumn));
                break;
            case ENUM:
            case STRING:
                if (useUTF8Strings) {
                    columnsOut.add(ColumnDefinition.of(mappedNameForColumn, utf8Type));
                } else {
                    columnsOut.add(ColumnDefinition.ofString(mappedNameForColumn));
                }
                break;
            case UNION: {
                final Schema effectiveSchema = KafkaSchemaUtils.getEffectiveSchema(fieldName, fieldSchema);
                if (effectiveSchema == fieldSchema) {
                    // It is an honest to god Union; we don't support them right now other than giving back
                    // an Object column with a GenericRecord object.
                    columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, GenericRecord.class));
                    break;
                }
                // It was a union with null, which is simply the other unioned type in DH.
                pushColumnTypesFromAvroField(
                        columnsOut, fieldPathToColumnNameOut,
                        fieldNamePrefix, fieldName,
                        effectiveSchema, mappedNameForColumn, effectiveSchema.getType(), fieldPathToColumnName,
                        useUTF8Strings);
                return;
            }
            case RECORD:
                // Linearize any nesting.
                for (final Field nestedField : fieldSchema.getFields()) {
                    pushColumnTypesFromAvroField(
                            columnsOut, fieldPathToColumnNameOut,
                            fieldNamePrefix + fieldName + NESTED_FIELD_NAME_SEPARATOR, nestedField,
                            fieldPathToColumnName, useUTF8Strings);
                }
                return;
            case BYTES:
            case FIXED: {
                final LogicalType logicalType = getEffectiveLogicalType(fieldName, fieldSchema);
                if (logicalType instanceof LogicalTypes.Decimal) {
                    columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, BigDecimal.class));
                    break;
                }
                columnsOut.add(ColumnDefinition.ofVector(mappedNameForColumn, ByteVector.class));
                break;
            }
            case ARRAY: {
                Schema elementTypeSchema = fieldSchema.getElementType();
                Schema.Type elementTypeType = elementTypeSchema.getType();
                if (elementTypeType.equals(Schema.Type.UNION)) {
                    elementTypeSchema = KafkaSchemaUtils.getEffectiveSchema(fieldName, elementTypeSchema);
                    elementTypeType = elementTypeSchema.getType();
                }
                switch (elementTypeType) {
                    case INT:
                        columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, int[].class));
                        break;
                    case LONG:
                        final LogicalType logicalType = getEffectiveLogicalType(fieldName, elementTypeSchema);
                        if (LogicalTypes.timestampMicros().equals(logicalType) ||
                                LogicalTypes.timestampMillis().equals(logicalType)) {
                            columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, Instant[].class));
                        } else {
                            columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, long[].class));
                        }
                        break;
                    case FLOAT:
                        columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, float[].class));
                        break;
                    case DOUBLE:
                        columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, double[].class));
                        break;
                    case BOOLEAN:
                        columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, Boolean[].class));
                        break;
                    case ENUM:
                    case STRING:
                        columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, String[].class));
                        break;
                    default:
                        columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, Object[].class));
                        break;
                }
                break;
            }
            case MAP:
                columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, Map.class));
                break;
            case NULL:
            default:
                columnsOut.add(ColumnDefinition.fromGenericType(mappedNameForColumn, GenericContainer.class));
                break;
        }
        if (fieldPathToColumnNameOut != null) {
            fieldPathToColumnNameOut.put(fieldNamePrefix + fieldName, mappedNameForColumn);
        }
    }

    private static LogicalType getEffectiveLogicalType(final String fieldName, final Schema fieldSchema) {
        final Schema effectiveSchema = KafkaSchemaUtils.getEffectiveSchema(fieldName, fieldSchema);
        return effectiveSchema.getLogicalType();
    }
}
