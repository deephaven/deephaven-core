/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import gnu.trove.map.hash.TIntLongHashMap;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.time.DateTime;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.LocalTableMap;
import io.deephaven.engine.table.impl.StreamTableTools;
import io.deephaven.engine.table.TableMap;
import io.deephaven.engine.table.TransformableTableMap;
import io.deephaven.engine.util.BigDecimalUtils;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.kafka.ingest.*;
import io.deephaven.kafka.publish.*;
import io.deephaven.stream.StreamToTableAdapter;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.codec.Charsets;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.http.*;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    public static final String NESTED_FIELD_NAME_SEPARATOR = "__";
    private static final Pattern NESTED_FIELD_NAME_SEPARATOR_PATTERN =
            Pattern.compile(Pattern.quote(NESTED_FIELD_NAME_SEPARATOR));
    public static final String AVRO_LATEST_VERSION = "latest";

    private static final Logger log = LoggerFactory.getLogger(KafkaTools.class);

    private static final int CHUNK_SIZE = 2048;

    private static String extractSchemaServerResponse(final HttpResponse response) throws IOException {
        final HttpEntity entity = response.getEntity();
        final Header encodingHeader = entity.getContentEncoding();
        final Charset encoding = encodingHeader == null
                ? StandardCharsets.UTF_8
                : Charsets.toCharset(encodingHeader.getValue());
        return EntityUtils.toString(entity, encoding);
    }

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
            final String json = extractSchemaServerResponse(response);
            action = "parse schema server response: " + json;
            return new Schema.Parser().parse(json);
        } catch (Exception e) {
            throw new UncheckedDeephavenException("Exception while trying to " + action, e);
        }
    }

    /**
     * Push an Avro schema from a Confluent compatible Schema Server.
     *
     * @param schema An Avro schema
     * @param schemaServerUrl The schema server URL
     * @param resourceName The resource name that the schema will be known as in the schema server
     * @return The version for the added resource as returned by schema server.
     */
    public static String putAvroSchema(
            final Schema schema, final String schemaServerUrl, final String resourceName) {
        String action = "setup http client";
        try (final CloseableHttpClient client = HttpClients.custom().build()) {
            final String requestStr =
                    schemaServerUrl + "/subjects/" + resourceName + "/versions/";
            final ObjectMapper mapper = new ObjectMapper();
            final ObjectNode root = mapper.createObjectNode();
            root.put("schema", schema.toString());
            final String jsonRequest = mapper.writeValueAsString(root);
            final HttpUriRequest request = RequestBuilder.post()
                    .setHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .setEntity(new StringEntity(jsonRequest))
                    .setUri(requestStr)
                    .build();
            action = "execute schema request " + requestStr;
            final HttpResponse response = client.execute(request);
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED) {
                throw new UncheckedDeephavenException(
                        "Got response status code " + statusCode + " for request '" + jsonRequest + "': " +
                                extractSchemaServerResponse(response));
            }
            action = "extract json server response";
            final String jsonStr = extractSchemaServerResponse(response);
            action = "parse json server response: '" + jsonStr + "' for request '" + jsonRequest + "'";
            final JsonNode jnode = mapper.readTree(jsonStr).get("version");
            if (jnode == null) {
                return null;
            }
            return jnode.asText();
        } catch (Exception e) {
            throw new UncheckedDeephavenException("Exception while trying to " + action, e);
        }
    }

    public static Schema columnDefinitionsToAvroSchema(
            final Table t,
            final String schemaName,
            final String namespace,
            final Properties colProps,
            final Predicate<String> includeOnly,
            final Predicate<String> exclude,
            final MutableObject<Properties> colPropsOut) {
        SchemaBuilder.FieldAssembler<Schema> fass = SchemaBuilder.record(schemaName).namespace(namespace).fields();
        final ColumnDefinition<?>[] colDefs = t.getDefinition().getColumns();
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
        } else if (type == DateTime.class) {
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
            final Map<String, String> fieldPathToColumnNameOut,
            final String prefix,
            final Schema.Field field,
            final Function<String, String> fieldPathToColumnName) {
        final Schema fieldSchema = field.schema();
        final String fieldName = field.name();
        final String mappedName = fieldPathToColumnName.apply(prefix + fieldName);
        if (mappedName == null) {
            // allow the user to specify fields to skip by providing a mapping to null.
            return;
        }
        final Schema.Type fieldType = fieldSchema.getType();
        pushColumnTypesFromAvroField(
                columnsOut, fieldPathToColumnNameOut,
                prefix, fieldName,
                fieldSchema, mappedName, fieldType, fieldPathToColumnName);
    }

    private static LogicalType getEffectiveLogicalType(final String fieldName, final Schema fieldSchema) {
        final Schema effectiveSchema = KafkaSchemaUtils.getEffectiveSchema(fieldName, fieldSchema);
        return effectiveSchema.getLogicalType();
    }

    private static void pushColumnTypesFromAvroField(
            final List<ColumnDefinition<?>> columnsOut,
            final Map<String, String> fieldPathToColumnNameOut,
            final String prefix,
            final String fieldName,
            final Schema fieldSchema,
            final String mappedName,
            final Schema.Type fieldType,
            final Function<String, String> fieldPathToColumnName) {
        switch (fieldType) {
            case BOOLEAN:
                columnsOut.add(ColumnDefinition.ofBoolean(mappedName));
                break;
            case INT:
                columnsOut.add(ColumnDefinition.ofInt(mappedName));
                break;
            case LONG: {
                final LogicalType logicalType = getEffectiveLogicalType(fieldName, fieldSchema);
                if (LogicalTypes.timestampMicros().equals(logicalType) ||
                        LogicalTypes.timestampMillis().equals(logicalType)) {
                    columnsOut.add(ColumnDefinition.ofTime(mappedName));
                } else {
                    columnsOut.add(ColumnDefinition.ofLong(mappedName));
                }
                break;
            }
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
            case UNION: {
                final Schema effectiveSchema = KafkaSchemaUtils.getEffectiveSchema(fieldName, fieldSchema);
                pushColumnTypesFromAvroField(
                        columnsOut, fieldPathToColumnNameOut,
                        prefix, fieldName,
                        effectiveSchema, mappedName, effectiveSchema.getType(), fieldPathToColumnName);
                return;
            }
            case RECORD:
                // Linearize any nesting.
                for (final Schema.Field nestedField : fieldSchema.getFields()) {
                    pushColumnTypesFromAvroField(
                            columnsOut, fieldPathToColumnNameOut,
                            prefix + fieldName + NESTED_FIELD_NAME_SEPARATOR, nestedField,
                            fieldPathToColumnName);
                }
                return;
            case BYTES:
            case FIXED: {
                final LogicalType logicalType = getEffectiveLogicalType(fieldName, fieldSchema);
                if (logicalType instanceof LogicalTypes.Decimal) {
                    columnsOut.add(ColumnDefinition.fromGenericType(mappedName, BigDecimal.class));
                    break;
                }
                // fallthrough
            }
            case MAP:
            case NULL:
            case ARRAY:
            default:
                throw new UnsupportedOperationException("Type " + fieldType + " not supported for field " + fieldName);
        }
        if (fieldPathToColumnNameOut != null) {
            fieldPathToColumnNameOut.put(prefix + fieldName, mappedName);
        }
    }

    public static void avroSchemaToColumnDefinitions(
            final List<ColumnDefinition<?>> columnsOut,
            final Map<String, String> fieldPathToColumnNameOut,
            final Schema schema,
            final Function<String, String> requestedFieldPathToColumnName) {
        if (schema.isUnion()) {
            throw new UnsupportedOperationException("Schemas defined as a union of records are not supported");
        }
        final Schema.Type type = schema.getType();
        if (type != Schema.Type.RECORD) {
            throw new IllegalArgumentException("The schema is not a toplevel record definition.");
        }
        final List<Schema.Field> fields = schema.getFields();
        for (final Schema.Field field : fields) {
            pushColumnTypesFromAvroField(columnsOut, fieldPathToColumnNameOut, "", field,
                    requestedFieldPathToColumnName);
        }
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

    /**
     * Enum to specify the expected processing (format) for Kafka KEY or VALUE fields.
     */
    public enum DataFormat {
        IGNORE, SIMPLE, AVRO, JSON
    }

    public static class Consume {

        /**
         * Class to specify conversion of Kafka KEY or VALUE fields to table columns.
         */
        static abstract class KeyOrValueSpec {
            /**
             * Data format for this Spec.
             *
             * @return Data format for this Spec
             */
            abstract DataFormat dataFormat();

            static final class Ignore extends KeyOrValueSpec {
                @Override
                DataFormat dataFormat() {
                    return DataFormat.IGNORE;
                }
            }

            /**
             * Avro spec.
             */
            static final class Avro extends KeyOrValueSpec {
                final Schema schema;
                final String schemaName;
                final String schemaVersion;
                /** fields mapped to null are skipped. */
                final Function<String, String> fieldPathToColumnName;

                private Avro(final Schema schema, final Function<String, String> fieldPathToColumnName) {
                    this.schema = schema;
                    this.schemaName = null;
                    this.schemaVersion = null;
                    this.fieldPathToColumnName = fieldPathToColumnName;
                }

                private Avro(final String schemaName,
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
             * Single spec for unidimensional (basic Kafka encoded for one type) fields.
             */
            static final class Simple extends KeyOrValueSpec {
                final String columnName;
                final Class<?> dataType;

                private Simple(final String columnName, final Class<?> dataType) {
                    this.columnName = columnName;
                    this.dataType = dataType;
                }

                @Override
                DataFormat dataFormat() {
                    return DataFormat.SIMPLE;
                }
            }

            /**
             * The names for the key or value columns can be provided in the properties as "key.column.name" or
             * "value.column.name", and otherwise default to "key" or "value". The types for key or value are either
             * specified in the properties as "key.type" or "value.type", or deduced from the serializer classes for key
             * or value in the provided Properties object.
             */
            private static final Simple FROM_PROPERTIES = new Simple(null, null);

            /**
             * JSON spec.
             */
            static final class Json extends KeyOrValueSpec {
                final ColumnDefinition<?>[] columnDefinitions;
                final Map<String, String> fieldNameToColumnName;

                private Json(
                        final ColumnDefinition<?>[] columnDefinitions,
                        final Map<String, String> fieldNameToColumnName) {
                    this.columnDefinitions = columnDefinitions;
                    this.fieldNameToColumnName = fieldNameToColumnName;
                }

                @Override
                DataFormat dataFormat() {
                    return DataFormat.JSON;
                }
            }
        }

        public static final KeyOrValueSpec.Ignore IGNORE = new KeyOrValueSpec.Ignore();

        /**
         * Spec to explicitly ask
         * {@link #consumeToTable(Properties, String, IntPredicate, IntToLongFunction, KeyOrValueSpec, KeyOrValueSpec, TableType)
         * consumeToTable} to ignore either key or value.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec ignoreSpec() {
            return IGNORE;
        }

        /**
         * A JSON spec from a set of column definitions.
         *
         * @param columnDefinitions An array of column definitions for specifying the table to be created.
         * @param fieldNameToColumnName A mapping from JSON field names to column names provided in the definition.
         *        Fields not included will be ignored.
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
         * @param columnDefinitions An array of column definitions for specifying the table to be created. The column
         *        names should map one to JSON fields expected; is not necessary to include all fields from the expected
         *        JSON, any fields not included would be ignored.
         *
         * @return A JSON spec for the given inputs.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec jsonSpec(final ColumnDefinition<?>[] columnDefinitions) {
            return jsonSpec(columnDefinitions, null);
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
            return new KeyOrValueSpec.Avro(schema, fieldNameToColumnName);
        }

        /**
         * Avro spec from an Avro schema. All fields in the schema are mapped to columns of the same name.
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
            return new KeyOrValueSpec.Avro(schemaName, schemaVersion, fieldNameToColumnName);
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
            return new KeyOrValueSpec.Avro(schemaName, AVRO_LATEST_VERSION, fieldNameToColumnName);
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
            return new KeyOrValueSpec.Avro(schemaName, schemaVersion, Function.identity());
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
            return new KeyOrValueSpec.Avro(schemaName, AVRO_LATEST_VERSION, Function.identity());
        }

        @SuppressWarnings("unused")
        public static KeyOrValueSpec simpleSpec(final String columnName, final Class<?> dataType) {
            return new KeyOrValueSpec.Simple(columnName, dataType);
        }

        /**
         * The types for key or value are either specified in the properties as "key.type" or "value.type", or deduced
         * from the serializer classes for key or value in the provided Properties object.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec simpleSpec(final String columnName) {
            return new KeyOrValueSpec.Simple(columnName, null);
        }
    }

    public static class Produce {
        /**
         * Class to specify conversion of table columns to Kafka KEY or VALUE fields.
         */
        static abstract class KeyOrValueSpec {
            /**
             * Data format for this Spec.
             *
             * @return Data format for this Spec
             */
            abstract DataFormat dataFormat();

            static final class Ignore extends KeyOrValueSpec {
                @Override
                DataFormat dataFormat() {
                    return DataFormat.IGNORE;
                }
            }

            /**
             * Single spec for unidimensional (basic Kafka encoded for one type) fields.
             */
            static final class Simple extends KeyOrValueSpec {
                final String columnName;

                Simple(final String columnName) {
                    this.columnName = columnName;
                }

                @Override
                DataFormat dataFormat() {
                    return DataFormat.SIMPLE;
                }
            }

            /**
             * Avro spec.
             */
            static final class Avro extends KeyOrValueSpec {
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

                Avro(final Schema schema,
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
                }

                @Override
                DataFormat dataFormat() {
                    return DataFormat.AVRO;
                }

                void ensureSchema(final Table t, final Properties kafkaProperties) {
                    if (schema != null) {
                        return;
                    }
                    final String schemaServiceUrl = ensureAndGetSchemaServerProprety(kafkaProperties);
                    if (publishSchema) {
                        schema = columnDefinitionsToAvroSchema(t,
                                schemaName, schemaNamespace, columnProperties.getValue(), includeOnlyColumns,
                                excludeColumns, columnProperties);
                        final String putVersion = putAvroSchema(schema, schemaServiceUrl, schemaName);
                        if (putVersion != null && schemaVersion != null && !schemaVersion.equals(putVersion)) {
                            throw new IllegalStateException("Specified expected version " + schemaVersion
                                    + " mismatch: schema server put for schema name " +
                                    schemaName + " resulted in version " + putVersion);
                        }
                    } else {
                        schema = getAvroSchema(schemaServiceUrl, schemaName, schemaVersion);
                    }
                }

                private static String ensureAndGetSchemaServerProprety(final Properties kafkaProperties) {
                    if (!kafkaProperties.containsKey(SCHEMA_SERVER_PROPERTY)) {
                        throw new IllegalArgumentException(
                                "Avro schema name specified but schema server url property " +
                                        SCHEMA_SERVER_PROPERTY + " not found.");
                    }
                    return kafkaProperties.getProperty(SCHEMA_SERVER_PROPERTY);
                }

                String[] getColumnNames(final Table t, final Properties kafkaProperties) {
                    ensureSchema(t, kafkaProperties);
                    final List<Schema.Field> fields = schema.getFields();
                    // ensure we got timestampFieldName right
                    if (timestampFieldName != null) {
                        boolean found = false;
                        for (final Schema.Field field : fields) {
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
                    for (final Schema.Field field : fields) {
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

            /**
             * JSON spec.
             */
            static final class Json extends KeyOrValueSpec {
                final String[] includeColumns;
                final Predicate<String> excludeColumns;
                final Map<String, String> columnNameToFieldName;
                final String nestedObjectDelimiter;
                final boolean outputNulls;
                final String timestampFieldName;

                Json(final String[] includeColumns,
                        final Predicate<String> excludeColumns,
                        final Map<String, String> columnNameToFieldName,
                        final String nestedObjectDelimiter,
                        final boolean outputNulls,
                        final String timestampFieldName) {
                    this.includeColumns = includeColumns;
                    this.excludeColumns = excludeColumns;
                    this.columnNameToFieldName = columnNameToFieldName;
                    this.nestedObjectDelimiter = nestedObjectDelimiter;
                    this.outputNulls = outputNulls;
                    this.timestampFieldName = timestampFieldName;
                }

                @Override
                DataFormat dataFormat() {
                    return DataFormat.JSON;
                }

                String[] getColumnNames(final Table t) {
                    if (excludeColumns != null && includeColumns != null) {
                        throw new IllegalArgumentException(
                                "Can't have both excludeColumns and includeColumns not null");
                    }
                    final String[] tableColumnNames = t.getDefinition().getColumnNamesArray();
                    if (excludeColumns == null && includeColumns == null) {
                        return tableColumnNames;
                    }
                    final Set<String> tableColumnsSet = new HashSet<>(Arrays.asList(tableColumnNames));
                    if (includeColumns != null) {
                        // Validate includes
                        final List<String> missing = Arrays.stream(includeColumns)
                                .filter(cn -> !tableColumnsSet.contains(cn)).collect(Collectors.toList());
                        if (missing.size() > 0) {
                            throw new IllegalArgumentException(
                                    "includeColumns contains names not found in table columns: " + missing);
                        }
                        return includeColumns;
                    }
                    return Arrays.stream(tableColumnNames)
                            .filter(cn -> !excludeColumns.test(cn)).toArray(String[]::new);
                }

                String[] getFieldNames(final String[] columnNames) {
                    final String[] fieldNames = new String[columnNames.length];
                    for (int i = 0; i < columnNames.length; ++i) {
                        if (columnNameToFieldName == null) {
                            fieldNames[i] = columnNames[i];
                        } else {
                            fieldNames[i] = columnNameToFieldName.getOrDefault(columnNames[i], columnNames[i]);
                        }
                    }
                    return fieldNames;
                }
            }
        }

        public static final KeyOrValueSpec.Ignore IGNORE = new KeyOrValueSpec.Ignore();

        /**
         * Spec to explicitly ask
         * {@link #consumeToTable(Properties, String, IntPredicate, IntToLongFunction, Consume.KeyOrValueSpec, Consume.KeyOrValueSpec, TableType)
         * consumeToTable} to ignore either key or value.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec ignoreSpec() {
            return IGNORE;
        }


        /**
         * A simple spec for sending one column as either key or value in a Kafka message.
         *
         * @param columnName The name of the column to include.
         * @return A simple spec for the given input.
         */
        @SuppressWarnings("unused")
        public static KeyOrValueSpec simpleSpec(final String columnName) {
            return new KeyOrValueSpec.Simple(columnName);
        }

        /**
         * A JSON spec from a set of column names
         *
         * @param includeColumns An array with an entry for each column intended to be included in the JSON output. If
         *        null, include all columns except those specified in {@code excludeColumns}. If {@code includeColumns}
         *        is not null, {@code excludeColumns} should be null.
         * @param excludeColumns A set specifying column names to ommit; can only be used when {@columnNames} is null.
         *        In this case all table columns except for the ones in {@code excludeColumns} will be included.
         * @param columnToFieldMapping A map from column name to JSON field name to use for that column. Any column
         *        names implied by earlier arguments not included as a key in the map will be mapped to JSON fields of
         *        the same name. If null, map all columns to fields of the same name.
         * @param nestedObjectDelimiter if nested JSON fields are desired, the field separator that is used for the
         *        fieldNames parameter, or null for no nesting. For instance, if a particular column should be mapped to
         *        JSON field {@code X} nested inside field {@code Y}, the corresponding field name value for the column
         *        key in the {@code columnToFieldMapping} map can be the string {@code "X.Y"}, in which case the value
         *        for {@code nestedObjectDelimiter} should be {code "."}
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
            return new KeyOrValueSpec.Json(
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
         * @param excludeColumns A predicate specifying column names to ommit; can only be used when {@columnNames} is
         *        null. In this case all table columns except for the ones in {@code excludeColumns} will be included.
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
            return new KeyOrValueSpec.Avro(schema, null, null, fieldToColumnMapping,
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
            return new KeyOrValueSpec.Avro(
                    null, schemaName, schemaVersion, fieldToColumnMapping,
                    timestampFieldName, includeOnlyColumns, excludeColumns, publishSchema,
                    schemaNamespace, columnProperties);
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
         * {@link io.deephaven.engine.table.impl.StreamTableTools} for related tooling.
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
    @ScriptApi
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
     * Consume from Kafka to a Deephaven table.
     *
     * @param kafkaProperties Properties to configure this table and also to be passed to create the KafkaConsumer
     * @param topic Kafka topic name
     * @param partitionFilter A predicate returning true for the partitions to consume. The convenience constant
     *        {@code ALL_PARTITIONS} is defined to facilitate requesting all partitions.
     * @param partitionToInitialOffset A function specifying the desired initial offset for each partition consumed
     * @param keySpec Conversion specification for Kafka record keys
     * @param valueSpec Conversion specification for Kafka record values
     * @param resultType {@link TableType} specifying the type of the expected result
     * @return The result table containing Kafka stream data formatted according to {@code resultType}
     */
    @SuppressWarnings("unused")
    public static Table consumeToTable(
            @NotNull final Properties kafkaProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final IntToLongFunction partitionToInitialOffset,
            @NotNull final Consume.KeyOrValueSpec keySpec,
            @NotNull final Consume.KeyOrValueSpec valueSpec,
            @NotNull final TableType resultType) {
        final boolean ignoreKey = keySpec.dataFormat() == DataFormat.IGNORE;
        final boolean ignoreValue = valueSpec.dataFormat() == DataFormat.IGNORE;
        if (ignoreKey && ignoreValue) {
            throw new IllegalArgumentException(
                    "can't ignore both key and value: keySpec and valueSpec can't both be ignore specs");
        }
        if (ignoreKey) {
            setDeserIfNotSet(kafkaProperties, KeyOrValue.KEY, DESERIALIZER_FOR_IGNORE);
        }
        if (ignoreValue) {
            setDeserIfNotSet(kafkaProperties, KeyOrValue.VALUE, DESERIALIZER_FOR_IGNORE);
        }

        final ColumnDefinition<?>[] commonColumns = new ColumnDefinition<?>[3];
        getCommonCols(commonColumns, 0, kafkaProperties);
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
                getIngestData(KeyOrValue.KEY, kafkaProperties, columnDefinitions, nextColumnIndexMut, keySpec);
        final KeyOrValueIngestData valueIngestData =
                getIngestData(KeyOrValue.VALUE, kafkaProperties, columnDefinitions, nextColumnIndexMut,
                        valueSpec);

        final TableDefinition tableDefinition = new TableDefinition(columnDefinitions);

        final StreamTableMap streamTableMap = resultType.isMap ? new StreamTableMap(tableDefinition) : null;
        final UpdateSourceRegistrar updateSourceRegistrar =
                streamTableMap == null ? UpdateGraphProcessor.DEFAULT : streamTableMap.refreshCombiner;

        final Supplier<Pair<StreamToTableAdapter, ConsumerRecordToStreamPublisherAdapter>> adapterFactory = () -> {
            final StreamPublisherImpl streamPublisher = new StreamPublisherImpl();
            final StreamToTableAdapter streamToTableAdapter =
                    new StreamToTableAdapter(tableDefinition, streamPublisher, updateSourceRegistrar,
                            "Kafka-" + topic + '-' + partitionFilter);
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

        final MutableObject<KafkaIngester> kafkaIngesterHolder = new MutableObject<>();
        final UnaryOperator<Table> tableConversion =
                resultType.isAppend ? StreamTableTools::streamToAppendOnlyTable : UnaryOperator.identity();
        final Table result;
        final IntFunction<KafkaStreamConsumer> partitionToConsumer;
        if (resultType.isMap) {
            result = streamTableMap.asTable(true, true, true);
            partitionToConsumer = (final int partition) -> {
                final Pair<StreamToTableAdapter, ConsumerRecordToStreamPublisherAdapter> partitionAdapterPair =
                        adapterFactory.get();
                partitionAdapterPair.getFirst().setShutdownCallback(
                        () -> kafkaIngesterHolder.getValue().shutdownPartition(partition));
                final Table partitionTable = tableConversion.apply(partitionAdapterPair.getFirst().table());
                streamTableMap.enqueueUpdate(() -> Assert.eqNull(streamTableMap.put(partition, partitionTable),
                        "streamTableMap.put(partition, partitionTable)"));
                return new SimpleKafkaStreamConsumer(partitionAdapterPair.getSecond(), partitionAdapterPair.getFirst());
            };
        } else {
            final Pair<StreamToTableAdapter, ConsumerRecordToStreamPublisherAdapter> singleAdapterPair =
                    adapterFactory.get();
            result = tableConversion.apply(singleAdapterPair.getFirst().table());
            partitionToConsumer = (final int partition) -> {
                singleAdapterPair.getFirst().setShutdownCallback(() -> kafkaIngesterHolder.getValue().shutdown());
                return new SimpleKafkaStreamConsumer(singleAdapterPair.getSecond(), singleAdapterPair.getFirst());
            };
        }

        final KafkaIngester ingester = new KafkaIngester(
                log,
                kafkaProperties,
                topic,
                partitionFilter,
                partitionToConsumer,
                partitionToInitialOffset);
        kafkaIngesterHolder.setValue(ingester);
        ingester.start();

        return result;
    }

    private static KeyOrValueSerializer<?> getAvroSerializer(
            @NotNull final Table t,
            @NotNull final Produce.KeyOrValueSpec.Avro avroSpec,
            @NotNull final String[] columnNames) {
        return new GenericRecordKeyOrValueSerializer(
                t, avroSpec.schema, columnNames, avroSpec.timestampFieldName, avroSpec.columnProperties.getValue());
    }

    private static KeyOrValueSerializer<?> getJsonSerializer(
            @NotNull final Table t,
            @NotNull final Produce.KeyOrValueSpec.Json jsonSpec,
            @NotNull final String[] columnNames) {
        final String[] fieldNames = jsonSpec.getFieldNames(columnNames);
        return new JsonKeyOrValueSerializer(
                t, columnNames, fieldNames,
                jsonSpec.timestampFieldName, jsonSpec.nestedObjectDelimiter, jsonSpec.outputNulls);
    }

    private static KeyOrValueSerializer<?> getSerializer(
            @NotNull final Table t,
            @NotNull final Produce.KeyOrValueSpec spec,
            @NotNull final String[] columnNames) {
        switch (spec.dataFormat()) {
            case AVRO:
                final Produce.KeyOrValueSpec.Avro avroSpec = (Produce.KeyOrValueSpec.Avro) spec;
                return getAvroSerializer(t, avroSpec, columnNames);
            case JSON:
                final Produce.KeyOrValueSpec.Json jsonSpec = (Produce.KeyOrValueSpec.Json) spec;
                return getJsonSerializer(t, jsonSpec, columnNames);
            case IGNORE:
                return null;
            case SIMPLE:
                final Produce.KeyOrValueSpec.Simple simpleSpec = (Produce.KeyOrValueSpec.Simple) spec;
                return new SimpleKeyOrValueSerializer(t, simpleSpec.columnName);
            default:
                throw new IllegalStateException("Unrecognized spec type");
        }
    }

    private static String[] getColumnNames(
            @NotNull final Properties kafkaProperties,
            @NotNull final Table t,
            @NotNull final Produce.KeyOrValueSpec spec) {
        switch (spec.dataFormat()) {
            case AVRO:
                final Produce.KeyOrValueSpec.Avro avroSpec = (Produce.KeyOrValueSpec.Avro) spec;
                return avroSpec.getColumnNames(t, kafkaProperties);
            case JSON:
                final Produce.KeyOrValueSpec.Json jsonSpec = (Produce.KeyOrValueSpec.Json) spec;
                return jsonSpec.getColumnNames(t);
            case IGNORE:
                return null;
            case SIMPLE:
                final Produce.KeyOrValueSpec.Simple simpleSpec = (Produce.KeyOrValueSpec.Simple) spec;
                return new String[] {simpleSpec.columnName};
            default:
                throw new IllegalStateException("Unrecognized spec type");
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
     * {@link Table#STREAM_TABLE_ATTRIBUTE stream table} or should only ever add rows (regardless of whether the
     * {@link Table#ADD_ONLY_TABLE_ATTRIBUTE attribute} is specified).</li>
     * </ol>
     * <p>
     * If other use cases are identified, a publication mode or extensible listener framework may be introduced at a
     * later date.
     *
     * @param table The table used as a source of data to be sent to Kafka.
     * @param kafkaProperties Properties to be passed to create the associated KafkaProducer.
     * @param topic Kafka topic name
     * @param keySpec Conversion specification for Kafka record keys from table column data. If not
     *        {@link DataFormat#IGNORE Ignore}, must specify a key serializer that maps each input tuple to a unique
     *        output key.
     * @param valueSpec Conversion specification for Kafka record values from table column data.
     * @param lastByKeyColumns Whether to publish only the last record for each unique key. Ignored when {@code keySpec}
     *        is {@code IGNORE}. Otherwise, if {@code lastByKeycolumns == true} this method will internally perform a
     *        {@link Table#lastBy(String...) lastBy} aggregation on {@code table} grouped by the input columns of
     *        {@code keySpec} and publish to Kafka from the result.
     * @return a callback to stop producing and shut down the associated table listener; note a caller should keep a
     *         reference to this return value to ensure liveliness.
     */
    @SuppressWarnings("unused")
    public static Runnable produceFromTable(
            @NotNull final Table table,
            @NotNull final Properties kafkaProperties,
            @NotNull final String topic,
            @NotNull final Produce.KeyOrValueSpec keySpec,
            @NotNull final Produce.KeyOrValueSpec valueSpec,
            final boolean lastByKeyColumns) {
        if (table.isRefreshing()
                && !UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread()
                && !UpdateGraphProcessor.DEFAULT.sharedLock().isHeldByCurrentThread()) {
            throw new KafkaPublisherException(
                    "Calling thread must hold an exclusive or shared UpdateGraphProcessor lock to publish live sources");
        }

        final boolean ignoreKey = keySpec.dataFormat() == DataFormat.IGNORE;
        final boolean ignoreValue = valueSpec.dataFormat() == DataFormat.IGNORE;
        if (ignoreKey && ignoreValue) {
            throw new IllegalArgumentException(
                    "can't ignore both key and value: keySpec and valueSpec can't both be ignore specs");
        }
        setSerIfNotSet(kafkaProperties, KeyOrValue.KEY, keySpec, table);
        setSerIfNotSet(kafkaProperties, KeyOrValue.VALUE, valueSpec, table);

        final String[] keyColumns = getColumnNames(kafkaProperties, table, keySpec);
        final String[] valueColumns = getColumnNames(kafkaProperties, table, valueSpec);

        final LivenessScope publisherScope = new LivenessScope(true);
        try (final SafeCloseable ignored = LivenessScopeStack.open(publisherScope, false)) {
            final Table effectiveTable = (!ignoreKey && lastByKeyColumns)
                    ? table.lastBy(keyColumns)
                    : table.coalesce();

            final KeyOrValueSerializer<?> keySerializer = getSerializer(effectiveTable, keySpec, keyColumns);
            final KeyOrValueSerializer<?> valueSerializer = getSerializer(effectiveTable, valueSpec, valueColumns);

            final PublishToKafka producer = new PublishToKafka(
                    kafkaProperties, effectiveTable, topic,
                    keyColumns, keySerializer,
                    valueColumns, valueSerializer);
        }
        return publisherScope::release;
    }

    private static void setSerIfNotSet(
            @NotNull final Properties prop,
            @NotNull final KeyOrValue keyOrValue,
            @NotNull final Produce.KeyOrValueSpec spec,
            @NotNull final Table table) {
        final String propKey = (keyOrValue == KeyOrValue.KEY)
                ? ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
                : ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
        if (prop.containsKey(propKey)) {
            return;
        }
        final String value;
        switch (spec.dataFormat()) {
            case IGNORE:
                value = SERIALIZER_FOR_IGNORE;
                break;
            case SIMPLE:
                value = getSerializerNameForSimpleSpec(keyOrValue, (Produce.KeyOrValueSpec.Simple) spec, table);
                break;
            case JSON:
                value = STRING_SERIALIZER;
                break;
            case AVRO:
                value = AVRO_SERIALIZER;
                break;
            default:
                throw new IllegalStateException("Unknown dataFormat=" + spec.dataFormat());
        }
        prop.setProperty(propKey, value);
    }

    private static String getSerializerNameForSimpleSpec(
            @NotNull final KeyOrValue keyOrValue,
            @NotNull final Produce.KeyOrValueSpec.Simple simpleSpec,
            @NotNull final Table table) {
        final Class<?> dataType = table.getDefinition().getColumn(simpleSpec.columnName).getDataType();
        if (dataType == short.class) {
            return SHORT_SERIALIZER;
        }
        if (dataType == int.class) {
            return INT_SERIALIZER;
        }
        if (dataType == long.class) {
            return LONG_SERIALIZER;
        }
        if (dataType == float.class) {
            return FLOAT_SERIALIZER;
        }
        if (dataType == double.class) {
            return DOUBLE_SERIALIZER;
        }
        if (dataType == String.class) {
            return STRING_SERIALIZER;
        }
        throw new UncheckedDeephavenException(
                "Serializer for " + keyOrValue + " not set in kafka consumer properties " +
                        "and can't automatically set it for type " + dataType);
    }

    private static class StreamTableMap extends LocalTableMap implements Runnable {

        private final UpdateSourceCombiner refreshCombiner = new UpdateSourceCombiner();
        private final Queue<Runnable> deferredUpdates = new ConcurrentLinkedQueue<>();

        private StreamTableMap(@NotNull final TableDefinition constituentDefinition) {
            super(null, constituentDefinition);
            refreshCombiner.addSource(this); // Results in managing the refreshCombiner
            UpdateGraphProcessor.DEFAULT.addSource(refreshCombiner);
        }

        @Override
        public void run() {
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
            final Consume.KeyOrValueSpec spec,
            final TableDefinition tableDef,
            final StreamToTableAdapter streamToTableAdapter,
            final KeyOrValueIngestData data) {
        switch (spec.dataFormat()) {
            case IGNORE:
            case SIMPLE:
                return null;
            case AVRO:
                return GenericRecordChunkAdapter.make(
                        tableDef,
                        streamToTableAdapter::chunkTypeForIndex,
                        data.fieldPathToColumnName,
                        NESTED_FIELD_NAME_SEPARATOR_PATTERN,
                        (Schema) data.extra,
                        true);
            case JSON:
                return JsonNodeChunkAdapter.make(
                        tableDef, streamToTableAdapter::chunkTypeForIndex, data.fieldPathToColumnName, true);
            default:
                throw new IllegalStateException("Unknown KeyOrvalueSpec value" + spec.dataFormat());
        }
    }

    private static class KeyOrValueIngestData {
        public Map<String, String> fieldPathToColumnName;
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

    static Schema getAvroSchema(final Properties kafkaProperties, final String schemaName, final String schemaVersion) {
        final String schemaServiceUrl = kafkaProperties.getProperty(SCHEMA_SERVER_PROPERTY);
        if (schemaServiceUrl == null) {
            throw new IllegalArgumentException(
                    "Schema server url property " + SCHEMA_SERVER_PROPERTY + " not found.");

        }
        return getAvroSchema(schemaServiceUrl, schemaName, schemaVersion != null ? schemaVersion : AVRO_LATEST_VERSION);
    }

    static Schema getAvroSchema(final Properties kafkaProperties, final String schemaName) {
        return getAvroSchema(kafkaProperties, schemaName, AVRO_LATEST_VERSION);
    }

    private static KeyOrValueIngestData getIngestData(
            final KeyOrValue keyOrValue,
            final Properties kafkaConsumerProperties,
            final List<ColumnDefinition<?>> columnDefinitions,
            final MutableInt nextColumnIndexMut,
            final Consume.KeyOrValueSpec keyOrValueSpec) {
        if (keyOrValueSpec.dataFormat() == DataFormat.IGNORE) {
            return null;
        }
        final KeyOrValueIngestData data = new KeyOrValueIngestData();
        switch (keyOrValueSpec.dataFormat()) {
            case AVRO:
                setDeserIfNotSet(kafkaConsumerProperties, keyOrValue, AVRO_DESERIALIZER);
                final Consume.KeyOrValueSpec.Avro avroSpec = (Consume.KeyOrValueSpec.Avro) keyOrValueSpec;
                data.fieldPathToColumnName = new HashMap<>();
                final Schema schema;
                if (avroSpec.schema != null) {
                    schema = avroSpec.schema;
                } else {
                    schema = getAvroSchema(kafkaConsumerProperties, avroSpec.schemaName, avroSpec.schemaVersion);
                }
                avroSchemaToColumnDefinitions(
                        columnDefinitions, data.fieldPathToColumnName, schema, avroSpec.fieldPathToColumnName);
                data.extra = schema;
                break;
            case JSON:
                setDeserIfNotSet(kafkaConsumerProperties, keyOrValue, STRING_DESERIALIZER);
                data.toObjectChunkMapper = jsonToObjectChunkMapper;
                final Consume.KeyOrValueSpec.Json jsonSpec = (Consume.KeyOrValueSpec.Json) keyOrValueSpec;
                columnDefinitions.addAll(Arrays.asList(jsonSpec.columnDefinitions));
                // Populate out field to column name mapping from two potential sources.
                data.fieldPathToColumnName = new HashMap<>(jsonSpec.columnDefinitions.length);
                final Set<String> coveredColumns = new HashSet<>(jsonSpec.columnDefinitions.length);
                if (jsonSpec.fieldNameToColumnName != null) {
                    for (final Map.Entry<String, String> entry : jsonSpec.fieldNameToColumnName.entrySet()) {
                        final String colName = entry.getValue();
                        data.fieldPathToColumnName.put(entry.getKey(), colName);
                        coveredColumns.add(colName);
                    }
                }
                for (final ColumnDefinition<?> colDef : jsonSpec.columnDefinitions) {
                    final String colName = colDef.getName();
                    if (!coveredColumns.contains(colName)) {
                        data.fieldPathToColumnName.put(colName, colName);
                    }
                }
                break;
            case SIMPLE:
                data.simpleColumnIndex = nextColumnIndexMut.getAndAdd(1);
                final Consume.KeyOrValueSpec.Simple simpleSpec = (Consume.KeyOrValueSpec.Simple) keyOrValueSpec;
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
                (final String colName) -> ColumnDefinition.fromGenericType(colName, DateTime.class));
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
    public static final Consume.KeyOrValueSpec FROM_PROPERTIES = Consume.KeyOrValueSpec.FROM_PROPERTIES;

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
