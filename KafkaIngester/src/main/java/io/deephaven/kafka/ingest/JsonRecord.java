package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.jetbrains.annotations.NotNull;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Provides convenience methods to facilitate parsing of a JSON String and extraction of typed values.
 */
public class JsonRecord {

    private final JsonNode record;
    private final boolean allowMissingKeys;
    private final boolean allowNullValues;
    private static final ObjectMapper objectMapper = new ObjectMapper().setNodeFactory(JsonNodeFactory.withExactBigDecimals(true)).configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    /**
     * Encapsulates a JSONObject from which to retrieve Deephaven column values. Works in conjunction
     * with {@link JsonRecordUtil}.
     * @param json A string representation of the JSON object.
     * @param allowMissingKeys If true, attempts to retrieve values that don't exist in the record will
     *                         return a null or QueryConstants null primitive value; if false, the attempt
     *                         will result in an InvalidArgumentException being thrown.
     * @param allowNullValues If true, attempts to retrieve values that are null in the record will
     *                         return a null or QueryConstants null primitive value; if false, the attempt
     *                         will result in an InvalidArgumentException being thrown.
     * @throws IllegalArgumentException if the JSON String cannot be parsed.
     */
    public JsonRecord(@NotNull String json, boolean allowMissingKeys, boolean allowNullValues) {
        try {
            this.record = objectMapper.readTree(json);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Failed to parse JSON string.", ex);
        }
        this.allowMissingKeys = allowMissingKeys;
        this.allowNullValues = allowNullValues;
    }

    /**
     * Encapsulates a JSONObject from which to retrieve Deephaven column values. Works in conjunction
     * with {@link JsonRecordUtil}.
     * @param jsonNode A JsonNode to encapsulate.
     * @param allowMissingKeys If true, attempts to retrieve values that don't exist in the record will
     *                         return a null or QueryConstants null primitive value; if false, the attempt
     *                         will result in an InvalidArgumentException being thrown.
     * @param allowNullValues If true, attempts to retrieve values that are null in the record will
     *                         return a null or QueryConstants null primitive value; if false, the attempt
     *                         will result in an InvalidArgumentException being thrown.
     * @throws IllegalArgumentException if the JSON String cannot be parsed.
     */
    public JsonRecord(@NotNull JsonNode jsonNode, boolean allowMissingKeys, boolean allowNullValues) {
        this.record = jsonNode;
        this.allowMissingKeys = allowMissingKeys;
        this.allowNullValues = allowNullValues;
    }

    /**
     * Encapsulates a JSONObject from which to retrieve Deephaven column values. Works in conjunction
     * with {@link JsonRecordUtil}.
     * @param json A string representation of the JSON object.
     * @throws IllegalArgumentException if the JSON String cannot be parsed.
     */
    public JsonRecord(@NotNull String json) {
        this(json, true, true);
    }

    /**
     * Encapsulates a JSONObject from which to retrieve Deephaven column values. Works in conjunction
     * with {@link JsonRecordUtil}.
     * @param jsonNode A JsonNode to encapsulate.
     * @throws IllegalArgumentException if the JSON String cannot be parsed.
     */
    public JsonRecord(@NotNull JsonNode jsonNode) {
        this(jsonNode, true, true);
    }

    public JsonNode getRecord() {
        return this.record;
    }

    /**
     * @return A boolean indicating whether this JsonRecord allows requests for keys that are not in the
     * field set.
     */
    public boolean getAllowMissingKeys() {
        return this.allowMissingKeys;
    }

    /**
     * @return A boolean indicating whether this JsonRecord allows null values.
     */
    public boolean getAllowNullValues() {
        return this.allowNullValues;
    }

    /**
     * Returns a value by key from the JSON record. Uses JSON implementation methods to interpret data values from
     * the record. The second argument to getValue is used to select which method needs to be used to retrieve and
     * interpret the value. In the case of DBDateTime columns, this method will try to guess the formatting of the
     * data to be converted to a DBDateTime.
     * @param key The property of the JSON record to retrieve.
     * @param columnClass The class of the column to be filled from the result, which drives the
     *                    selection of JsonObject methods and other interpretation needed.
     * @return An object of compatible type to the columnClass requested; a generic object if the requested class
     * is an unexpected/unhandled type; or null when the underlying field is null or date parsing fails.
     */
    Object getValue(@NotNull String key, @NotNull Class columnClass) {
        switch(columnClass.getName()) {
            case "java.lang.Character":
                return JsonRecordUtil.getBoxedChar(this, key);
            case "char":
                return JsonRecordUtil.getChar(this, key);
            case "java.lang.Byte":
                return JsonRecordUtil.getBoxedByte(this, key);
            case "byte":
                return JsonRecordUtil.getByte(this, key);
            case "java.lang.String":
            case "java.lang.CharSequence":
                return JsonRecordUtil.getString(this, key);
            case "java.lang.Long":
                return JsonRecordUtil.getBoxedLong(this, key);
            case "long":
                return JsonRecordUtil.getLong(this, key);
            case "java.lang.Double":
                return JsonRecordUtil.getBoxedDouble(this, key);
            case "double":
                return JsonRecordUtil.getDouble(this, key);
            case "java.lang.Float":
                return JsonRecordUtil.getBoxedFloat(this, key);
            case "float":
                return JsonRecordUtil.getFloat(this, key);
            case "java.lang.Integer":
                return JsonRecordUtil.getBoxedInt(this, key);
            case "int":
                return JsonRecordUtil.getInt(this, key);
            case "java.lang.Short":
                return JsonRecordUtil.getBoxedShort(this, key);
            case "short":
                return JsonRecordUtil.getShort(this, key);
            case "java.lang.Boolean":
            case "boolean":
                return JsonRecordUtil.getBoolean(this, key);
            case "java.math.BigInteger":
                return JsonRecordUtil.getBigInteger(this, key);
            case "java.math.BigDecimal":
                return JsonRecordUtil.getBigDecimal(this, key);
            case "io.deephaven.db.tables.utils.DBDateTime":
                return JsonRecordUtil.getDBDateTime(this, key);
            default:
                return record.get(key);
        }
    }

}
