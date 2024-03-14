//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;

public class JsonNodeUtil {
    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper()
            .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    public static JsonNode makeJsonNode(final ObjectMapper mapper, final String json) {
        try {
            return mapper != null ? mapper.readTree(json) : DEFAULT_OBJECT_MAPPER.readTree(json);
        } catch (JsonProcessingException ex) {
            throw new UncheckedDeephavenException("Failed to parse JSON string.", ex);
        }
    }

    private static JsonNode checkAllowMissingOrNull(
            final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        final JsonNode tmpNode = node == null ? null : node.get(key);
        checkNode(key, tmpNode, allowMissingKeys, allowNullValues);
        return tmpNode;
    }

    private static JsonNode checkAllowMissingOrNull(
            final JsonNode node, @NotNull final JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        final JsonNode tmpNode = node == null ? null : node.at(ptr);
        checkNode(ptr, tmpNode, allowMissingKeys, allowNullValues);
        return tmpNode;
    }

    private static void checkNode(Object key, JsonNode node, boolean allowMissingKeys, boolean allowNullValues) {
        if (!allowMissingKeys && (node == null || node.isMissingNode())) {
            throw new IllegalArgumentException(
                    String.format("Key '%s' not found in the record, and allowMissingKeys is false.", key));
        }
        if (!allowNullValues && isNullOrMissingField(node)) {
            throw new IllegalArgumentException(String
                    .format("Value for '%s' is null or missing in the record, and allowNullValues is false.", key));
        }
    }

    /**
     * @return true if node is null object or is a "Json" null or missing
     */
    private static boolean isNullOrMissingField(final JsonNode node) {
        return node == null || node.isNull() || node.isMissingNode();
    }

    /**
     * Returns a Deephaven int (primitive int with reserved values for null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven int (primitive int with reserved values for null)
     */
    public static int getInt(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getInt(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven int (primitive int with reserved values for null) from a {@link JsonNode}.
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @return A Deephaven int (primitive int with reserved values for null)
     */
    public static int getInt(@NotNull final JsonNode node, @NotNull JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getInt(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven int (primitive int with reserved values for null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven int (primitive int with reserved values for null)
     */
    public static int getInt(final JsonNode node) {
        return isNullOrMissingField(node) ? QueryConstants.NULL_INT : node.asInt();
    }

    /**
     * Returns an Integer from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return An Integer
     */
    @Nullable
    public static Integer getBoxedInt(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return TypeUtils.box(getInt(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns an Integer from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return An Integer
     */
    @Nullable
    public static Integer getBoxedInt(final JsonNode node) {
        return TypeUtils.box(getInt(node));
    }

    /**
     * Returns a Deephaven short (primitive short with reserved values for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven short (primitive short with reserved values for Null)
     */
    public static short getShort(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getShort(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven short (primitive short with reserved values for Null) from a {@link JsonNode}.
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @return A Deephaven short (primitive short with reserved values for Null)
     */
    public static short getShort(@NotNull final JsonNode node, @NotNull JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getShort(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven short (primitive short with reserved values for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven short (primitive short with reserved values for Null)
     */
    public static short getShort(final JsonNode node) {
        return isNullOrMissingField(node) ? QueryConstants.NULL_SHORT : (short) node.asInt();
    }

    /**
     * Returns a Short from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Short
     */
    @Nullable
    public static Short getBoxedShort(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return TypeUtils.box(getShort(node, key, allowMissingKeys, allowNullValues));
    }

    @Nullable
    public static Short getBoxedShort(final JsonNode node) {
        return TypeUtils.box(getShort(node));
    }

    /**
     * Returns a Deephaven long (primitive long with reserved values for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven long (primitive long with reserved values for Null)
     */
    public static long getLong(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getLong(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven long (primitive long with reserved values for Null) from a {@link JsonNode}.
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @return A Deephaven long (primitive long with reserved values for Null)
     */
    public static long getLong(@NotNull final JsonNode node, @NotNull JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getLong(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven long (primitive long with reserved values for null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven long (primitive long with reserved values for null)
     */
    public static long getLong(final JsonNode node) {
        return isNullOrMissingField(node) ? QueryConstants.NULL_LONG : node.asLong();
    }

    /**
     * Returns a Long from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Long
     */
    @Nullable
    public static Long getBoxedLong(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return TypeUtils.box(getLong(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Long from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Long
     */
    @Nullable
    public static Long getBoxedLong(final JsonNode node) {
        return TypeUtils.box(getLong(node));
    }

    /**
     * Returns a Deephaven double (primitive double with reserved values for null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven double (primitive double with reserved values for null)
     */
    public static double getDouble(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getDouble(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven double (primitive double with reserved values for null) from a {@link JsonNode}.
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @return A Deephaven double (primitive double with reserved values for null)
     */
    public static double getDouble(@NotNull final JsonNode node, @NotNull JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getDouble(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven double (primitive double with reserved values for null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven double (primitive double with reserved values for null)
     */
    public static double getDouble(final JsonNode node) {
        return isNullOrMissingField(node) ? QueryConstants.NULL_DOUBLE : node.asDouble();
    }

    /**
     * Returns a Double from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Double
     */
    @Nullable
    public static Double getBoxedDouble(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return TypeUtils.box(getDouble(node, key, allowMissingKeys, allowNullValues));

    }

    /**
     * Returns a Double from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Double
     */
    @Nullable
    public static Double getBoxedDouble(final JsonNode node) {
        return TypeUtils.box(getDouble(node));
    }

    /**
     * Returns a Deephaven float (primitive float with reserved values for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven float (primitive float with reserved values for Null)
     */
    public static float getFloat(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getFloat(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven float (primitive float with reserved values for Null) from a {@link JsonNode}.
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @return A Deephaven float (primitive float with reserved values for Null)
     */
    public static float getFloat(@NotNull final JsonNode node, @NotNull JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getFloat(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven float (primitive float with reserved values for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven float (primitive float with reserved values for Null)
     */
    public static float getFloat(final JsonNode node) {
        return isNullOrMissingField(node) ? QueryConstants.NULL_FLOAT : (float) node.asDouble();
    }

    /**
     * Returns a Float from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Float
     */
    @Nullable
    public static Float getBoxedFloat(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return TypeUtils.box(getFloat(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Float from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Float
     */
    @Nullable
    public static Float getBoxedFloat(final JsonNode node) {
        return TypeUtils.box(getFloat(node));
    }

    /**
     * Returns a Deephaven byte (primitive byte with a reserved value for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven byte (primitive byte with a reserved value for Null)
     */
    public static byte getByte(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getByte(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven byte (primitive byte with a reserved value for Null) from a {@link JsonNode}.
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @return A Deephaven byte (primitive byte with a reserved value for Null)
     */
    public static byte getByte(@NotNull final JsonNode node, @NotNull JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getByte(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven byte (primitive byte with a reserved value for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven byte (primitive byte with a reserved value for Null)
     */
    public static byte getByte(final JsonNode node) {
        if (isNullOrMissingField(node)) {
            return QueryConstants.NULL_BYTE;
        }
        return (byte) node.asInt();
    }

    /**
     * Returns a Byte from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Byte
     */
    @Nullable
    public static Byte getBoxedByte(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return TypeUtils.box(getByte(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Byte from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Byte
     */
    @Nullable
    public static Byte getBoxedByte(final JsonNode node) {
        return TypeUtils.box(getByte(node));
    }

    /**
     * Returns a Deephaven char (primitive char with a reserved value for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven char (primitive char with a reserved value for Null)
     */
    public static char getChar(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getChar(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven char (primitive char with a reserved value for Null) from a {@link JsonNode}.
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @return A Deephaven char (primitive char with a reserved value for Null)
     */
    public static char getChar(@NotNull final JsonNode node, @NotNull final JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getChar(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Deephaven char (primitive char with a reserved value for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven char (primitive char with a reserved value for Null)
     */
    public static char getChar(final JsonNode node) {
        if (isNullOrMissingField(node)) {
            return QueryConstants.NULL_CHAR;
        }
        if (node.isNumber()) {
            // We don't expect this to be a common case; but if the node happens to be a number, we'll assume it's meant
            // to represent a char by casting. This is much more appropriate than just taking the first character of the
            // number's string.
            return (char) node.numberValue().intValue();
        }
        final String s = node.asText();
        // It would not be unreasonable for us to throw an error if s.length() != 1.
        // All of the other code paths are very lenient though, so we'll be lenient here too.
        if (s.isEmpty()) {
            return QueryConstants.NULL_CHAR;
        }
        return s.charAt(0);
    }

    /**
     * Returns a Character from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Character
     */
    @Nullable
    public static Character getBoxedChar(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return TypeUtils.box(getChar(node, key, allowMissingKeys, allowNullValues));
    }

    @Nullable
    public static Character getBoxedChar(final JsonNode node) {
        return TypeUtils.box(getChar(node));
    }

    /**
     * Returns a String from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A String
     */
    @Nullable
    public static String getString(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getString(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a String from a {@link JsonNode}.
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @return A String
     */
    @Nullable
    public static String getString(@NotNull final JsonNode node, @NotNull final JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getString(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a String from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A String
     */
    @Nullable
    public static String getString(final JsonNode node) {
        if (isNullOrMissingField(node)) {
            return null;
        }
        return node.isValueNode() ? node.asText() : node.toString();
    }

    /**
     * Returns a Boolean from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Boolean
     */
    public static Boolean getBoolean(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getBoolean(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Boolean from a {@link JsonNode}.
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @return A Boolean
     */
    public static Boolean getBoolean(@NotNull final JsonNode node, @NotNull final JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getBoolean(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a Boolean from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Boolean
     */
    @Nullable
    public static Boolean getBoolean(final JsonNode node) {
        return isNullOrMissingField(node) ? null : node.asBoolean();
    }

    /**
     * Returns a BigInteger from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A BigInteger
     */
    public static BigInteger getBigInteger(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getBigInteger(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a BigInteger from a {@link JsonNode}.
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @return A BigInteger
     */
    public static BigInteger getBigInteger(@NotNull final JsonNode node, @NotNull final JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getBigInteger(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a BigInteger from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A BigInteger
     */
    @Nullable
    public static BigInteger getBigInteger(final JsonNode node) {
        return isNullOrMissingField(node) ? null : node.bigIntegerValue();
    }

    /**
     * Returns a BigDecimal from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A BigDecimal
     */
    @Nullable
    public static BigDecimal getBigDecimal(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getBigDecimal(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a BigDecimal from a {@link JsonNode}.
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @return A BigDecimal
     */
    @Nullable
    public static BigDecimal getBigDecimal(@NotNull final JsonNode node, @NotNull final JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getBigDecimal(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a BigDecimal from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A BigDecimal
     */
    @Nullable
    public static BigDecimal getBigDecimal(final JsonNode node) {
        return isNullOrMissingField(node) ? null : node.decimalValue();
    }

    /**
     * Returns a generic Object from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return An Object
     */
    @Nullable
    public static Object getValue(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getValue(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns a generic Object from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return An Object
     */
    @Nullable
    public static Object getValue(final JsonNode node) {
        return isNullOrMissingField(node) ? null : node;
    }

    /**
     * Returns an {@link Instant} from a {@link JsonNode}. Will try to infer precision of a long value to be parsed
     * using {@link DateTimeUtils} autoEpochToTime. If the value in the JSON record is not numeric, this method will
     * attempt to parse it as a Deephaven Instant string (yyyy-MM-ddThh:mm:ss[.nnnnnnnnn] TZ).
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @returnan {@link Instant}
     */
    @Nullable
    public static Instant getInstant(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getInstant(checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns an {@link Instant} from a {@link JsonNode}. Will try to infer precision of a long value to be parsed
     * using {@link DateTimeUtils} autoEpochToTime. If the value in the JSON record is not numeric, this method will
     * attempt to parse it as a Deephaven Instant string (yyyy-MM-ddThh:mm:ss[.nnnnnnnnn] TZ).
     *
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param ptr A JsonPointer to the node for the value to retrieve.
     * @returnan {@link Instant}
     */
    @Nullable
    public static Instant getInstant(@NotNull final JsonNode node, @NotNull final JsonPointer ptr,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        return getInstant(checkAllowMissingOrNull(node, ptr, allowMissingKeys, allowNullValues));
    }

    /**
     * Returns an {@link Instant} from a {@link JsonNode}. Will try to infer precision of a long value to be parsed
     * using {@link DateTimeUtils} autoEpochToTime. If the value in the JSON record is not numeric, this method will
     * attempt to parse it as a Deephaven Instant string (yyyy-MM-ddThh:mm:ss[.nnnnnnnnn] TZ).
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @returnan {@link Instant}
     */
    @Nullable
    public static Instant getInstant(final JsonNode node) {
        if (isNullOrMissingField(node)) {
            return null;
        }
        // Try to guess formatting from common formats
        // ISO Zoned String, millis (small number), or nanos (large number)
        if (node.isLong() || node.isInt()) {
            final long value = node.asLong();
            return DateTimeUtils.epochAutoToInstant(value);
        } else {
            return DateTimeUtils.parseInstant(node.asText());
        }
    }
}
