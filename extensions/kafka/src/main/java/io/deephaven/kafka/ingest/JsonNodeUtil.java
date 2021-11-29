package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;

public class JsonNodeUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    public static JsonNode makeJsonNode(final String json) {
        final JsonNode node;
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException ex) {
            throw new UncheckedDeephavenException("Failed to parse JSON string.", ex);
        }
    }

    private static JsonNode checkAllowMissingOrNull(
            final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        final JsonNode tmpNode = node == null ? null : node.get(key);
        if (!allowMissingKeys && tmpNode == null) {
            throw new IllegalArgumentException(
                    "Key " + key + " not found in the record, and allowMissingKeys is false.");
        }
        if (tmpNode != null && !allowNullValues && tmpNode.isNull()) {
            throw new IllegalArgumentException(
                    "Value for Key " + key + " is null in the record, and allowNullValues is false.");
        }
        return tmpNode;
    }

    /**
     * @return true if node is null object or is a "Json" null
     */
    private static boolean isNullField(final JsonNode node) {
        return node == null || node.isNull();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getInt(tmpNode);
    }

    /**
     * Returns a Deephaven int (primitive int with reserved values for null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven int (primitive int with reserved values for null)
     */
    public static int getInt(final JsonNode node) {
        return isNullField(node) ? QueryConstants.NULL_INT : node.asInt();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getBoxedInt(tmpNode);
    }

    /**
     * Returns an Integer from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return An Integer
     */
    @Nullable
    public static Integer getBoxedInt(final JsonNode node) {
        return isNullField(node) ? null : node.asInt();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getShort(tmpNode);
    }

    /**
     * Returns a Deephaven short (primitive short with reserved values for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven short (primitive short with reserved values for Null)
     */
    public static short getShort(final JsonNode node) {
        return isNullField(node) ? QueryConstants.NULL_SHORT : (short) node.asInt();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getBoxedShort(tmpNode);
    }

    @Nullable
    public static Short getBoxedShort(final JsonNode node) {
        return isNullField(node) ? null : (short) node.asInt();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getLong(tmpNode);
    }

    /**
     * Returns a Deephaven long (primitive long with reserved values for null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven long (primitive long with reserved values for null)
     */
    public static long getLong(final JsonNode node) {
        return isNullField(node) ? QueryConstants.NULL_LONG : node.asLong();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getBoxedLong(tmpNode);
    }

    /**
     * Returns a Long from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Long
     */
    @Nullable
    public static Long getBoxedLong(final JsonNode node) {
        return isNullField(node) ? null : node.asLong();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getDouble(tmpNode);
    }

    /**
     * Returns a Deephaven double (primitive double with reserved values for null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven double (primitive double with reserved values for null)
     */
    public static double getDouble(final JsonNode node) {
        return isNullField(node) ? QueryConstants.NULL_DOUBLE : node.asDouble();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getBoxedDouble(tmpNode);
    }

    /**
     * Returns a Double from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Double
     */
    @Nullable
    public static Double getBoxedDouble(final JsonNode node) {
        return isNullField(node) ? null : node.asDouble();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getFloat(tmpNode);
    }

    /**
     * Returns a Deephaven float (primitive float with reserved values for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven float (primitive float with reserved values for Null)
     */
    public static float getFloat(final JsonNode node) {
        return isNullField(node) ? QueryConstants.NULL_FLOAT : (float) node.asDouble();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getBoxedFloat(tmpNode);
    }

    /**
     * Returns a Float from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Float
     */
    @Nullable
    public static Float getBoxedFloat(final JsonNode node) {
        return isNullField(node) ? null : (float) node.asDouble();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getByte(tmpNode);
    }

    /**
     * Returns a Deephaven byte (primitive byte with a reserved value for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven byte (primitive byte with a reserved value for Null)
     */
    public static byte getByte(final JsonNode node) {
        if (isNullField(node)) {
            return QueryConstants.NULL_BYTE;
        }
        final byte[] bytes = node.asText().getBytes();
        if (bytes.length == 0) {
            return QueryConstants.NULL_BYTE;
        }
        return bytes[0];
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getBoxedByte(tmpNode);
    }

    /**
     * Returns a Byte from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Byte
     */
    @Nullable
    public static Byte getBoxedByte(final JsonNode node) {
        if (isNullField(node)) {
            return null;
        }
        final byte[] bytes = node.asText().getBytes();
        if (bytes.length == 0) {
            return null;
        }
        return bytes[0];
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getChar(tmpNode);
    }

    /**
     * Returns a Deephaven char (primitive char with a reserved value for Null) from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven char (primitive char with a reserved value for Null)
     */
    public static char getChar(final JsonNode node) {
        if (isNullField(node)) {
            return QueryConstants.NULL_CHAR;
        }
        final String s = node.asText();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getBoxedChar(tmpNode);
    }

    @Nullable
    public static Character getBoxedChar(final JsonNode tmpNode) {
        if (isNullField(tmpNode)) {
            return null;
        }
        final String s = tmpNode.asText();
        if (s.isEmpty()) {
            return null;
        }
        return s.charAt(0);
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getString(tmpNode);
    }

    /**
     * Returns a String from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A String
     */
    @Nullable
    public static String getString(final JsonNode node) {
        return isNullField(node) ? null : node.asText();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getBoolean(tmpNode);
    }

    /**
     * Returns a Boolean from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Boolean
     */
    @Nullable
    public static Boolean getBoolean(final JsonNode node) {
        return isNullField(node) ? null : node.asBoolean();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getBigInteger(tmpNode);
    }

    /**
     * Returns a BigInteger from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A BigInteger
     */
    @Nullable
    public static BigInteger getBigInteger(final JsonNode node) {
        return isNullField(node) ? null : node.bigIntegerValue();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getBigDecimal(tmpNode);
    }

    /**
     * Returns a BigDecimal from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A BigDecimal
     */
    @Nullable
    public static BigDecimal getBigDecimal(final JsonNode node) {
        return isNullField(node) ? null : node.decimalValue();
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
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getValue(tmpNode);
    }

    /**
     * Returns a generic Object from a {@link JsonNode}.
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return An Object
     */
    @Nullable
    public static Object getValue(final JsonNode node) {
        return isNullField(node) ? null : node;
    }

    /**
     * Returns a {@link DateTime} from a {@link JsonNode}. Will try to infer precision of a long value to be parsed
     * using {@link DateTimeUtils} autoEpochToTime. If the value in the JSON record is not numeric, this method will
     * attempt to parse it as a Deephaven DateTime string (yyyy-MM-ddThh:mm:ss[.nnnnnnnnn] TZ).
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A {@link DateTime}
     */
    @Nullable
    public static DateTime getDateTime(@NotNull final JsonNode node, @NotNull final String key,
            final boolean allowMissingKeys, final boolean allowNullValues) {
        final JsonNode tmpNode = checkAllowMissingOrNull(node, key, allowMissingKeys, allowNullValues);
        return getDateTime(tmpNode);
    }

    /**
     * Returns a {@link DateTime} from a {@link JsonNode}. Will try to infer precision of a long value to be parsed
     * using {@link DateTimeUtils} autoEpochToTime. If the value in the JSON record is not numeric, this method will
     * attempt to parse it as a Deephaven DateTime string (yyyy-MM-ddThh:mm:ss[.nnnnnnnnn] TZ).
     * 
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A {@link DateTime}
     */
    @Nullable
    public static DateTime getDateTime(final JsonNode node) {
        if (isNullField(node)) {
            return null;
        }
        // Try to guess formatting from common formats
        // ISO Zoned String, millis (small number), or nanos (large number)
        if (node.isLong() || node.isInt()) {
            final long value = node.asLong();
            return DateTimeUtils.autoEpochToTime(value);
        } else {
            return DateTimeUtils.convertDateTime(node.asText());
        }
    }
}
