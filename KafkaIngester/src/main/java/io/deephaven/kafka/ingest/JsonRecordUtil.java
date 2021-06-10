package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * <P>Provides methods and classes for retrieving typed values from a {@link JsonRecord}. The embedded classes
 * are used to connect set methods of {@link RowSetter} objects so they can be called without boxing
 * primitive types as they are read.</P>
 * <P>If AllowMissingKeys is false in the JsonRecord being used and a requested key does not exist in the
 * record, an IllegalArgumentException will be thrown. If missing keys are allowed, then the type-appropriate
 * null value will be returned for get requests of missing keys.</P>
 * <P>If AllowNullValues is false in the JsonRecord being used and a requested key has a null value in the
 * record, an IllegalArgumentException will be thrown. If null values are allowed, then the type-appropriate
 * null value will be returned for get requests of a key whose value is null.</P>
 */
public class JsonRecordUtil {

    private static JsonNode checkAllowMissingOrNull(@NotNull final JsonRecord record, @NotNull final String key) {
        final JsonNode tmpNode = record.getRecord().get(key);
        if (!(record.getAllowMissingKeys()) && tmpNode == null) {
            throw new IllegalArgumentException("Key " + key + " not found in the record, and allowMissingKeys is false.");
        }
        if (tmpNode != null && !(record.getAllowNullValues()) && tmpNode.isNull()) {
            throw new IllegalArgumentException("Value for Key " + key + " is null in the record, and allowNullValues is false.");
        }
        return tmpNode;
    }

    /**
     * Provides a Deephaven int-typed set method to call a {@link RowSetter} set method and populate it with a
     * value from a {@link JsonRecord}.
     */
    public static class JsonIntSetter implements JsonRecordSetter {
        @Override
        public void set(@NotNull final JsonRecord record,@NotNull final String key,@NotNull final RowSetter setter) {
            setter.setInt(getInt(record, key));
        }
    }

    /**
     * Provides a Deephaven long-typed set method to call a {@link RowSetter} set method and populate it with a
     * value from a {@link JsonRecord}.
     */
    public static class JsonLongSetter implements JsonRecordSetter {
        @Override
        public void set(@NotNull final JsonRecord record,@NotNull final String key,@NotNull final RowSetter setter) {
            setter.setLong(getLong(record, key));
        }
    }

    /**
     * Provides a Deephaven double-typed set method to call a {@link RowSetter} set method and populate it with a
     * value from a {@link JsonRecord}.
     */
    public static class JsonDoubleSetter implements JsonRecordSetter {
        @Override
        public void set(@NotNull final JsonRecord record,@NotNull final String key,@NotNull final RowSetter setter) {
            setter.setDouble(getDouble(record, key));
        }
    }

    /**
     * Provides a Deephaven float-typed set method to call a {@link RowSetter} set method and populate it with a
     * value from a {@link JsonRecord}.
     */
    public static class JsonFloatSetter implements JsonRecordSetter {
        @Override
        public void set(@NotNull final JsonRecord record,@NotNull final String key,@NotNull final RowSetter setter) {
            setter.setFloat(getFloat(record, key));
        }
    }

    /**
     * Provides a Deephaven short-typed set method to call a {@link RowSetter} set method and populate it with a
     * value from a {@link JsonRecord}.
     */
    public static class JsonShortSetter implements JsonRecordSetter {
        @Override
        public void set(@NotNull final JsonRecord record,@NotNull final String key,@NotNull final RowSetter setter) {
            setter.set(getShort(record, key));
        }
    }

    /**
     * Provides a generic Object set method to call a {@link RowSetter} set method and populate it with a
     * value from a {@link JsonRecord}.
     */
    public static class JsonSetter implements JsonRecordSetter {
        @Override
        public void set(@NotNull final JsonRecord record,@NotNull final String key,@NotNull final RowSetter setter) {
            setter.set(getValue(record, key));
        }
    }

    /**
     * Provides a String-typed set method to call a {@link RowSetter} set method and populate it with a
     * value from a {@link JsonRecord}.
     */
    public static class JsonStringSetter implements JsonRecordSetter {
        @Override
        public void set(@NotNull final JsonRecord record,@NotNull final String key,@NotNull final RowSetter setter) {
            setter.set(getString(record, key));
        }
    }

    /**
     * Provides a Boolean-typed set method to call a {@link RowSetter} set method and populate it with a
     * value from a {@link JsonRecord}.
     */
    public static class JsonBooleanSetter implements JsonRecordSetter {
        @Override
        public void set(@NotNull final JsonRecord record,@NotNull final String key,@NotNull final RowSetter setter) {
            setter.setBoolean(getBoolean(record, key));
        }
    }

    /**
     * Provides a Deephaven {@link DBDateTime}-typed set method to call a {@link RowSetter} set method and populate it with a
     * value from a {@link JsonRecord}.
     */
    public static class JsonDateTimeSetter implements JsonRecordSetter {
        @Override
        public void set(@NotNull final JsonRecord record,@NotNull final String key,@NotNull final RowSetter setter) {
            setter.set(getDBDateTime(record, key));
        }
    }

    /**
     * Provides a BigDecimal-typed set method to call a {@link RowSetter} set method and populate it with a
     * value from a {@link JsonRecord}.
     */
    public static class JsonBigDecimalSetter implements JsonRecordSetter {
        @Override
        public void set(@NotNull final JsonRecord record,@NotNull final String key,@NotNull final RowSetter setter) {
            setter.set(getBigDecimal(record, key));
        }
    }

    /**
     * Provides a BigInteger-typed set method to call a {@link RowSetter} set method and populate it with a
     * value from a {@link JsonRecord}.
     */
    public static class JsonBigIntegerSetter implements JsonRecordSetter {
        @Override
        public void set(@NotNull final JsonRecord record,@NotNull final String key,@NotNull final RowSetter setter) {
            setter.set(getBigInteger(record, key));
        }
    }

    /**
     * Gets the appropriate {@link JsonRecordSetter} for the type of the Deephaven column to be populated.
     * @param columnClass Class of the column to be populated.
     * @return A {@link JsonRecordSetter}
     */
    public static JsonRecordSetter getSetter(final Class columnClass) {
        switch(columnClass.getName()) {
            case "java.lang.String":
            case "java.lang.CharSequence":
                return new JsonStringSetter();
            case "long":
                return new JsonLongSetter();
            case "double":
                return new JsonDoubleSetter();
            case "int":
                return new JsonIntSetter();
            case "short":
                return new JsonShortSetter();
            case "float":
                return new JsonFloatSetter();
            case "java.lang.Boolean":
                return new JsonBooleanSetter();
            case "java.math.BigInteger":
                return new JsonBigIntegerSetter();
            case "java.math.BigDecimal":
                return new JsonBigDecimalSetter();
            case "io.deephaven.db.tables.utils.DBDateTime":
                return new JsonDateTimeSetter();
            default:
                return new JsonSetter();
        }
    }

    /**
     * @return true if node is null object or is a "Json" null
     */
    private static boolean isNullField(final JsonNode node) {
        return node == null || node.isNull();
    }

    /**
     * Returns a Deephaven int (primitive int with reserved values for null)
     * from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven int (primitive int with reserved values for null)
     */
    public static int getInt(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getInt(tmpNode);
    }

    /**
     * Returns a Deephaven int (primitive int with reserved values for null)
     * from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven int (primitive int with reserved values for null)
     */
    public static int getInt(JsonNode node) {
        return isNullField(node) ? QueryConstants.NULL_INT : node.asInt();
    }

    /**
     * Returns an Integer from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return An Integer
     */
    @Nullable
    public static Integer getBoxedInt(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getBoxedInt(tmpNode);
    }

    /**
     * Returns an Integer from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return An Integer
     */
    @Nullable
    public static Integer getBoxedInt(JsonNode node) {
        return isNullField(node) ? null : node.asInt();
    }

    /**
     * Returns a Deephaven short (primitive short with reserved values for Null)
     * from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven short (primitive short with reserved values for Null)
     */
    public static short getShort(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getShort(tmpNode);
    }

    /**
     * Returns a Deephaven short (primitive short with reserved values for Null)
     * from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven short (primitive short with reserved values for Null)
     */
    public static short getShort(JsonNode node) {
        return isNullField(node) ? QueryConstants.NULL_SHORT : (short)node.asInt();
    }

    /**
     * Returns a Short from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Short
     */
    @Nullable
    public static Short getBoxedShort(@NotNull final JsonRecord record, @NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getBoxedShort(tmpNode);
    }

    @Nullable
    public static Short getBoxedShort(JsonNode node) {
        return isNullField(node) ? null : (short)node.asInt();
    }

    /**
     * Returns a Deephaven long (primitive long with reserved values for Null)
     * from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven long (primitive long with reserved values for Null)
     */
    public static long getLong(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getLong(tmpNode);
    }

    /**
     * Returns a Deephaven long (primitive long with reserved values for null)
     * from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven long (primitive long with reserved values for null)
     */
    public static long getLong(JsonNode node) {
        return isNullField(node) ? QueryConstants.NULL_LONG : node.asLong();
    }

    /**
     * Returns a Long from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Long
     */
    @Nullable
    public static Long getBoxedLong(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getBoxedLong(tmpNode);
    }

    /**
     * Returns a Long from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Long
     */
    @Nullable
    public static Long getBoxedLong(JsonNode node) {
        return isNullField(node) ? null : node.asLong();
    }

    /**
     * Returns a Deephaven double (primitive double with reserved values for null)
     * from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven double (primitive double with reserved values for null)
     */
    public static double getDouble(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getDouble(tmpNode);
    }

    /**
     * Returns a Deephaven double (primitive double with reserved values for null)
     * from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven double (primitive double with reserved values for null)
     */
    public static double getDouble(JsonNode node) {
        return isNullField(node) ? QueryConstants.NULL_DOUBLE : node.asDouble();
    }

    /**
     * Returns a Double from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Double
     */
    @Nullable
    public static Double getBoxedDouble(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getBoxedDouble(tmpNode);
    }

    /**
     * Returns a Double from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Double
     */
    @Nullable
    public static Double getBoxedDouble(JsonNode node) {
        return isNullField(node) ? null : node.asDouble();
    }

    /**
     * Returns a Deephaven float (primitive float with reserved values for Null)
     * from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven float (primitive float with reserved values for Null)
     */
    public static float getFloat(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getFloat(tmpNode);
    }

    /**
     * Returns a Deephaven float (primitive float with reserved values for Null)
     * from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven float (primitive float with reserved values for Null)
     */
    public static float getFloat(JsonNode node) {
        return isNullField(node) ? QueryConstants.NULL_FLOAT : (float)node.asDouble();
    }

    /**
     * Returns a Float from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Float
     */
    @Nullable
    public static Float getBoxedFloat(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getBoxedFloat(tmpNode);
    }

    /**
     * Returns a Float from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Float
     */
    @Nullable
    public static Float getBoxedFloat(JsonNode node) {
        return isNullField(node) ? null : (float)node.asDouble();
    }

    /**
     * Returns a Deephaven byte (primitive byte with a reserved value for Null)
     * from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven byte (primitive byte with a reserved value for Null)
     */
    public static byte getByte(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getByte(tmpNode);
    }

    /**
     * Returns a Deephaven byte (primitive byte with a reserved value for Null)
     * from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven byte (primitive byte with a reserved value for Null)
     */
    public static byte getByte(JsonNode node) {
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
     * Returns a Byte from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Byte
     */
    @Nullable
    public static Byte getBoxedByte(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getBoxedByte(tmpNode);
    }

    /**
     * Returns a Byte from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Byte
     */
    @Nullable
    public static Byte getBoxedByte(JsonNode node) {
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
     * Returns a Deephaven char (primitive char with a reserved value for Null)
     * from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Deephaven char (primitive char with a reserved value for Null)
     */
    public static char getChar(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getChar(tmpNode);
    }

    /**
     * Returns a Deephaven char (primitive char with a reserved value for Null)
     * from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Deephaven char (primitive char with a reserved value for Null)
     */
    public static char getChar(JsonNode node) {
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
     * Returns a Character from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Character
     */
    @Nullable
    public static Character getBoxedChar(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getBoxedChar(tmpNode);
    }

    @Nullable
    public static Character getBoxedChar(JsonNode tmpNode) {
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
     * Returns a String from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A String
     */
    @Nullable
    public static String getString(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getString(tmpNode);
    }

    /**
     * Returns a String from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A String
     */
    @Nullable
    public static String getString(JsonNode node) {
        return isNullField(node) ? null : node.asText();
    }

    /**
     * Returns a Boolean from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A Boolean
     */
    public static Boolean getBoolean(@NotNull final JsonRecord record,@NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getBoolean(tmpNode);
    }

    /**
     * Returns a Boolean from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A Boolean
     */
    @Nullable
    public static Boolean getBoolean(JsonNode node) {
        return isNullField(node) ? null : node.asBoolean();
    }

    /**
     * Returns a BigInteger from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A BigInteger
     */
    public static BigInteger getBigInteger(@NotNull final JsonRecord record, @NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getBigInteger(tmpNode);
    }

    /**
     * Returns a BigInteger from a {@link JsonRecord}.
     * @param node The {@link JsonRecord} from which to retrieve the value.
     * @return A BigInteger
     */
    @Nullable
    public static BigInteger getBigInteger(JsonNode node) {
        return isNullField(node) ? null : node.bigIntegerValue();
    }

    /**
     * Returns a BigDecimal from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A BigDecimal
     */
    @Nullable
    public static BigDecimal getBigDecimal(@NotNull final JsonRecord record, @NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getBigDecimal(tmpNode);
    }

    /**
     * Returns a BigDecimal from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A BigDecimal
     */
    @Nullable
    public static BigDecimal getBigDecimal(JsonNode node) {
        return isNullField(node) ? null : node.decimalValue();
    }

    /**
     * Returns a generic Object from a {@link JsonRecord}.
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return An Object
     */
    @Nullable
    public static Object getValue(@NotNull final JsonRecord record, @NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getValue(tmpNode);
    }

    /**
     * Returns a generic Object from a {@link JsonNode}.
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return An Object
     */
    @Nullable
    public static Object getValue(JsonNode node) {
        return isNullField(node) ? null : node;
    }

    /**
     * Returns a {@link DBDateTime} from a {@link JsonRecord}. Will try to infer precision of a long
     * value to be parsed using {@link DBTimeUtils} autoEpochToTime. If the value in the JSON record
     * is not numeric, this method will attempt to parse it as a Deephaven DBDateTime string
     * (yyyy-MM-ddThh:mm:ss[.nnnnnnnnn] TZ).
     * @param record The {@link JsonRecord} from which to retrieve the value.
     * @param key The String key of the value to retrieve.
     * @return A {@link DBDateTime}
     */
    @Nullable
    public static DBDateTime getDBDateTime(@NotNull final JsonRecord record, @NotNull final String key) {
        final JsonNode tmpNode = checkAllowMissingOrNull(record, key);
        return getDBDateTime(tmpNode);
    }

    /**
     * Returns a {@link DBDateTime} from a {@link JsonNode}. Will try to infer precision of a long
     * value to be parsed using {@link DBTimeUtils} autoEpochToTime. If the value in the JSON record
     * is not numeric, this method will attempt to parse it as a Deephaven DBDateTime string
     * (yyyy-MM-ddThh:mm:ss[.nnnnnnnnn] TZ).
     * @param node The {@link JsonNode} from which to retrieve the value.
     * @return A {@link DBDateTime}
     */
    @Nullable
    public static DBDateTime getDBDateTime(JsonNode node) {
        if (isNullField(node)) {
            return null;
        }
        // Try to guess formatting from common formats
        // ISO Zoned String, millis (small number), or nanos (large number)
        if (node.isLong() || node.isInt()) {
            final long value = node.asLong();
            return DBTimeUtils.autoEpochToTime(value);
        } else {
            return DBTimeUtils.convertDateTime(node.asText());
        }
    }
}