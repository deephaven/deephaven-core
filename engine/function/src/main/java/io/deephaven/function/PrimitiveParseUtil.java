package io.deephaven.function;


import static io.deephaven.util.QueryConstants.*;

/**
 * A set of commonly used functions for parsing strings to primitive values.
 */
@SuppressWarnings("WeakerAccess")
public class PrimitiveParseUtil {

    // byte

    /**
     * Parses the string argument as a {@code byte}.
     *
     * @param s string.
     * @return parsed value.
     */
    public static byte parseByte(String s) {
        if (s == null) {
            return NULL_BYTE;
        }
        return Byte.parseByte(s);
    }

    /**
     * Parses the string argument as a {@code byte}.
     *
     * @param s string.
     * @param radix The radix to use in parsing {@code s}
     * @return parsed value.
     */
    public static byte parseByte(String s, int radix) {
        if (s == null || IntegerPrimitives.isNull(radix)) {
            return NULL_BYTE;
        }
        return Byte.parseByte(s, radix);
    }

    // short

    /**
     * Parses the string argument as a {@code short}.
     *
     * @param s string.
     * @return parsed value.
     */
    public static short parseShort(String s) {
        if (s == null) {
            return NULL_SHORT;
        }
        return Short.parseShort(s);
    }

    /**
     * Parses the string argument as a {@code short}.
     *
     * @param s string.
     * @param radix The radix to use in parsing {@code s}
     * @return parsed value.
     */
    public static short parseShort(String s, int radix) {
        if (s == null || IntegerPrimitives.isNull(radix)) {
            return NULL_SHORT;
        }
        return Short.parseShort(s, radix);
    }

    // integer

    /**
     * Parses the string argument as an {@code int}.
     *
     * @param s string.
     * @return parsed value.
     */
    public static int parseInt(String s) {
        if (s == null) {
            return NULL_INT;
        }
        return Integer.parseInt(s);
    }

    /**
     * Parses the string argument as an {@code int}.
     *
     * @param s string.
     * @param radix The radix to use in parsing {@code s}
     * @return parsed value.
     */
    public static int parseInt(String s, int radix) {
        if (s == null || IntegerPrimitives.isNull(radix)) {
            return NULL_INT;
        }
        return Integer.parseInt(s, radix);
    }

    /**
     * Parses the string argument as an unsigned {@code int}.
     *
     * @param s string.
     * @return parsed value.
     */
    public static int parseUnsignedInt(String s) {
        if (s == null) {
            return NULL_INT;
        }
        return Integer.parseUnsignedInt(s);
    }

    /**
     * Parses the string argument as an unsigned {@code int}.
     *
     * @param s string.
     * @param radix The radix to use in parsing {@code s}
     * @return parsed value.
     */
    public static int parseUnsignedInt(String s, int radix) {
        if (s == null || IntegerPrimitives.isNull(radix)) {
            return NULL_INT;
        }
        return Integer.parseUnsignedInt(s, radix);
    }

    // long

    /**
     * Parses the string argument as a {@code long}.
     *
     * @param s string.
     * @return parsed value.
     */
    public static long parseLong(String s) {
        if (s == null) {
            return NULL_LONG;
        }
        return Long.parseLong(s);
    }

    /**
     * Parses the string argument as a {@code long}.
     *
     * @param s string.
     * @param radix The radix to use in parsing {@code s}
     * @return parsed value.
     */
    public static long parseLong(String s, int radix) {
        if (s == null || IntegerPrimitives.isNull(radix)) {
            return NULL_LONG;
        }
        return Long.parseLong(s, radix);
    }

    /**
     * Parses the string argument as an unsigned {@code long}.
     *
     * @param s string.
     * @return parsed value.
     */
    public static long parseUnsignedLong(String s) {
        if (s == null) {
            return NULL_LONG;
        }
        return Long.parseUnsignedLong(s);
    }

    /**
     * Parses the string argument as an unsigned {@code long}.
     *
     * @param s string.
     * @param radix The radix to use in parsing {@code s}
     * @return parsed value.
     */
    public static long parseUnsignedLong(String s, int radix) {
        if (s == null || IntegerPrimitives.isNull(radix)) {
            return NULL_LONG;
        }
        return Long.parseUnsignedLong(s, radix);
    }

    // double

    /**
     * Parses the string argument as a {@code double}.
     *
     * @param s string.
     * @return parsed value.
     */
    public static double parseDouble(String s) {
        if (s == null) {
            return NULL_DOUBLE;
        }
        return Double.parseDouble(s);
    }

    // float

    /**
     * Parses the string argument as a {@code float}.
     *
     * @param s string.
     * @return parsed value.
     */
    public static float parseFloat(String s) {
        if (s == null) {
            return NULL_FLOAT;
        }
        return Float.parseFloat(s);
    }

    // boolean

    /**
     * Parses the string argument as a {@code boolean}.
     *
     * @param s string.
     * @return parsed value.
     */
    public static Boolean parseBoolean(String s) {
        if (s == null) {
            return NULL_BOOLEAN;
        }
        return s.equalsIgnoreCase("true");
    }

}
