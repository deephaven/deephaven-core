package io.deephaven.util;

/**
 * Constants for primitive types within the Deephaven engine.  These constants include null values, ranges of values, infinite values, and NaN values.
 */
public class QueryConstants {
    /**
     * This class should not be instantiated.
     */
    private QueryConstants() {}


    /////////////////////////////////////////////////////////////////

    /**
     * Null boolean value.
     */
    public static final Boolean NULL_BOOLEAN = null;


    /////////////////////////////////////////////////////////////////


    /**
     * Null char value.
     */
    public static final char NULL_CHAR = Character.MAX_VALUE - 1;

    /**
     * Null byte value.
     */
    public static final byte NULL_BYTE = Byte.MIN_VALUE;

    /**
     * Null short value.
     */
    public static final short NULL_SHORT = Short.MIN_VALUE;
    
    /**
     * Null int value.
     */
    public static final int NULL_INT = Integer.MIN_VALUE;

    /**
     * Null long value.
     */
    public static final long NULL_LONG = Long.MIN_VALUE;

    /**
     * Null float value.
     */
    public static final float NULL_FLOAT = -Float.MAX_VALUE;

    /**
     * Null double value.
     */
    public static final double NULL_DOUBLE = -Double.MAX_VALUE;


    /////////////////////////////////////////////////////////////////


    /**
     * Null boxed Character value.
     */
    @SuppressWarnings("unused")
    public static final Character NULL_CHAR_BOXED = NULL_CHAR;

    /**
     * Null boxed Byte value.
     */
    @SuppressWarnings("unused")
    public static final Byte NULL_BYTE_BOXED = NULL_BYTE;

    /**
     * Null boxed Short value.
     */
    @SuppressWarnings("unused")
    public static final Short NULL_SHORT_BOXED = NULL_SHORT;

    /**
     * Null boxed Integer value.
     */
    public static final Integer NULL_INT_BOXED = NULL_INT;

    /**
     * Null boxed Long value.
     */
    public static final Long NULL_LONG_BOXED = NULL_LONG;

    /**
     * Null boxed Float value.
     */
    public static final Float NULL_FLOAT_BOXED = NULL_FLOAT;

    /**
     * Null boxed Double value.
     */
    public static final Double NULL_DOUBLE_BOXED = NULL_DOUBLE;


    /////////////////////////////////////////////////////////////////


    /**
     * Maximum value of type byte.
     */
    public static final byte MAX_BYTE = Byte.MAX_VALUE;

    /**
     * Maximum value of type short.
     */
    public static final short MAX_SHORT = Short.MAX_VALUE;

    /**
     * Maximum value of type int.
     */
    public static final int MAX_INT = Integer.MAX_VALUE;

    /**
     * Maximum value of type long.
     */
    public static final long MAX_LONG = Long.MAX_VALUE;

    /**
     * Maximum finite value of type float.
     */
    public static final float MAX_FLOAT = Float.MAX_VALUE;

    /**
     * Maximum finite value of type double.
     */
    public static final double MAX_DOUBLE = Double.MAX_VALUE;


    /////////////////////////////////////////////////////////////////


    /**
     * Minimum value of type byte.
     */
    public static final byte MIN_BYTE = Byte.MIN_VALUE + 1;

    /**
     * Minimum value of type short.
     */
    public static final short MIN_SHORT = Short.MIN_VALUE + 1;

    /**
     * Minimum value of type int.
     */
    public static final int MIN_INT = Integer.MIN_VALUE + 1;

    /**
     * Minimum value of type long.
     */
    public static final long MIN_LONG = Long.MIN_VALUE + 1;

    /**
     * Minimum finite value of type float.
     */
    public static final float MIN_FLOAT = Float.valueOf("-0x1.fffffdp127");

    /**
     * Minimum finite value of type double.
     */
    public static final double MIN_DOUBLE = Double.valueOf("-0x1.ffffffffffffep1023");


    /////////////////////////////////////////////////////////////////


    /**
     * Positive infinity of type float.
     */
    public static final float POSITIVE_INFINITY_FLOAT = Float.POSITIVE_INFINITY;

    /**
     * Positive infinity of type double.
     */
    public static final double POSITIVE_INFINITY_DOUBLE = Double.POSITIVE_INFINITY;


    /////////////////////////////////////////////////////////////////


    /**
     * Negative infinity of type float.
     */
    public static final float NEGATIVE_INFINITY_FLOAT = Float.NEGATIVE_INFINITY;

    /**
     * Negative infinity of type double.
     */
    public static final double NEGATIVE_INFINITY_DOUBLE = Double.NEGATIVE_INFINITY;


    /////////////////////////////////////////////////////////////////


    /**
     * Not-a-Number (NaN) of type float.
     */
    public static final float NAN_FLOAT = Float.NaN;

    /**
     * Not-a-Number (NaN) of type double.
     */
    public static final double NAN_DOUBLE = Double.NaN;
}
