package io.deephaven.util;

/**
 * Constants for null and infinite values within the Deephaven engine.
 */
public class QueryConstants {
    /**
     * This class should not be instantiated.
     */
    protected QueryConstants() {}

    public static final Boolean NULL_BOOLEAN = null;
    public static final char NULL_CHAR = Character.MAX_VALUE - 1;
    public static final byte NULL_BYTE = Byte.MIN_VALUE;
    public static final short NULL_SHORT = Short.MIN_VALUE;
    public static final int NULL_INT = Integer.MIN_VALUE;
    public static final long NULL_LONG = Long.MIN_VALUE;
    public static final float NULL_FLOAT = -Float.MAX_VALUE;
    public static final double NULL_DOUBLE = -Double.MAX_VALUE;

    @SuppressWarnings("unused")
    public static final Character NULL_CHAR_BOXED = NULL_CHAR;
    @SuppressWarnings("unused")
    public static final Byte NULL_BYTE_BOXED = NULL_BYTE;
    @SuppressWarnings("unused")
    public static final Short NULL_SHORT_BOXED = NULL_SHORT;
    public static final Integer NULL_INT_BOXED = NULL_INT;
    public static final Long NULL_LONG_BOXED = NULL_LONG;
    public static final Float NULL_FLOAT_BOXED = NULL_FLOAT;
    public static final Double NULL_DOUBLE_BOXED = NULL_DOUBLE;

    public static final byte POS_INF_BYTE = Byte.MAX_VALUE;
    public static final short POS_INF_SHORT = Short.MAX_VALUE;
    public static final int POS_INF_INT = Integer.MAX_VALUE;
    public static final long POS_INF_LONG = Long.MAX_VALUE;
    public static final float POS_INF_FLOAT = Float.POSITIVE_INFINITY;
    public static final double POS_INF_DOUBLE = Double.POSITIVE_INFINITY;

    public static final byte NEG_INF_BYTE = Byte.MIN_VALUE;
    public static final short NEG_INF_SHORT = Short.MIN_VALUE + 1;
    public static final int NEG_INF_INT = Integer.MIN_VALUE + 1;
    public static final long NEG_INF_LONG = Long.MIN_VALUE + 1;
    public static final float NEG_INF_FLOAT = Float.NEGATIVE_INFINITY;
    public static final double NEG_INF_DOUBLE = Double.NEGATIVE_INFINITY;
}
