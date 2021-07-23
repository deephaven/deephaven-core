package io.deephaven.qst.array;

class Util {
    static final int DEFAULT_BUILDER_INITIAL_CAPACITY = 16;

    static final byte NULL_BYTE = Byte.MIN_VALUE;

    static final byte NULL_BOOL = (byte) -1;
    static final byte TRUE_BOOL = (byte) 1;
    static final byte FALSE_BOOL = (byte) 0;

    static final char NULL_CHAR = Character.MAX_VALUE - 1;

    static final double NULL_DOUBLE = -Double.MAX_VALUE;

    static final float NULL_FLOAT = -Float.MAX_VALUE;

    static final int NULL_INT = Integer.MIN_VALUE;

    static final long NULL_LONG = Long.MIN_VALUE;

    static final short NULL_SHORT = Short.MIN_VALUE;
}
