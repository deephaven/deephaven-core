package io.deephaven.qst.array;

class Util {
    static final int DEFAULT_BUILDER_INITIAL_CAPACITY = 16;

    // TODO(deephaven-core#895): Consolidate NULL_X paths

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

    static byte adapt(Boolean x) {
        return x == null ? NULL_BOOL : (x ? TRUE_BOOL : FALSE_BOOL);
    }

    static byte adapt(Byte x) {
        return x == null ? NULL_BYTE : x;
    }

    static char adapt(Character x) {
        return x == null ? NULL_CHAR : x;
    }

    static double adapt(Double x) {
        return x == null ? NULL_DOUBLE : x;
    }

    static float adapt(Float x) {
        return x == null ? NULL_FLOAT : x;
    }

    static int adapt(Integer x) {
        return x == null ? NULL_INT : x;
    }

    static long adapt(Long x) {
        return x == null ? NULL_LONG : x;
    }

    static short adapt(Short x) {
        return x == null ? NULL_SHORT : x;
    }
}
