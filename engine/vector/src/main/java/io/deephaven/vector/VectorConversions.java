package io.deephaven.vector;

/**
 * Utility methods for converting vectors to native arrays. Used in formulas.
 */
@SuppressWarnings("unused")
public class VectorConversions {

    public static Boolean[] nullSafeVectorToArray(final BooleanVector vector) {
        return vector == null ? null : vector.toArray();
    }

    public static char[] nullSafeVectorToArray(final CharVector vector) {
        return vector == null ? null : vector.toArray();
    }

    public static byte[] nullSafeVectorToArray(final ByteVector vector) {
        return vector == null ? null : vector.toArray();
    }

    public static short[] nullSafeVectorToArray(final ShortVector vector) {
        return vector == null ? null : vector.toArray();
    }

    public static int[] nullSafeVectorToArray(final IntVector vector) {
        return vector == null ? null : vector.toArray();
    }

    public static long[] nullSafeVectorToArray(final LongVector vector) {
        return vector == null ? null : vector.toArray();
    }

    public static float[] nullSafeVectorToArray(final FloatVector vector) {
        return vector == null ? null : vector.toArray();
    }

    public static double[] nullSafeVectorToArray(final DoubleVector vector) {
        return vector == null ? null : vector.toArray();
    }

    public static <T> T[] nullSafeVectorToArray(final ObjectVector<T> vector) {
        return vector == null ? null : vector.toArray();
    }
}
