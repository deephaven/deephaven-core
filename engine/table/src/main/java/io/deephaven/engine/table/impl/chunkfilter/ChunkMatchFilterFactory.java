package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.util.type.ArrayTypeUtils;

public class ChunkMatchFilterFactory {
    private ChunkMatchFilterFactory() {} // static only

    public static ChunkFilter getChunkFilter(Class type, boolean caseInsensitive, boolean invertMatch,
            final Object... keys) {
        if (keys.length == 0) {
            if (invertMatch) {
                return ChunkFilter.TRUE_FILTER_INSTANCE;
            } else {
                return ChunkFilter.FALSE_FILTER_INSTANCE;
            }
        }
        if (type == char.class) {
            final char[] charKeys = ArrayTypeUtils.getUnboxedCharArray(keys);
            return CharChunkMatchFilterFactory.makeFilter(invertMatch, charKeys);
        }
        if (type == byte.class) {
            final byte[] byteKeys = ArrayTypeUtils.getUnboxedByteArray(keys);
            return ByteChunkMatchFilterFactory.makeFilter(invertMatch, byteKeys);
        }
        if (type == short.class) {
            final short[] shortKeys = ArrayTypeUtils.getUnboxedShortArray(keys);
            return ShortChunkMatchFilterFactory.makeFilter(invertMatch, shortKeys);
        }
        if (type == int.class) {
            final int[] intKeys = ArrayTypeUtils.getUnboxedIntArray(keys);
            return IntChunkMatchFilterFactory.makeFilter(invertMatch, intKeys);
        }
        if (type == long.class) {
            final long[] longKeys = ArrayTypeUtils.getUnboxedLongArray(keys);
            return LongChunkMatchFilterFactory.makeFilter(invertMatch, longKeys);
        }
        if (type == float.class) {
            final float[] floatKeys = ArrayTypeUtils.getUnboxedFloatArray(keys);
            return FloatChunkMatchFilterFactory.makeFilter(invertMatch, floatKeys);
        }
        if (type == double.class) {
            final double[] doubleKeys = ArrayTypeUtils.getUnboxedDoubleArray(keys);
            return DoubleChunkMatchFilterFactory.makeFilter(invertMatch, doubleKeys);
        }
        if (type == String.class && caseInsensitive) {
            return StringChunkMatchFilterFactory.makeCaseInsensitiveFilter(invertMatch, keys);
        }
        // TODO: we should do something nicer with booleans
        // TODO: we need to consider symbol tables
        // TODO: we need to consider reinterpretation of DateTimes
        return ObjectChunkMatchFilterFactory.makeFilter(invertMatch, keys);
    }
}
