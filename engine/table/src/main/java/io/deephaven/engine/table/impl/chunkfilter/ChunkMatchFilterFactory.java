//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.engine.table.MatchOptions;
import io.deephaven.util.type.ArrayTypeUtils;

public class ChunkMatchFilterFactory {
    private ChunkMatchFilterFactory() {} // static only

    public static ChunkFilter getChunkFilter(
            final Class type,
            final MatchOptions matchOptions,
            final Object... keys) {
        if (keys.length == 0) {
            if (matchOptions.inverted()) {
                return ChunkFilter.TRUE_FILTER_INSTANCE;
            } else {
                return ChunkFilter.FALSE_FILTER_INSTANCE;
            }
        }
        if (type == char.class) {
            final char[] charKeys = ArrayTypeUtils.getUnboxedCharArray(keys);
            return CharChunkMatchFilterFactory.makeFilter(matchOptions, charKeys);
        }
        if (type == byte.class) {
            final byte[] byteKeys = ArrayTypeUtils.getUnboxedByteArray(keys);
            return ByteChunkMatchFilterFactory.makeFilter(matchOptions, byteKeys);
        }
        if (type == short.class) {
            final short[] shortKeys = ArrayTypeUtils.getUnboxedShortArray(keys);
            return ShortChunkMatchFilterFactory.makeFilter(matchOptions, shortKeys);
        }
        if (type == int.class) {
            final int[] intKeys = ArrayTypeUtils.getUnboxedIntArray(keys);
            return IntChunkMatchFilterFactory.makeFilter(matchOptions, intKeys);
        }
        if (type == long.class) {
            final long[] longKeys = ArrayTypeUtils.getUnboxedLongArray(keys);
            return LongChunkMatchFilterFactory.makeFilter(matchOptions, longKeys);
        }
        if (type == float.class) {
            final float[] floatKeys = ArrayTypeUtils.getUnboxedFloatArray(keys);
            return FloatChunkMatchFilterFactory.makeFilter(matchOptions, floatKeys);
        }
        if (type == double.class) {
            final double[] doubleKeys = ArrayTypeUtils.getUnboxedDoubleArray(keys);
            return DoubleChunkMatchFilterFactory.makeFilter(matchOptions, doubleKeys);
        }
        if (type == String.class && matchOptions.caseInsensitive()) {
            return StringChunkMatchFilterFactory.makeCaseInsensitiveFilter(matchOptions, keys);
        }
        // TODO: we should do something nicer with booleans
        // TODO: we need to consider symbol tables
        // TODO: we need to consider reinterpretation of DateTimes
        return ObjectChunkMatchFilterFactory.makeFilter(matchOptions, keys);
    }
}
