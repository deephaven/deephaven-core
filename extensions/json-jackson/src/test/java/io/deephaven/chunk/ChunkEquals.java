//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import java.util.Arrays;
import java.util.Objects;

// todo: current dupe w/ extensions-kafka-v2
public class ChunkEquals {

    // todo: should probably be moved into respective classes



    public static boolean equals(Chunk<?> x, Chunk<?> y) {
        if (!x.getChunkType().equals(y.getChunkType())) {
            return false;
        }
        switch (x.getChunkType()) {
            case Boolean:
                return equals((BooleanChunk<?>) x, (BooleanChunk<?>) y);
            case Char:
                return equals((CharChunk<?>) x, (CharChunk<?>) y);
            case Byte:
                return equals((ByteChunk<?>) x, (ByteChunk<?>) y);
            case Short:
                return equals((ShortChunk<?>) x, (ShortChunk<?>) y);
            case Int:
                return equals((IntChunk<?>) x, (IntChunk<?>) y);
            case Long:
                return equals((LongChunk<?>) x, (LongChunk<?>) y);
            case Float:
                return equals((FloatChunk<?>) x, (FloatChunk<?>) y);
            case Double:
                return equals((DoubleChunk<?>) x, (DoubleChunk<?>) y);
            case Object:
                // Note: we can't be this precise b/c io.deephaven.chunk.ObjectChunk.makeArray doesn't actually use the
                // typed
                // class
                /*
                 * if (!actualObjectChunk.data.getClass().equals(expectedObjectChunk.data.getClass())) { return false; }
                 */
                return equals((ObjectChunk<Object, ?>) x, (ObjectChunk<Object, ?>) y);
            default:
                throw new IllegalStateException();
        }
    }

    public static boolean equals(BooleanChunk<?> actual, BooleanChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(ByteChunk<?> actual, ByteChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(CharChunk<?> actual, CharChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(ShortChunk<?> actual, ShortChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(IntChunk<?> actual, IntChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(LongChunk<?> actual, LongChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(FloatChunk<?> actual, FloatChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static boolean equals(DoubleChunk<?> actual, DoubleChunk<?> expected) {
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size);
    }

    public static <T> boolean equals(ObjectChunk<T, ?> actual, ObjectChunk<T, ?> expected) {
        // this is probably only suitable for unit testing, not something we'd want in real code?
        return Arrays.equals(actual.data, actual.offset, actual.size, expected.data, expected.offset, expected.size,
                ChunkEquals::fakeDeepCompareForEquals);
    }

    private static int fakeDeepCompareForEquals(Object a, Object b) {
        return Objects.deepEquals(a, b) ? 0 : 1;
    }
}
