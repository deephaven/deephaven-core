/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.functions.BooleanFunction;
import io.deephaven.functions.ByteFunction;
import io.deephaven.functions.CharFunction;
import io.deephaven.functions.DoubleFunction;
import io.deephaven.functions.FloatFunction;
import io.deephaven.functions.IntFunction;
import io.deephaven.functions.LongFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.functions.ShortFunction;

class ChunkUtils {

    public static <T> void applyInto(
            BooleanFunction<T> booleanFunction,
            ObjectChunk<T, ?> src,
            int srcOffset,
            WritableBooleanChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, booleanFunction.test(src.get(i + srcOffset)));
        }
    }

    public static <T> void applyInto(
            ByteFunction<T> byteFunction,
            ObjectChunk<T, ?> src,
            int srcOffset,
            WritableByteChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, byteFunction.applyAsByte(src.get(srcOffset + i)));
        }
    }

    public static <T> void applyInto(
            CharFunction<T> charFunction,
            ObjectChunk<T, ?> src,
            int srcOffset,
            WritableCharChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, charFunction.applyAsChar(src.get(srcOffset + i)));
        }
    }

    public static <T> void applyInto(
            ShortFunction<T> shortFunction,
            ObjectChunk<T, ?> src,
            int srcOffset,
            WritableShortChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, shortFunction.applyAsShort(src.get(srcOffset + i)));
        }
    }

    public static <T> void applyInto(
            IntFunction<T> intFunction,
            ObjectChunk<T, ?> src,
            int srcOffset,
            WritableIntChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, intFunction.applyAsInt(src.get(srcOffset + i)));
        }
    }


    public static <T> void applyInto(
            LongFunction<T> longFunction,
            ObjectChunk<T, ?> src,
            int srcOffset,
            WritableLongChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, longFunction.applyAsLong(src.get(srcOffset + i)));
        }
    }

    public static <T> void applyInto(
            FloatFunction<T> floatFunction,
            ObjectChunk<T, ?> src,
            int srcOffset,
            WritableFloatChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, floatFunction.applyAsFloat(src.get(srcOffset + i)));
        }
    }

    public static <T> void applyInto(
            DoubleFunction<T> doubleFunction,
            ObjectChunk<T, ?> src,
            int srcOffset,
            WritableDoubleChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, doubleFunction.applyAsDouble(src.get(srcOffset + i)));
        }
    }

    public static <T, R> void applyInto(
            ObjectFunction<T, R> objFunction,
            ObjectChunk<T, ?> src,
            int srcOffset,
            WritableObjectChunk<R, ?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, objFunction.apply(src.get(srcOffset + i)));
        }
    }
}
