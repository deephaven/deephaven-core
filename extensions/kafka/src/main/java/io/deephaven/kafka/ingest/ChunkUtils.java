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
import io.deephaven.functions.ToByteFunction;
import io.deephaven.functions.ToCharFunction;
import io.deephaven.functions.ToFloatFunction;
import io.deephaven.functions.ToShortFunction;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public class ChunkUtils {

    public static <T> void applyInto(
            Predicate<? super T> booleanFunction,
            ObjectChunk<? extends T, ?> src,
            int srcOffset,
            WritableBooleanChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, booleanFunction.test(src.get(i + srcOffset)));
        }
    }

    public static <T> void applyInto(
            ToByteFunction<? super T> byteFunction,
            ObjectChunk<? extends T, ?> src,
            int srcOffset,
            WritableByteChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, byteFunction.applyAsByte(src.get(srcOffset + i)));
        }
    }

    public static <T> void applyInto(
            ToCharFunction<? super T> charFunction,
            ObjectChunk<? extends T, ?> src,
            int srcOffset,
            WritableCharChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, charFunction.applyAsChar(src.get(srcOffset + i)));
        }
    }

    public static <T> void applyInto(
            ToShortFunction<? super T> shortFunction,
            ObjectChunk<? extends T, ?> src,
            int srcOffset,
            WritableShortChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, shortFunction.applyAsShort(src.get(srcOffset + i)));
        }
    }

    public static <T> void applyInto(
            ToIntFunction<? super T> intFunction,
            ObjectChunk<? extends T, ?> src,
            int srcOffset,
            WritableIntChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, intFunction.applyAsInt(src.get(srcOffset + i)));
        }
    }

    public static <T> void applyInto(
            ToLongFunction<? super T> longFunction,
            ObjectChunk<? extends T, ?> src,
            int srcOffset,
            WritableLongChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, longFunction.applyAsLong(src.get(srcOffset + i)));
        }
    }

    public static <T> void applyInto(
            ToFloatFunction<? super T> floatFunction,
            ObjectChunk<? extends T, ?> src,
            int srcOffset,
            WritableFloatChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, floatFunction.applyAsFloat(src.get(srcOffset + i)));
        }
    }

    public static <T> void applyInto(
            ToDoubleFunction<? super T> doubleFunction,
            ObjectChunk<? extends T, ?> src,
            int srcOffset,
            WritableDoubleChunk<?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, doubleFunction.applyAsDouble(src.get(srcOffset + i)));
        }
    }

    public static <T, R> void applyInto(
            Function<? super T, ? extends R> objFunction,
            ObjectChunk<? extends T, ?> src,
            int srcOffset,
            WritableObjectChunk<R, ?> dest,
            int destOffset,
            int length) {
        for (int i = 0; i < length; ++i) {
            dest.set(destOffset + i, objFunction.apply(src.get(srcOffset + i)));
        }
    }
}
