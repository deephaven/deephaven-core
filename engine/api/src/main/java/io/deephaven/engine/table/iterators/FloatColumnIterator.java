//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterColumnIterator and run "./gradlew replicateColumnIterators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.primitive.function.FloatToDoubleFunction;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfFloat;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link ColumnIterator} implementation for columns of primitive floats.
 */
public interface FloatColumnIterator extends ColumnIterator<Float>, CloseablePrimitiveIteratorOfFloat {

    @Override
    @FinalDefault
    default Float next() {
        return TypeUtils.box(nextFloat());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Float> action) {
        forEachRemaining((final float element) -> action.accept(TypeUtils.box(element)));
    }

    /**
     * Create a {@link DoubleStream} over the remaining elements of this ChunkedFloatColumnIterator by applying
     * {@code adapter} to each element. The result <em>must</em> be {@link java.util.stream.BaseStream#close() closed}
     * in order to ensure resources are released. A try-with-resources block is strongly encouraged.
     *
     * @return A {@link DoubleStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    @FinalDefault
    default DoubleStream streamAsDouble(@NotNull final FloatToDoubleFunction adapter) {
        final PrimitiveIterator.OfDouble adapted = adaptToOfDouble(adapter);
        return StreamSupport.doubleStream(
                Spliterators.spliterator(
                        adapted,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    /**
     * Create an unboxed {@link DoubleStream} over the remaining elements of this ChunkedFloatColumnIterator by casting
     * each element to {@code double} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_FLOAT
     * NULL_FLOAT} to {@link io.deephaven.util.QueryConstants#NULL_DOUBLE NULL_DOUBLE}. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return An unboxed {@link DoubleStream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default DoubleStream streamAsDouble() {
        return streamAsDouble(
                (final float value) -> value == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_DOUBLE : (double) value);
    }

    /**
     * Create a boxed {@link Stream} over the remaining elements of this FloatColumnIterator. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default Stream<Float> stream() {
        return StreamSupport.stream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
}
