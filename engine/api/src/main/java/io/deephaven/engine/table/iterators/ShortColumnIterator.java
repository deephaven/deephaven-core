//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterColumnIterator and run "./gradlew replicateColumnIterators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.primitive.function.ShortToIntFunction;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfShort;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link ColumnIterator} implementation for columns of primitive shorts.
 */
public interface ShortColumnIterator extends ColumnIterator<Short>, CloseablePrimitiveIteratorOfShort {

    @Override
    @FinalDefault
    default Short next() {
        return TypeUtils.box(nextShort());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Short> action) {
        forEachRemaining((final short element) -> action.accept(TypeUtils.box(element)));
    }

    /**
     * Create a {@link IntStream} over the remaining elements of this ChunkedShortColumnIterator by applying
     * {@code adapter} to each element. The result <em>must</em> be {@link java.util.stream.BaseStream#close() closed}
     * in order to ensure resources are released. A try-with-resources block is strongly encouraged.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    @FinalDefault
    default IntStream streamAsInt(@NotNull final ShortToIntFunction adapter) {
        final PrimitiveIterator.OfInt adapted = adaptToOfInt(adapter);
        return StreamSupport.intStream(
                Spliterators.spliterator(
                        adapted,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    /**
     * Create an unboxed {@link IntStream} over the remaining elements of this ChunkedShortColumnIterator by casting
     * each element to {@code int} with the appropriate adjustment of {@link io.deephaven.util.QueryConstants#NULL_SHORT
     * NULL_SHORT} to {@link io.deephaven.util.QueryConstants#NULL_INT NULL_INT}. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return An unboxed {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default IntStream streamAsInt() {
        return streamAsInt(
                (final short value) -> value == QueryConstants.NULL_SHORT ? QueryConstants.NULL_INT : (int) value);
    }

    /**
     * Create a boxed {@link Stream} over the remaining elements of this ShortColumnIterator. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default Stream<Short> stream() {
        return StreamSupport.stream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
}
