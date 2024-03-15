//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit IntegerColumnIterator and run "./gradlew replicateColumnIterators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfDouble;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link ColumnIterator} implementation for columns of primitive doubles.
 */
public interface DoubleColumnIterator extends ColumnIterator<Double>, CloseablePrimitiveIteratorOfDouble {

    @Override
    @FinalDefault
    default Double next() {
        return TypeUtils.box(nextDouble());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Double> action) {
        forEachRemaining((final double element) -> action.accept(TypeUtils.box(element)));
    }

    /**
     * Create an unboxed {@link DoubleStream} over the remaining elements of this DoubleColumnIterator. The result
     * <em>must</em> be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return An unboxed {@link DoubleStream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default DoubleStream doubleStream() {
        return StreamSupport.doubleStream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    /**
     * Create a boxed {@link Stream} over the remaining elements of this DoubleColumnIterator. The result <em>must</em>
     * be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default Stream<Double> stream() {
        return doubleStream().mapToObj(TypeUtils::box);
    }
}
