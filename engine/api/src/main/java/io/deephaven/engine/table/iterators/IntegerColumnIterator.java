//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link ColumnIterator} implementation for columns of primitive ints.
 */
public interface IntegerColumnIterator extends ColumnIterator<Integer>, CloseablePrimitiveIteratorOfInt {

    @Override
    @FinalDefault
    default Integer next() {
        return TypeUtils.box(nextInt());
    }

    @Override
    @FinalDefault
    default void forEachRemaining(@NotNull final Consumer<? super Integer> action) {
        forEachRemaining((final int element) -> action.accept(TypeUtils.box(element)));
    }

    /**
     * Create an unboxed {@link IntStream} over the remaining elements of this IntegerColumnIterator. The result
     * <em>must</em> be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return An unboxed {@link IntStream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default IntStream intStream() {
        return StreamSupport.intStream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    /**
     * Create a boxed {@link Stream} over the remaining elements of this IntegerColumnIterator. The result <em>must</em>
     * be {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A boxed {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close()
     *         closed}.
     */
    @Override
    @FinalDefault
    default Stream<Integer> stream() {
        return intStream().mapToObj(TypeUtils::box);
    }
}
