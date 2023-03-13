package io.deephaven.engine.primitive.iterator;

import io.deephaven.util.SafeCloseable;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This interface extends {@link Iterator} and {@link SafeCloseable} in order to allow for iterators that acquire
 * resources that must be released. Such iterators must document this need, and appropriate measures (e.g. a
 * try-with-resources block) should be taken to ensure that they are {@link #close() closed}. Methods that return
 * streams over CloseableIterator instances should ensure that closing the resulting {@link Stream},
 * {@link java.util.stream.IntStream IntStream}, {@link java.util.stream.LongStream LongStream}, or
 * {@link java.util.stream.DoubleStream DoubleStream} will also close the iterator.
 */
public interface CloseableIterator<TYPE> extends Iterator<TYPE>, SafeCloseable {

    /**
     * Create a {@link Stream} over the remaining elements of this CloseableIterator. Closing the result will close this
     * CloseableIterator.
     *
     * @return A {@link Stream} over the remaining contents of this iterator
     */
    default Stream<TYPE> stream() {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        this,
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    @Override
    default void close() {}
}
