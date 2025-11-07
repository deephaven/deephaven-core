//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.value.iterator;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface ValueIterator<TYPE> extends CloseableIterator<TYPE> {
    /**
     * @return The number of elements remaining in this ValueIterator
     */
    long remaining();

    /**
     * Create a {@link Stream} over the remaining elements of this ValueIterator. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    default Stream<TYPE> stream() {
        return StreamSupport.stream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }

    /**
     * A re-usable, immutable ValueIterator with no elements.
     */
    ValueIterator<?> EMPTY = new ValueIterator<>() {
        @Override
        public Object next() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public long remaining() {
            return 0;
        }
    };

    /**
     * Get a ValueIterator with no elements. The result does not need to be {@link #close() closed}.
     *
     * @return A ValueIterator with no elements
     */
    static <TYPE> ValueIterator<TYPE> empty() {
        // noinspection unchecked
        return (ValueIterator<TYPE>) EMPTY;
    }

    /**
     * Create a ValueIterator over an array of {@code TYPE}. The result does not need to be {@link #close() closed}.
     *
     * @param values The elements to iterate
     * @return A ValueIterator of {@code values}
     */
    static <TYPE> ValueIterator<TYPE> of(@NotNull final TYPE... values) {
        Objects.requireNonNull(values);
        return new ValueIterator<>() {

            private int valueIndex;

            @Override
            public TYPE next() {
                if (valueIndex < values.length) {
                    return values[valueIndex++];
                }
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasNext() {
                return valueIndex < values.length;
            }

            @Override
            public long remaining() {
                return values.length - valueIndex;
            }
        };
    }

    /**
     * Wraps a ValueIterator with set number of prefix nulls, postfix nulls, or both. The result must be {@link #close()
     * closed}.
     *
     * @param iterator The ValueIterator to wrap
     * @param prefixNulls The number of nulls to add to the beginning of the iterator
     * @param postfixNulls The number of nulls to add to the end of the iterator
     * @return A ValueIterator with the specified number of prefix and postfix nulls
     */
    static <TYPE> ValueIterator<TYPE> wrapWithNulls(
            @Nullable final ValueIterator<TYPE> iterator,
            long prefixNulls,
            long postfixNulls) {

        if (prefixNulls == 0 && postfixNulls == 0) {
            return iterator == null ? ValueIterator.empty() : iterator;
        }
        final long initialLength = prefixNulls + postfixNulls + (iterator == null ? 0 : iterator.remaining());
        return new ValueIterator<>() {
            private long nextIndex = 0;

            @Override
            public TYPE next() {
                if (nextIndex >= initialLength) {
                    throw new NoSuchElementException();
                }
                if (nextIndex++ < prefixNulls || iterator == null || !iterator.hasNext()) {
                    return null;
                }
                return iterator.next();
            }

            @Override
            public boolean hasNext() {
                return nextIndex < initialLength;
            }

            @Override
            public long remaining() {
                return initialLength - nextIndex;
            }
        };
    }
}
