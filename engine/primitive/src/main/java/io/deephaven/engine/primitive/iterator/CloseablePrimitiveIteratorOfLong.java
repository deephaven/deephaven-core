/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CloseablePrimitiveIteratorOfInt and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.primitive.iterator;

import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code long}.
 */
public interface CloseablePrimitiveIteratorOfLong
        extends PrimitiveIterator.OfLong, CloseablePrimitiveIterator<Long, LongConsumer> {

    /**
     * Create a {@link LongStream} over the remaining elements of this CloseablePrimitiveIteratorOfLong. Closing the
     * result will close this CloseablePrimitiveIteratorOfLong.
     *
     * @return A {@link LongStream} over the remaining contents of this iterator
     */
    default LongStream longStream() {
        return StreamSupport.longStream(
                Spliterators.spliteratorUnknownSize(
                        this,
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
}
