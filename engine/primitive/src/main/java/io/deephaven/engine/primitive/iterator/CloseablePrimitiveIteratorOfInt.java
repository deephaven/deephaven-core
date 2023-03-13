package io.deephaven.engine.primitive.iterator;

import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code int}.
 */
public interface CloseablePrimitiveIteratorOfInt
        extends PrimitiveIterator.OfInt, CloseablePrimitiveIterator<Integer, IntConsumer> {

    /**
     * Create a {@link IntStream} over the remaining elements of this CloseablePrimitiveIteratorOfInt. Closing the
     * result will close this CloseablePrimitiveIteratorOfInt.
     *
     * @return A {@link IntStream} over the remaining contents of this iterator
     */
    default IntStream intStream() {
        return StreamSupport.intStream(
                Spliterators.spliteratorUnknownSize(
                        this,
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
}
