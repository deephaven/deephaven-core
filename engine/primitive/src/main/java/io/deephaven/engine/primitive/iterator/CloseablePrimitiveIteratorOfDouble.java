/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CloseablePrimitiveIteratorOfInt and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.primitive.iterator;

import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.DoubleConsumer;
import java.util.stream.DoubleStream;
import java.util.stream.StreamSupport;

/**
 * {@link CloseablePrimitiveIterator Closeable primitive iterator} over elements of type {@code double}.
 */
public interface CloseablePrimitiveIteratorOfDouble
        extends PrimitiveIterator.OfDouble, CloseablePrimitiveIterator<Double, DoubleConsumer> {

    /**
     * Create a {@link DoubleStream} over the remaining elements of this CloseablePrimitiveIteratorOfDouble. Closing the
     * result will close this CloseablePrimitiveIteratorOfDouble.
     *
     * @return A {@link DoubleStream} over the remaining contents of this iterator
     */
    default DoubleStream doubleStream() {
        return StreamSupport.doubleStream(
                Spliterators.spliteratorUnknownSize(
                        this,
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
}
