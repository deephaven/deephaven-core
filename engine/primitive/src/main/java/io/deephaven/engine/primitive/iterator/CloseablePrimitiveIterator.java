//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.iterator;

import io.deephaven.util.SafeCloseable;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Interface for {@link SafeCloseable closeable} {@link PrimitiveIterator primitive iterators}.
 */
public interface CloseablePrimitiveIterator<TYPE, TYPE_CONSUMER>
        extends PrimitiveIterator<TYPE, TYPE_CONSUMER>, CloseableIterator<TYPE> {
}
