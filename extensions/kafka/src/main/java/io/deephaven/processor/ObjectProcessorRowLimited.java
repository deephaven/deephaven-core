/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;

import java.util.List;

/**
 * A specialization of {@link ObjectProcessor} that provides row-limited guarantees around the implementation of
 * {@link #processAll(ObjectChunk, List)}.
 *
 * <p>
 * For use-cases that are naturally performed one row at a time, callers are encouraged to extend
 * {@link ObjectProcessorRowBase}. For use-cases where the implementation can be columnar but callers would like to add
 * a row limit, callers are encouraged to create a columnar implementation against {@link ObjectProcessor} and use
 * {@link #of(ObjectProcessor, int)}.
 *
 * @param <T> the object type
 */
public interface ObjectProcessorRowLimited<T> extends ObjectProcessor<T> {

    /**
     * Creates or returns a row-limited implementation. If {@code delegate} is already limited more than
     * {@code rowLimit}, {@code delegate} is returned. Otherwise, a row-limited implementation is created that wraps
     * {@code delegate} and invokes {@link #processAll(ObjectChunk, List) delegate#processAll} with {@code rowLimit}
     * sized out chunks, except for the last invocation which may have size less-than {@code rowLimit}.
     *
     * <p>
     * Adding a row-limit may be useful in cases where the input objects are "wide". By limiting the number of rows
     * considered at any given time, there may be better opportunity for read caching.
     *
     * @param delegate the delegate
     * @param rowLimit the max chunk size
     * @return the row-limited processor
     * @param <T> the object type
     */
    static <T> ObjectProcessorRowLimited<T> of(ObjectProcessor<T> delegate, int rowLimit) {
        if (delegate instanceof ObjectProcessorRowLimited) {
            final ObjectProcessorRowLimited<T> limited = (ObjectProcessorRowLimited<T>) delegate;
            if (limited.rowLimit() <= rowLimit) {
                // already limited more than rowLimit
                return limited;
            }
            if (limited instanceof ObjectProcessorRowLimitedImpl) {
                // don't want to wrap multiple times, so operate on the inner delegate
                return of(((ObjectProcessorRowLimitedImpl<T>) limited).delegate(), rowLimit);
            }
        }
        return new ObjectProcessorRowLimitedImpl<>(delegate, rowLimit);
    }

    /**
     * A guarantee that {@link #processAll(ObjectChunk, List)} operates on at most row-limit rows at a time.
     *
     * @return the row-limit
     */
    int rowLimit();
}
