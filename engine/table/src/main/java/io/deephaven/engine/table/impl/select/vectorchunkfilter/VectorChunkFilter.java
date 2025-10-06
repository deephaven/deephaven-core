//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.vectorchunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * A ChunkFilter that wraps another ChunkFilter, passing each element of an array or vector to the inner filter. If the
 * inner filter matches any element of the Vector or array, then this filter considers the row with the array or vector
 * to be a match.
 */
abstract class VectorChunkFilter implements ChunkFilter, SafeCloseable {
    final VectorComponentFilterWrapper vectorComponentFilterWrapper;
    final WritableIntChunk<ChunkPositions> srcPos;
    final WritableBooleanChunk<Values> matched;
    final int chunkSize;

    /**
     * Create a VectorChunkFilter.
     *
     * @param vectorComponentFilterWrapper the VectorComponentFilterWrapper that is providing the wrapped chunk filter
     * @param chunkSize the number of elements to consider at once (i.e. how many inner elements are passed to the
     *        wrapped chunkfilter at a time)
     */
    VectorChunkFilter(final VectorComponentFilterWrapper vectorComponentFilterWrapper, final int chunkSize) {
        this.vectorComponentFilterWrapper = vectorComponentFilterWrapper;
        this.chunkSize = chunkSize;
        srcPos = WritableIntChunk.makeWritableChunk(chunkSize);
        matched = WritableBooleanChunk.makeWritableChunk(chunkSize);
    }

    @Override
    public void filter(final Chunk<? extends Values> values, final LongChunk<OrderedRowKeys> keys,
            final WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);

        final IntConsumer addResult = mp -> results.add(keys.get(mp));
        doFilter(values, (x) -> true, addResult);
    }

    @Override
    public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        results.setSize(values.size());
        results.fillWithValue(0, results.size(), false);
        return filterToBooleanChunk(values, results, (x) -> true);
    }

    @Override
    public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        return filterToBooleanChunk(values, results, (x) -> {
            final boolean shouldFilter = results.get(x);
            if (shouldFilter) {
                // set the value to false for now, if we have a match, we will reset it to true
                results.set(x, false);
            }
            return shouldFilter;
        });
    }

    private int filterToBooleanChunk(Chunk<? extends Values> values, WritableBooleanChunk<Values> results,
            final IntPredicate predicate) {
        final MutableInt trueCount = new MutableInt(0);
        final IntConsumer setMatch = mp -> {
            results.set(mp, true);
            trueCount.increment();
        };
        doFilter(values, predicate, setMatch);
        return trueCount.get();
    }

    /**
     * The method that implementing classes must override to filter a chunk of vectors
     * 
     * @param values the chunk of vectors
     * @param applyFilter a predicate indicating whether we should apply a filter to the given chunk position. If the
     *        predicate returns true, then the vector at that position is is unwrapped into chunks of values; and if any
     *        of them match matchConsumer is called with that position. If the predicate returns false, then the vector
     *        is skipped and matchConsumer is never called.
     * @param matchConsumer a callback that is invoked with the position of each filtered Vector that has a matching
     *        element
     */
    abstract void doFilter(final Chunk<? extends Values> values,
            final IntPredicate applyFilter,
            final IntConsumer matchConsumer);

    /**
     * When the {@link #doFilter(Chunk, IntPredicate, IntConsumer)} method fills a chunk (or comes to the end of the
     * input chunk), then it should invoke doFilter to actually complete the filtering of the chunk.
     *
     * @param matchConsumer a callback that is invoked with the position of each filtered Vector that has a matching
     *        element
     * @param pos the last position in temporaryValues that has been filled
     * @param temporaryValues a chunk of element values, to be passed to the wrapped chunk filter
     * @return the last position in this chunk that matched, or -1 if no value matched
     */
    int flushMatches(final IntConsumer matchConsumer,
            final int pos,
            final WritableChunk<? extends Values> temporaryValues) {
        int lastMatch = -1;
        if (pos == 0) {
            return lastMatch;
        }

        temporaryValues.setSize(pos);
        matched.setSize(pos);
        vectorComponentFilterWrapper.chunkFilter.filter(temporaryValues, matched);
        temporaryValues.setSize(chunkSize);
        for (int mm = 0; mm < pos; ++mm) {
            final int thisPos = srcPos.get(mm);
            if (thisPos != lastMatch && matched.get(mm)) {
                matchConsumer.accept(thisPos);
                lastMatch = thisPos;
            }
        }

        return lastMatch;
    }

    @Override
    public void close() {
        srcPos.close();
        matched.close();
    }
}
