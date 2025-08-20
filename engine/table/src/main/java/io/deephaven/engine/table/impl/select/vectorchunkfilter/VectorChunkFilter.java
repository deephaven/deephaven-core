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

abstract class VectorChunkFilter implements ChunkFilter, SafeCloseable {
    final VectorComponentFilterWrapper vectorComponentFilterWrapper;
    final WritableIntChunk<ChunkPositions> srcPos;
    final WritableBooleanChunk<Values> matched;
    final int chunkSize;

    public VectorChunkFilter(final VectorComponentFilterWrapper vectorComponentFilterWrapper, final int chunkSize) {
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
        return filterToBooleanChunk(values, results, (x) -> true);
    }

    @Override
    public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        return filterToBooleanChunk(values, results, results::get);
    }

    private int filterToBooleanChunk(Chunk<? extends Values> values, WritableBooleanChunk<Values> results,
            final IntPredicate predicate) {
        results.setSize(values.size());
        results.fillWithValue(0, results.size(), false);
        final MutableInt trueCount = new MutableInt(0);
        final IntConsumer setMatch = mp -> {
            results.set(mp, true);
            trueCount.increment();
        };
        doFilter(values, predicate, setMatch);
        return trueCount.get();
    }

    abstract void doFilter(final Chunk<? extends Values> values,
            final IntPredicate applyFilter,
            final IntConsumer matchConsumer);

    long flushMatches(final IntConsumer matchConsumer,
            final int pos, final WritableChunk<? extends Values> temporaryValues) {
        long lastMatch = RowSet.NULL_ROW_KEY;
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
