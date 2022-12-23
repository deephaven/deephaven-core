/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.stream.Stream;

/**
 * {@link ChunkSource} that produces rollup {@link AggregationRowLookup} keys from multiple {@link ColumnSource
 * sources}.
 */
final class RollupRowLookupKeySource implements DefaultChunkSource.WithPrev<Values> {

    static final Object ROOT_NODE_KEY = new Object();

    private final ColumnSource<Integer> depthSource;
    private final ColumnSource<?>[] groupByValueSources;

    /**
     * Construct a new RollupRowLookupKeySource backed by the supplied column sources.
     *
     * @param depthSource A source of {@code int} widths that determines how many of the {@code groupByValueSources}
     *        should have their values included in the result
     * @param groupByValueSources Sources corresponding to the aggregation group-by values in the rollup
     */
    RollupRowLookupKeySource(
            @NotNull final ColumnSource<Integer> depthSource,
            @NotNull final ColumnSource<?>... groupByValueSources) {
        this.depthSource = depthSource;
        this.groupByValueSources = Stream.of(groupByValueSources)
                .map(ReinterpretUtils::maybeConvertToPrimitive).toArray(ColumnSource[]::new);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    public void fillChunk(
            @NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        fillChunkInternal(context, destination, rowSequence, false);
    }

    public void fillPrevChunk(
            @NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        fillChunkInternal(context, destination, rowSequence, true);
    }

    private void fillChunkInternal(
            @NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence,
            final boolean usePrev) {
        if (rowSequence.isEmpty()) {
            destination.setSize(0);
            return;
        }
        final FillContext fc = (FillContext) context;
        final IntChunk<? extends Values> depths = usePrev
                ? depthSource.getPrevChunk(fc.depthContext, rowSequence).asIntChunk()
                : depthSource.getChunk(fc.depthContext, rowSequence).asIntChunk();
        final int maxKeyWidth = getMaxKeyWidth(depths);
        final ObjectChunk<?, ? extends Values>[] groupByValues =
                getGroupByValuesChunks(fc, rowSequence, usePrev, maxKeyWidth);
        fillFromGroupByValues(rowSequence, depths, groupByValues, destination.asWritableObjectChunk());
    }

    private static int getMaxKeyWidth(@NotNull final IntChunk<? extends Values> depths) {
        final int size = depths.size();
        int maxKeyWidth = 0;
        for (int kwi = 0; kwi < size; ++kwi) {
            final int keyWidth = depths.get(kwi) - 1;
            if (keyWidth > maxKeyWidth) {
                maxKeyWidth = keyWidth;
            }
        }
        return maxKeyWidth;
    }

    private ObjectChunk<?, ? extends Values>[] getGroupByValuesChunks(
            @NotNull final FillContext fillContext,
            @NotNull final RowSequence rowSequence,
            final boolean usePrev,
            final int maxKeyWidth) {
        // noinspection unchecked
        final ObjectChunk<?, ? extends Values>[] groupByValuesChunks = new ObjectChunk[maxKeyWidth];
        for (int ci = 0; ci < maxKeyWidth; ++ci) {
            groupByValuesChunks[ci] = (usePrev
                    ? groupByValueSources[ci].getPrevChunk(fillContext.groupByValueContexts[ci], rowSequence)
                    : groupByValueSources[ci].getChunk(fillContext.groupByValueContexts[ci], rowSequence))
                            .asObjectChunk();
        }
        return groupByValuesChunks;
    }

    private void fillFromGroupByValues(
            @NotNull final RowSequence rowSequence,
            @NotNull final IntChunk<? extends Values> depths,
            @NotNull final ObjectChunk<?, ? extends Values>[] groupByValues,
            @NotNull final WritableObjectChunk<Object, ? super Values> destination) {
        final int size = rowSequence.intSize();
        destination.setSize(size);
        for (int ri = 0; ri < size; ++ri) {
            final int depth = depths.get(ri);
            if (depth == 0) {
                destination.set(ri, ROOT_NODE_KEY);
            } else if (depth == 1) {
                destination.set(ri, AggregationRowLookup.EMPTY_KEY);
            } else if (depth == 2) {
                destination.set(ri, groupByValues[0].get(ri));
            } else {
                final Object[] columnValues = new Object[depth - 1];
                for (int ci = 0; ci < depth - 1; ++ci) {
                    columnValues[ci] = groupByValues[ci].get(ri);
                }
                destination.set(ri, columnValues);
            }
        }
    }

    private static class FillContext implements ChunkSource.FillContext {

        private final ChunkSource.GetContext depthContext;
        private final ChunkSource.GetContext[] groupByValueContexts;

        private FillContext(
                final int chunkCapacity,
                @NotNull final ColumnSource<Integer> depthSource,
                @NotNull final ColumnSource[] groupByValueSources,
                final SharedContext sharedContext) {
            depthContext = depthSource.makeGetContext(chunkCapacity, sharedContext);
            groupByValueContexts = Stream.of(groupByValueSources)
                    .map(cs -> cs.makeGetContext(chunkCapacity, sharedContext))
                    .toArray(ChunkSource.GetContext[]::new);
        }

        @Override
        public void close() {
            depthContext.close();
            SafeCloseable.closeArray(groupByValueContexts);
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext(chunkCapacity, depthSource, groupByValueSources, sharedContext);
    }
}
