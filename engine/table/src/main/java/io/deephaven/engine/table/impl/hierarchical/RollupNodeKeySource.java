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

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.deephaven.engine.table.impl.by.AggregationRowLookup.EMPTY_KEY;

/**
 * {@link ChunkSource} that produces rollup {@link AggregationRowLookup} keys from multiple {@link ColumnSource
 * sources}, with special handling for the "root" node key.
 */
final class RollupNodeKeySource implements DefaultChunkSource.WithPrev<Values> {

    /**
     * Special sentinel node key that marks the root of a {@link RollupTableImpl}, at depth 0.
     */
    static final Object ROOT_NODE_KEY = new Object();

    /**
     * The root's "parent" doesn't really exist, but we pretend that it does, at depth -1.
     */
    static final int ROOT_PARENT_NODE_DEPTH = -1;

    private final ColumnSource<Integer> depthSource;
    private final ColumnSource<?>[] groupByValueSources;

    /**
     * Construct a new RollupNodeKeySource backed by the supplied column sources.
     *
     * @param depthSource A source of {@code int} depths that determines how many of the {@code groupByValueSources}
     *        should have their values included in the result
     * @param groupByValueSources Sources corresponding to the aggregation group-by values in the rollup
     */
    RollupNodeKeySource(
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
        final int maxKeyWidth = getMaxKeyWidth(depths, groupByValueSources.length);
        final ObjectChunk<?, ? extends Values>[] groupByValues =
                getGroupByValuesChunks(fc, rowSequence, usePrev, maxKeyWidth);
        fillFromGroupByValues(rowSequence, depths, groupByValues, destination.asWritableObjectChunk());
    }

    private static int getMaxKeyWidth(@NotNull final IntChunk<? extends Values> depths, final int maxMaxKeyWidth) {
        final int size = depths.size();
        int maxKeyWidth = 0;
        for (int kwi = 0; kwi < size; ++kwi) {
            // No need to special case the root's parent depth, since it's less than 0. For all other node keys, parent
            // depth is key width.
            final int keyWidth = depths.get(kwi);
            if (keyWidth >= maxMaxKeyWidth) {
                return maxMaxKeyWidth;
            }
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
            final int parentDepth = depths.get(ri);
            if (parentDepth < ROOT_PARENT_NODE_DEPTH || parentDepth > groupByValueSources.length) {
                final int usableDepth = Math.min(groupByValueSources.length, parentDepth);
                final int fri = ri;
                throw new IllegalArgumentException(String.format(
                        "Invalid depth %d, maximum for this rollup is %d, partial node key is [%s]",
                        parentDepth, groupByValueSources.length,
                        IntStream.range(0, usableDepth)
                                .mapToObj(ci -> Objects.toString(groupByValues[ci].get(fri)))
                                .collect(Collectors.joining())));
            }
            switch (parentDepth) {
                case ROOT_PARENT_NODE_DEPTH: // Root node
                    destination.set(ri, ROOT_NODE_KEY);
                    break;
                case 0: // Key width 0 (empty Object[])
                    destination.set(ri, EMPTY_KEY);
                    break;
                case 1: // Key width 1 (single Object)
                    destination.set(ri, groupByValues[0].get(ri));
                    break;
                default: // Key width > 1 (Object[] of key column values)
                    final Object[] columnValues = new Object[parentDepth];
                    for (int ci = 0; ci < parentDepth; ++ci) {
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
