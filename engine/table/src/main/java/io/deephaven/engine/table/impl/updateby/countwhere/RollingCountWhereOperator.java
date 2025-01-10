//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.countwhere;

import io.deephaven.base.ringbuffer.ByteRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.deephaven.util.QueryConstants.*;

// TODO: this operator can alomost certainly be re-used with minor changes for the cumulative version as well. Should
// probably be called CountWhereOperator, even though that sorta collides with AggBy operator name.

public class RollingCountWhereOperator extends BaseLongUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 512;

    /**
     * Store the input column names and type information.
     */
    private final String[] inputColumnNames;
    private final ChunkType[] inputChunkTypes;
    private final Class<?>[] inputColumnTypes;
    private final Class<?>[] inputComponentTypes;

    /**
     * The raw filters to apply to the data.
     */
    private final CountFilter[] filters;

    /**
     * Whether we need to create and maintain a chunk source table?
     */
    private final boolean chunkSourceTableRequired;

    private class Context extends BaseLongUpdateByOperator.Context {
        /**
         * The chunk sources that populate the chunkSourceTable for this context.
         */
        private final ChunkColumnSource<?>[] chunkColumnSources;
        /**
         * A table composed of chunk sources for generic where filters for this context.
         */
        private final Table chunkSourceTable;
        /**
         * Contains the results of the filters as a boolean chunk, where true indicates a row passed all filters.
         */
        private final WritableBooleanChunk<Values> resultsChunk;
        /**
         * The ring buffer representing the filter output for the values currently in the window.
         */
        private final ByteRingBuffer buffer;
        /**
         * The raw filters to apply to the data, these may diverge from the operator filters.
         */
        private final CountFilter[] contextFilters;
        /**
         * The chunk data from the recorders to be used as input to the filters.
         */
        private final Chunk<? extends Values>[][] filterChunks;
        /**
         * ConditionFilters need a context to store intermediate results.
         */
        final ConditionFilter.FilterKernel.Context[] conditionFilterContexts;

        /**
         * Store the SafeCloseable items returned by the WhereFilter#beginOperation() calls for clean up when the
         * context is released.
         */
        final SafeCloseable[] filterOperationContexts;

        @SuppressWarnings("unused")
        private Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);

            // Create a new chunk source table for this context
            if (chunkSourceTableRequired) {
                final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
                chunkColumnSources = new ChunkColumnSource<?>[inputColumnNames.length];
                for (int i = 0; i < inputColumnNames.length; i++) {
                    chunkColumnSources[i] =
                            ChunkColumnSource.make(inputChunkTypes[i], inputColumnTypes[i], inputComponentTypes[i]);
                    columnSourceMap.put(inputColumnNames[i], chunkColumnSources[i]);
                }
                chunkSourceTable = new QueryTable(RowSetFactory.empty().toTracking(), columnSourceMap);
            } else {
                chunkColumnSources = null;
                chunkSourceTable = null;
            }

            // Store the operator's chunk and condition filters, but create new WhereFilters. This is required because
            // the operator WhereFilters were initialized against a dummy table.
            contextFilters = new CountFilter[filters.length];
            filterOperationContexts = new SafeCloseable[filters.length];

            for (int fi = 0; fi < filters.length; fi++) {
                final CountFilter filter = filters[fi];
                if (filter.whereFilter != null) {
                    Require.neqNull(chunkSourceTable, "chunkSourceTable");
                    final WhereFilter copiedFilter = filter.whereFilter.copy();

                    // Initialize the copied filter against this context chunk source table.
                    copiedFilter.init(chunkSourceTable.getDefinition());

                    // noinspection resource
                    filterOperationContexts[fi] = copiedFilter.beginOperation(chunkSourceTable);

                    // Use this WhereFilter in chunk processing
                    contextFilters[fi] = new CountFilter(copiedFilter, filter.inputColumnIndices);
                } else {
                    contextFilters[fi] = filter;
                }
            }

            conditionFilterContexts = new ConditionFilter.FilterKernel.Context[filters.length];
            for (int ii = 0; ii < filters.length; ii++) {
                if (filters[ii].conditionFilter != null) {
                    conditionFilterContexts[ii] = filters[ii].conditionFilter.getContext(influencerChunkSize);
                }
            }

            // noinspection unchecked
            filterChunks = new Chunk[filters.length][];
            for (int ii = 0; ii < filters.length; ii++) {
                final CountFilter filter = filters[ii];
                // noinspection unchecked
                filterChunks[ii] = new Chunk[filter.inputColumnIndices.length];
            }

            resultsChunk = WritableBooleanChunk.makeWritableChunk(influencerChunkSize);
            buffer = new ByteRingBuffer(BUFFER_INITIAL_CAPACITY, true);

            curVal = 0;
        }

        @Override
        public void reset() {
            buffer.clear();
            curVal = 0;
        }

        @Override
        public void close() {
            // TODO: single SafeClosableList for all these items
            SafeCloseableArray.close(conditionFilterContexts);
            SafeCloseableArray.close(filterOperationContexts);
            resultsChunk.close();
            outputValues.close();
            outputFillContext.close();
        }

        @Override
        public void setValueChunks(@NotNull Chunk<? extends Values>[] valueChunks) {
            // Update the chunk source table chunks if needed.
            if (chunkSourceTableRequired) {
                for (int i = 0; i < inputColumnNames.length; i++) {
                    chunkColumnSources[i].clear();
                    chunkColumnSources[i].addChunk((WritableChunk<? extends Values>) valueChunks[i]);
                }
            }

            // Assign the filter input chunks from the value chunks.
            for (int fi = 0; fi < filters.length; fi++) {
                final CountFilter filter = filters[fi];

                for (int ri = 0; ri < filter.inputColumnIndices.length; ri++) {
                    filterChunks[fi][ri] = valueChunks[filter.inputColumnIndices[ri]];
                }
            }
        }

        /**
         * Do the work of applying the filters against the input data and assigning true to the result chunk where all
         * filters pass, false otherwise.
         */
        private void applyFilters(
                final Chunk<? extends Values>[] inputChunks,
                final int chunkSize) {

            // Use the filters to populate a boolean buffer with the filter results.
            boolean initialized = false;
            WritableRowSet remainingRows = null;
            final RowSet flatRowSet = RowSetFactory.flat(chunkSize);

            // We must apply the filters in the order they were given.
            for (int fi = 0; fi < contextFilters.length; fi++) {
                final CountFilter filter = contextFilters[fi];
                final Chunk<? extends Values>[] valueChunks = filterChunks[fi];
                final ConditionFilter.FilterKernel.Context conditionalFilterContext = conditionFilterContexts[fi];

                if (filter.chunkFilter != null) {
                    if (!initialized) {
                        // Chunk filters only have one input.
                        filter.chunkFilter.filter(valueChunks[0], resultsChunk);
                        initialized = true;
                    } else {
                        filter.chunkFilter.filterAnd(valueChunks[0], resultsChunk);
                    }
                    continue;
                } else if (filter.conditionFilter != null) {
                    if (!initialized) {
                        filter.conditionFilter.filter(conditionalFilterContext, valueChunks, chunkSize, resultsChunk);
                        initialized = true;
                    } else {
                        filter.conditionFilter.filterAnd(conditionalFilterContext, valueChunks, chunkSize,
                                resultsChunk);
                    }
                    continue;
                }

                if (remainingRows == null) {
                    // This is the first WhereFilter to run, initialize the remainingRows RowSet
                    remainingRows = initialized
                            ? buildFromBooleanChunk(resultsChunk, chunkSize)
                            : RowSetFactory.flat(chunkSize);
                }
                try (final RowSet ignored = remainingRows) {
                    remainingRows = filter.whereFilter.filter(remainingRows, flatRowSet, chunkSourceTable, false);
                }
                initialized = true;
            }

            try (final RowSet ignored = remainingRows; final RowSet ignored2 = flatRowSet) {
                if (remainingRows != null) {
                    // WhereFilters were used, so gather the info from remainingRows
                    resultsChunk.fillWithValue(0, chunkSize, false);
                    remainingRows.forAllRowKeyRanges(
                            (start, end) -> resultsChunk.fillWithValue((int) start, (int) end - (int) start + 1,
                                    true));
                }
            }
        }

        @Override
        public void accumulateRolling(
                @NotNull final RowSequence inputKeys,
                @NotNull final Chunk<? extends Values>[] influencerValueChunkArr,
                @Nullable final LongChunk<OrderedRowKeys> affectedPosChunk,
                @Nullable final LongChunk<OrderedRowKeys> influencerPosChunk,
                @NotNull final IntChunk<? extends Values> pushChunk,
                @NotNull final IntChunk<? extends Values> popChunk,
                final int len) {

            setValueChunks(influencerValueChunkArr);
            setPosChunks(affectedPosChunk, influencerPosChunk);

            applyFilters(influencerValueChunkArr, len);

            int pushIndex = 0;

            // chunk processing
            for (int ii = 0; ii < len; ii++) {
                final int pushCount = pushChunk.get(ii);
                final int popCount = popChunk.get(ii);

                if (pushCount == NULL_INT) {
                    writeNullToOutputChunk(ii);
                    continue;
                }

                // pop for this row
                if (popCount > 0) {
                    pop(popCount);
                }

                // push for this row
                if (pushCount > 0) {
                    push(pushIndex, pushCount);
                    pushIndex += pushCount;
                }

                // write the results to the output chunk
                writeToOutputChunk(ii);
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        protected void push(int pos, int count) {
            // Push from the pre-computed results chunk into the buffer.
            buffer.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final boolean val = resultsChunk.get(pos + ii);
                if (val) {
                    curVal++;
                    buffer.add((byte) 1);
                } else {
                    buffer.add((byte) 0);
                }
            }
        }

        @Override
        protected void pop(int count) {
            Assert.geq(buffer.size(), "buffer.size()", count);

            for (int ii = 0; ii < count; ii++) {
                final byte val = buffer.removeUnsafe();

                if (val == 1) {
                    curVal--;
                }
            }
        }

        @Override
        public void accumulateCumulative(
                @NotNull final RowSequence inputKeys,
                @NotNull final Chunk<? extends Values>[] valueChunkArr,
                @Nullable final LongChunk<? extends Values> tsChunk,
                final int len) {
            throw new UnsupportedOperationException(
                    "RollingCountWhereOperator is not supported in cumulative operations.");
        }
    }

    /**
     * Build a RowSet from a boolean chunk, including only the indices that are true.
     */
    private static WritableRowSet buildFromBooleanChunk(final BooleanChunk<Values> values, final int chunkSize) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (int ii = 0; ii < chunkSize; ii++) {
            if (values.get(ii)) {
                builder.appendKey(ii);
            }
        }
        return builder.build();
    }

    /**
     * Create a new RollingCountWhereOperator.
     *
     * @param pair Contains the output column name as a MatchPair
     * @param affectingColumns The names of the columns that when changed would affect this formula output
     * @param timestampColumnName The name of the column containing timestamps for time-based calculations (or null when
     *        not time-based)
     * @param reverseWindowScaleUnits The size of the reverse window in ticks (or nanoseconds when time-based)
     * @param forwardWindowScaleUnits The size of the forward window in ticks (or nanoseconds when time-based)
     * @param filters the filters to apply to the input columns
     * @param inputColumnNames The names of the key columns to be used as inputs
     * @param inputChunkTypes The chunk types for each input column
     * @param inputColumnTypes The data types for each input column
     * @param inputComponentTypes The component types for each input column
     */
    public RollingCountWhereOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            final CountFilter[] filters,
            final String[] inputColumnNames,
            final ChunkType[] inputChunkTypes,
            final Class<?>[] inputColumnTypes,
            final Class<?>[] inputComponentTypes,
            final boolean chunkSourceTableRequired) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        this.filters = filters;
        this.inputColumnNames = inputColumnNames;
        this.inputChunkTypes = inputChunkTypes;
        this.inputColumnTypes = inputColumnTypes;
        this.inputComponentTypes = inputComponentTypes;
        this.chunkSourceTableRequired = chunkSourceTableRequired;
    }

    @Override
    public UpdateByOperator copy() {
        return new RollingCountWhereOperator(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                filters,
                inputColumnNames,
                inputChunkTypes,
                inputColumnTypes,
                inputComponentTypes,
                chunkSourceTableRequired);
    }

    /**
     * This must be overridden to provide the input column chunks that will be needed for computation.
     */
    @Override
    @NotNull
    protected String[] getInputColumnNames() {
        return ArrayUtils.addAll(inputColumnNames);
    }

    @Override
    public UpdateByOperator.@NotNull Context makeUpdateContext(int affectedChunkSize, int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }
}
