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
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.select.AbstractConditionFilter;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.ExposesChunkFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.deephaven.util.QueryConstants.*;

public class CountWhereOperator extends BaseLongUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 512;

    /**
     * Store the input column names and type information.
     */
    private final String[] inputColumnNames;
    /**
     * Store representative sources for reinterpreted input and original columns, will not hold data.
     */
    private final ColumnSource<?>[] originalSources;
    private final ColumnSource<?>[] reinterpretedSources;
    /**
     * The raw filters to apply to the data.
     */
    private final CountFilter[] filters;

    /**
     * Whether we need to create and maintain a chunk source table.
     */
    private final boolean chunkSourceTableRequired;

    /**
     * Whether we need to produce original-typed chunks for condition and chunk filters.
     */
    private final boolean originalChunksRequired;

    /**
     * Holder class to hold filter objects and the input column indices they apply to.
     */
    public static class CountFilter {
        final ChunkFilter chunkFilter;
        final AbstractConditionFilter.Filter conditionFilter;
        final WhereFilter whereFilter;
        final int[] inputColumnIndices;

        public CountFilter(ChunkFilter chunkFilter, int[] inputColumnIndices) {
            this.chunkFilter = chunkFilter;
            this.conditionFilter = null;
            this.whereFilter = null;
            this.inputColumnIndices = inputColumnIndices;
        }

        public CountFilter(AbstractConditionFilter.Filter conditionFilter, int[] inputColumnIndices) {
            this.chunkFilter = null;
            this.conditionFilter = conditionFilter;
            this.whereFilter = null;
            this.inputColumnIndices = inputColumnIndices;
        }

        public CountFilter(WhereFilter whereFilter, int[] inputColumnIndices) {
            this.chunkFilter = null;
            this.conditionFilter = null;
            this.whereFilter = whereFilter;
            this.inputColumnIndices = inputColumnIndices;
        }

        public ChunkFilter chunkFilter() {
            return chunkFilter;
        }

        public AbstractConditionFilter.Filter conditionFilter() {
            return conditionFilter;
        }

        public WhereFilter whereFilter() {
            return whereFilter;
        }

        public int[] inputColumnIndices() {
            return inputColumnIndices;
        }

        /**
         * Create CountFilters from WhereFilter array, initializing the filters against the provided table.
         */
        public static CountFilter[] createCountFilters(
                final WhereFilter[] filters,
                final Table inputTable,
                final List<int[]> filterInputIndices) {

            // Create the internal filters
            final List<CountFilter> filterList = new ArrayList<>();
            boolean forcedWhereFilter = false;
            for (int fi = 0; fi < filters.length; fi++) {
                final WhereFilter filter = filters[fi];
                final CountFilter countFilter;
                if (!forcedWhereFilter && filter instanceof ConditionFilter) {
                    final ConditionFilter conditionFilter = (ConditionFilter) filter;
                    if (conditionFilter.hasVirtualRowVariables()) {
                        throw new UnsupportedOperationException(
                                "UpdateBy CountWhere operator does not support refreshing filters");
                    }
                    try {
                        countFilter = new CountFilter(conditionFilter.getFilter(inputTable, RowSetFactory.empty()),
                                filterInputIndices.get(fi));
                    } catch (final Exception e) {
                        throw new IllegalArgumentException(
                                "Error creating condition filter in UpdateBy CountWhere Operator", e);
                    }
                } else if (!forcedWhereFilter && filter instanceof ExposesChunkFilter
                        && ((ExposesChunkFilter) filter).chunkFilter().isPresent()) {
                    final Optional<ChunkFilter> chunkFilter = ((ExposesChunkFilter) filter).chunkFilter();
                    countFilter = new CountFilter(chunkFilter.get(), filterInputIndices.get(fi));
                } else {
                    try (final SafeCloseable ignored = filter.beginOperation(inputTable)) {
                        countFilter = new CountFilter(filter, filterInputIndices.get(fi));
                    }
                    forcedWhereFilter = true;
                }
                filterList.add(countFilter);
            }

            return filterList.toArray(CountFilter[]::new);
        }
    }

    private class Context extends BaseLongUpdateByOperator.Context {
        /**
         * The chunk sources that populate the chunkSourceTable for this context.
         */
        private final ChunkColumnSource<?>[] chunkColumnSources;
        /**
         * The chunk sources that help populate the original-typed chunks for this context.
         */
        private final ColumnSource<?>[] contextOriginalColumnSources;
        /**
         * Fill contexts to help convert back to original-typed chunks.
         */
        private final ChunkSource.FillContext[] originalFillContexts;
        /**
         * Chunks to store original-typed values for condition and chunk filters.
         */
        private final WritableChunk<? super Values>[] originalValueChunks;

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

        /**
         * A list of all items that need to be closed when the context is released.
         */
        final SafeCloseableList closeableList;

        @SuppressWarnings("unused")
        private Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);

            closeableList = new SafeCloseableList();

            if (originalChunksRequired) {
                originalFillContexts = new ChunkSource.FillContext[inputColumnNames.length];
                // noinspection unchecked
                originalValueChunks = new WritableChunk[inputColumnNames.length];
            } else {
                originalFillContexts = null;
                originalValueChunks = null;
            }

            // Create a new chunk source table for this context
            if (chunkSourceTableRequired) {
                contextOriginalColumnSources = new ColumnSource<?>[inputColumnNames.length];
                final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
                chunkColumnSources = new ChunkColumnSource<?>[inputColumnNames.length];
                for (int i = 0; i < inputColumnNames.length; i++) {
                    final ColumnSource<?> inputSource = reinterpretedSources[i];
                    chunkColumnSources[i] = ChunkColumnSource.make(inputSource.getChunkType(), inputSource.getType(),
                            inputSource.getComponentType());
                    if (originalSources[i] != null) {
                        // This column needs to be interpreted back to the original type.
                        final ColumnSource<?> toOriginal =
                                ReinterpretUtils.convertToOriginalType(originalSources[i], chunkColumnSources[i]);
                        // Put the re-reinterpreted column source in the map
                        columnSourceMap.put(inputColumnNames[i], toOriginal);
                        // Create chunk and fill context for original-typed converted values
                        originalFillContexts[i] = toOriginal.makeFillContext(influencerChunkSize);
                        closeableList.add(originalFillContexts[i]);
                        originalValueChunks[i] = toOriginal.getChunkType().makeWritableChunk(influencerChunkSize);
                        closeableList.add(originalValueChunks[i]);
                        contextOriginalColumnSources[i] = toOriginal;
                    } else {
                        columnSourceMap.put(inputColumnNames[i], chunkColumnSources[i]);
                    }
                }
                chunkSourceTable = new QueryTable(RowSetFactory.empty().toTracking(), columnSourceMap);
            } else {
                chunkColumnSources = null;
                chunkSourceTable = null;
                contextOriginalColumnSources = null;
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
                    closeableList.add(filterOperationContexts[fi]);

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
                    closeableList.add(conditionFilterContexts[ii]);
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
            closeableList.add(resultsChunk);

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
            super.close();
            closeableList.close();
        }

        /**
         * Assign the input chunks to the correct filters, maybe reinterpreting the chunks to the original type.
         */
        private void assignInputChunksToFilters(
                @NotNull Chunk<? extends Values>[] valueChunks,
                final int influencerChunkSize) {

            try (final RowSet influencerRs = originalChunksRequired ? RowSetFactory.flat(influencerChunkSize) : null) {
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

                    for (int fci = 0; fci < filter.inputColumnIndices.length; fci++) {
                        final int inputIndex = filter.inputColumnIndices[fci];
                        if (originalChunksRequired && contextOriginalColumnSources[inputIndex] != null) {
                            // Filling from contextOriginalColumnSources[] will produce original typed version of the
                            // input chunk data since we seeded the underlying ChunkColumnSource with the input chunk.
                            contextOriginalColumnSources[inputIndex].fillChunk(
                                    originalFillContexts[inputIndex],
                                    originalValueChunks[inputIndex],
                                    influencerRs);
                            // noinspection unchecked
                            filterChunks[fi][fci] = (Chunk<? extends Values>) originalValueChunks[inputIndex];
                        } else {
                            filterChunks[fi][fci] = valueChunks[inputIndex];
                        }
                    }
                }
            }
        }

        /**
         * Do the work of applying the filters against the input data and assigning true to the result chunk where all
         * filters pass, false otherwise.
         */
        private void applyFilters(final int chunkSize) {
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
                } else if (filter.conditionFilter != null) {
                    if (!initialized) {
                        filter.conditionFilter.filter(conditionalFilterContext, valueChunks, chunkSize, resultsChunk);
                        initialized = true;
                    } else {
                        filter.conditionFilter.filterAnd(conditionalFilterContext, valueChunks, chunkSize,
                                resultsChunk);
                    }
                } else {
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
                final int affectedCount,
                final int influencerCount) {

            assignInputChunksToFilters(influencerValueChunkArr, influencerCount);
            setPosChunks(affectedPosChunk, influencerPosChunk);

            applyFilters(influencerCount);

            int pushIndex = 0;

            // chunk processing
            for (int ii = 0; ii < affectedCount; ii++) {
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
        public void accumulateCumulative(
                @NotNull final RowSequence inputKeys,
                @NotNull final Chunk<? extends Values>[] valueChunkArr,
                @Nullable final LongChunk<? extends Values> tsChunk,
                final int len) {

            assignInputChunksToFilters(valueChunkArr, len);
            applyFilters(len);

            // chunk processing
            for (int ii = 0; ii < len; ii++) {
                push(ii, 1);
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
     * Create a new CountWhereOperator for rolling / windowed operations.
     *
     * @param pair Contains the output column name as a MatchPair
     * @param affectingColumns The names of the columns that when changed would affect this formula output
     * @param timestampColumnName The name of the column containing timestamps for time-based calculations (or null when
     *        not time-based)
     * @param reverseWindowScaleUnits The size of the reverse window in ticks (or nanoseconds when time-based)
     * @param forwardWindowScaleUnits The size of the forward window in ticks (or nanoseconds when time-based)
     * @param filters the filters to apply to the input columns
     * @param inputColumnNames The names of the key columns to be used as inputs
     * @param originalSources Representative original sources for the input columns; stores type information and helps
     *        convert input data chunks but does not hold any actual data
     * @param reinterpretedSources Representative reinterpreted sources for input columns; stores type information but
     *        does not hold any actual data
     * @param chunkSourceTableRequired Whether we need to create and maintain a chunk source table
     * @param originalChunksRequired Whether we need to produce original-typed chunks for condition and chunk filters
     */
    public CountWhereOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            final CountFilter[] filters,
            final String[] inputColumnNames,
            final ColumnSource<?>[] originalSources,
            final ColumnSource<?>[] reinterpretedSources,
            final boolean chunkSourceTableRequired,
            final boolean originalChunksRequired) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        this.filters = filters;
        this.inputColumnNames = inputColumnNames;
        this.originalSources = originalSources;
        this.reinterpretedSources = reinterpretedSources;
        this.chunkSourceTableRequired = chunkSourceTableRequired;
        this.originalChunksRequired = originalChunksRequired;
    }

    /**
     * Create a new CountWhereOperator for cumulative operations.
     *
     * @param pair Contains the output column name as a MatchPair
     * @param filters the filters to apply to the input columns
     * @param originalSources Representative original sources for the input columns; stores type information and helps
     *        convert input data chunks but does not hold any actual data
     * @param reinterpretedSources Representative reinterpreted sources for input columns; stores type information but
     *        does not hold any actual data
     * @param chunkSourceTableRequired Whether we need to create and maintain a chunk source table
     * @param originalChunksRequired Whether we need to produce original-typed chunks for condition and chunk filters
     */
    public CountWhereOperator(
            @NotNull final MatchPair pair,
            final CountFilter[] filters,
            final String[] inputColumnNames,
            final ColumnSource<?>[] originalSources,
            final ColumnSource<?>[] reinterpretedSources,
            final boolean chunkSourceTableRequired,
            final boolean originalChunksRequired) {
        super(pair, inputColumnNames, null, 0, 0, false);
        this.filters = filters;
        this.inputColumnNames = inputColumnNames;
        this.originalSources = originalSources;
        this.reinterpretedSources = reinterpretedSources;
        this.chunkSourceTableRequired = chunkSourceTableRequired;
        this.originalChunksRequired = originalChunksRequired;
    }

    @Override
    public UpdateByOperator copy() {
        if (!isWindowed) {
            return new CountWhereOperator(
                    pair,
                    filters,
                    inputColumnNames,
                    originalSources,
                    reinterpretedSources,
                    chunkSourceTableRequired,
                    originalChunksRequired);
        }
        return new CountWhereOperator(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                filters,
                inputColumnNames,
                originalSources,
                reinterpretedSources,
                chunkSourceTableRequired,
                originalChunksRequired);
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
