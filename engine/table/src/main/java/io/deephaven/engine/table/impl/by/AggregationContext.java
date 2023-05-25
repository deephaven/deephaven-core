/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.sort.permute.PermuteKernel;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.impl.util.UpdateSizeCalculator;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Encapsulates the operators and inputs for an aggregation operation.
 * <p>
 * Provides utility methods for the ChunkedOperationAggregationHelper to manipulate and interrogate the operators,
 * inputs and outputs.
 */
class AggregationContext {
    /**
     * Our operators.
     */
    final IterativeChunkedAggregationOperator[] operators;

    /**
     * Our input columns (currently one per operator, but that will change to support zero or multiple input operators).
     */
    final ChunkSource.WithPrev<Values>[] inputColumns;

    /**
     * Input column names, parallel to inputColumns.
     */
    final String[][] inputNames;

    /**
     * Does any operator require indices? See {@link IterativeChunkedAggregationOperator#requiresRowKeys()}.
     */
    private final boolean requiresIndices;

    /**
     * Does any operator require runs to be found? See {@link IterativeChunkedAggregationOperator#requiresRunFinds()}.
     */
    private final boolean requiresRunFinds;

    /**
     * Do any operators require inputs.
     */
    private final boolean requiresInputs;

    /**
     * Do all operators handle unchunked indices.
     */
    private final boolean unchunkedIndices;

    /**
     * Our overall result columns.
     */
    private final Map<String, ColumnSource<?>> resultColumns;

    private final AggregationContextTransformer[] transformers;

    /**
     * For a given operator index, the first slot that the column exists in. If the value in index oi is equal to oi,
     * then we will read from this column. If the value is not equal to oi (because it is -1 for a null column or a
     * value less than oi), we will reuse an already read value.
     */
    private final int[] inputSlots;

    AggregationContext(IterativeChunkedAggregationOperator[] operators, String[][] inputNames,
            ChunkSource.WithPrev<Values>[] inputColumns) {
        this(operators, inputNames, inputColumns, null);
    }

    AggregationContext(IterativeChunkedAggregationOperator[] operators, String[][] inputNames,
            ChunkSource.WithPrev<Values>[] inputColumns, AggregationContextTransformer[] transformers) {
        this.operators = operators;
        this.inputNames = inputNames;
        this.inputColumns = inputColumns;
        this.transformers = transformers;
        requiresIndices = Arrays.stream(this.operators).anyMatch(IterativeChunkedAggregationOperator::requiresRowKeys);
        requiresRunFinds =
                Arrays.stream(this.operators).anyMatch(IterativeChunkedAggregationOperator::requiresRunFinds);
        requiresInputs = Arrays.stream(this.inputColumns).anyMatch(Objects::nonNull);
        unchunkedIndices = Arrays.stream(this.operators).allMatch(IterativeChunkedAggregationOperator::unchunkedRowSet);
        // noinspection unchecked
        resultColumns = merge(Arrays.stream(this.operators).map(IterativeChunkedAggregationOperator::getResultColumns)
                .toArray(Map[]::new));

        this.inputSlots = new int[inputColumns.length];
        for (int currentSlot = 0; currentSlot < inputSlots.length; ++currentSlot) {
            if (inputColumns[currentSlot] == null) {
                inputSlots[currentSlot] = -1;
                continue;
            }
            boolean found = false;
            for (int priorSlotToCheck = 0; priorSlotToCheck < currentSlot; ++priorSlotToCheck) {
                if (inputColumns[priorSlotToCheck] == inputColumns[currentSlot]) {
                    inputSlots[currentSlot] = priorSlotToCheck;
                    found = true;
                    break;
                }
            }
            if (!found) {
                inputSlots[currentSlot] = currentSlot;
            }
        }
    }

    private static Map<String, ColumnSource<?>> merge(Map<String, ColumnSource<?>>[] operatorResultColumns) {
        final Map<String, ColumnSource<?>> mergedResult = new LinkedHashMap<>();
        for (final Map<String, ColumnSource<?>> operatorColumns : operatorResultColumns) {
            for (final Map.Entry<String, ColumnSource<?>> entry : operatorColumns.entrySet()) {
                if (mergedResult.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalStateException("Duplicate columns: " + entry.getKey());
                }
            }
        }
        return mergedResult;
    }

    int size() {
        return operators.length;
    }

    boolean requiresIndices() {
        return requiresIndices;
    }

    boolean requiresRunFinds(boolean skip) {
        return requiresRunFinds || !skip;
    }

    boolean unchunkedIndices() {
        return unchunkedIndices;
    }

    boolean requiresInputs() {
        return requiresInputs;
    }

    int inputSlot(int oi) {
        return inputSlots[oi];
    }

    boolean requiresIndices(boolean[] columnsToProcess) {
        for (int ii = 0; ii < columnsToProcess.length; ++ii) {
            if (operators[ii].requiresRowKeys()) {
                return true;
            }
        }
        return false;
    }

    void ensureCapacity(int size) {
        for (final IterativeChunkedAggregationOperator operator : operators) {
            operator.ensureCapacity(size);
        }
    }

    void startTrackingPrevValues() {
        for (final IterativeChunkedAggregationOperator operator : operators) {
            operator.startTrackingPrevValues();
        }
    }

    /**
     * Get any single {@link StateChangeRecorder} present in the {@code operators} array.
     *
     * @return Any single {@link StateChangeRecorder} present in the {@code operators} array
     * @throws io.deephaven.base.verify.AssertionFailure If there is no state change recorder present
     */
    StateChangeRecorder getStateChangeRecorder() {
        return (StateChangeRecorder) Arrays.stream(operators)
                .filter(op -> op instanceof StateChangeRecorder)
                .findAny()
                .orElseThrow(Assert::statementNeverExecuted);
    }

    /**
     * The helper passes in the result column source map, which contains the key columns if any. The context is
     * responsible for filling in the columns generated by the operators or transformations.
     *
     * @param keyColumns the keyColumns as input, the result column source map as output.
     */
    void getResultColumns(Map<String, ColumnSource<?>> keyColumns) {
        keyColumns.putAll(resultColumns);
        if (transformers == null) {
            return;
        }
        for (final AggregationContextTransformer transformer : transformers) {
            transformer.resultColumnFixup(keyColumns);
        }
    }

    QueryTable transformResult(QueryTable table) {
        if (transformers == null) {
            return table;
        }
        for (final AggregationContextTransformer transformer : transformers) {
            table = transformer.transformResult(table);
        }
        return table;
    }

    /**
     * Get the input modified column sets for a given table.
     *
     * @param input the input table
     * @return an array, parallel to operators, of the input column sets for each operator
     */
    ModifiedColumnSet[] getInputModifiedColumnSets(QueryTable input) {
        final ModifiedColumnSet[] inputModifiedColumnSet = new ModifiedColumnSet[inputColumns.length];
        for (int ii = 0; ii < inputColumns.length; ++ii) {
            inputModifiedColumnSet[ii] = input.newModifiedColumnSet(inputNames[ii]);
        }
        return inputModifiedColumnSet;
    }

    /**
     * Allow all operators to perform any internal state keeping needed for destinations that were added during
     * initialization.
     *
     * @param resultTable The result {@link QueryTable} after initialization
     * @param startingDestinationsCount The number of used destinations at the beginning of this step
     */
    void propagateInitialStateToOperators(@NotNull final QueryTable resultTable, final int startingDestinationsCount) {
        for (final IterativeChunkedAggregationOperator operator : operators) {
            operator.propagateInitialState(resultTable, startingDestinationsCount);
        }
    }

    /**
     * Initialize refreshing result support for all operators. As a side effect, get an array of factories to produce
     * result modified column sets for each operator from the upstream modified column set. Each factory is used in turn
     * when its operator reports a modification, in order to produce a final result modified column set.
     *
     * @param resultTable The result table
     * @param aggregationUpdateListener The aggregation update listener, which may be needed for referential integrity
     * @return An array, parallel to operators, of factories that produce a result modified column set from the upstream
     *         modified column set
     */
    UnaryOperator<ModifiedColumnSet>[] initializeRefreshing(@NotNull final QueryTable resultTable,
            @NotNull final LivenessReferent aggregationUpdateListener) {
        // noinspection unchecked
        final UnaryOperator<ModifiedColumnSet>[] resultModifiedColumnSetFactories =
                new UnaryOperator[inputColumns.length];
        for (int ii = 0; ii < inputColumns.length; ++ii) {
            resultModifiedColumnSetFactories[ii] =
                    operators[ii].initializeRefreshing(resultTable, aggregationUpdateListener);
        }
        return resultModifiedColumnSetFactories;
    }

    /**
     * Allow all operators to reset any per-step internal state. Note that the arguments to this method should not be
     * mutated in any way.
     * 
     * @param upstream The upstream {@link TableUpdateImpl}
     * @param startingDestinationsCount The number of used destinations at the beginning of this step
     */
    void resetOperatorsForStep(@NotNull final TableUpdate upstream, final int startingDestinationsCount) {
        for (final IterativeChunkedAggregationOperator operator : operators) {
            operator.resetForStep(upstream, startingDestinationsCount);
        }
    }

    /**
     * Allow all operators to perform any internal state keeping needed for destinations that were added (went from 0
     * keys to &gt 0), removed (went from &gt 0 keys to 0), or modified (keys added or removed, or keys modified) by
     * this iteration. Note that the arguments to this method should not be mutated in any way.
     * 
     * @param downstream The downstream {@link TableUpdate} (which does <em>not</em> have its {@link ModifiedColumnSet}
     *        finalized yet)
     * @param newDestinations New destinations added on this update
     */
    void propagateChangesToOperators(@NotNull final TableUpdate downstream,
            @NotNull final RowSet newDestinations) {
        for (final IterativeChunkedAggregationOperator operator : operators) {
            operator.propagateUpdates(downstream, newDestinations);
        }
    }

    /**
     * Propagate listener failure to all operators.
     *
     * @param originalException The error {@link Throwable}
     * @param sourceEntry The {@link PerformanceEntry} for the failed listener
     */
    void propagateFailureToOperators(@NotNull final Throwable originalException,
            @NotNull final TableListener.Entry sourceEntry) {
        for (final IterativeChunkedAggregationOperator operator : operators) {
            operator.propagateFailure(originalException, sourceEntry);
        }
    }

    /**
     * Initialize the getContexts array; using the provided shared context.
     *
     * @param sharedContext the SharedContext
     * @param getContexts the array to initialize with getContexts
     * @param maxSize maximum size to allocate
     */
    void initializeGetContexts(SharedContext sharedContext, ChunkSource.GetContext[] getContexts, long maxSize) {
        final int chunkSize = ChunkedOperatorAggregationHelper.chunkSize(maxSize);
        for (int oi = 0; oi < size(); ++oi) {
            if (inputSlot(oi) != oi) {
                continue;
            }
            getContexts[oi] = inputColumns[oi].makeGetContext(chunkSize, sharedContext);
        }
    }

    /**
     * Initialize the getContexts array; using the provided shared context.
     *
     * @param sharedContext the SharedContext
     * @param getContexts the array to initialize with getContexts
     * @param maxSize maximum size to allocate
     * @param mask only initialize getContexts[i] if mask[i] is true
     */
    void initializeGetContexts(SharedContext sharedContext, ChunkSource.GetContext[] getContexts, long maxSize,
            boolean[] mask) {
        final int chunkSize = ChunkedOperatorAggregationHelper.chunkSize(maxSize);
        for (int oi = 0; oi < size(); ++oi) {
            if (!mask[oi])
                continue;
            final int inputSlot = inputSlot(oi);
            if (inputSlot < 0 || getContexts[inputSlot] != null)
                continue;
            getContexts[inputSlot] = inputColumns[inputSlot].makeGetContext(chunkSize, sharedContext);
        }
    }

    /**
     * Initialize an array of singleton contexts for our operators.
     *
     * @param contexts the array to initialize with getContexts
     */
    void initializeSingletonContexts(IterativeChunkedAggregationOperator.SingletonContext[] contexts, long maxSize) {
        final int chunkSize = ChunkedOperatorAggregationHelper.chunkSize(maxSize);
        for (int oi = 0; oi < size(); ++oi) {
            contexts[oi] = operators[oi].makeSingletonContext(chunkSize);
        }
    }

    /**
     * Initialize an array of singleton contexts for our operators.
     *
     * @param contexts the array to initialize with getContexts
     * @param mask the columns to initialize
     */
    private void initializeSingletonContexts(IterativeChunkedAggregationOperator.SingletonContext[] contexts,
            long maxSize, boolean[] mask) {
        final int chunkSize = ChunkedOperatorAggregationHelper.chunkSize(maxSize);
        for (int oi = 0; oi < size(); ++oi) {
            if (mask[oi]) {
                contexts[oi] = operators[oi].makeSingletonContext(chunkSize);
            }
        }
    }

    /**
     * Initialize an array of singleton contexts based on an upstream update.
     */
    void initializeSingletonContexts(IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
            TableUpdate upstream, boolean[] modifiedColumns) {
        final long maxSize = UpdateSizeCalculator.chunkSize(upstream, ChunkedOperatorAggregationHelper.CHUNK_SIZE);
        if (upstream.removed().isNonempty() || upstream.added().isNonempty()) {
            initializeSingletonContexts(opContexts, maxSize);
            return;
        }

        final boolean[] toInitialize = computeInitializationMaskFromUpdate(upstream, modifiedColumns);
        initializeSingletonContexts(opContexts, maxSize, toInitialize);
    }

    private boolean[] computeInitializationMaskFromUpdate(TableUpdate upstream,
            boolean[] modifiedColumns) {
        final boolean[] toInitialize = new boolean[size()];
        if (requiresIndices() && upstream.shifted().nonempty()) {
            for (int ii = 0; ii < size(); ++ii) {
                if (operators[ii].requiresRowKeys()) {
                    toInitialize[ii] = true;
                }
            }
        }

        if (upstream.modified().isNonempty()) {
            for (int ii = 0; ii < size(); ++ii) {
                if (operators[ii].requiresRowKeys()) {
                    toInitialize[ii] = true;
                } else {
                    toInitialize[ii] |= modifiedColumns[ii];
                }
            }
        }
        return toInitialize;
    }

    /**
     * Initialize an array of bucketed contexts for our operators.
     *
     * @param contexts the array to initialize with getContexts
     */
    void initializeBucketedContexts(IterativeChunkedAggregationOperator.BucketedContext[] contexts, long maxSize) {
        final int chunkSize = ChunkedOperatorAggregationHelper.chunkSize(maxSize);
        for (int oi = 0; oi < size(); ++oi) {
            contexts[oi] = operators[oi].makeBucketedContext(chunkSize);
        }
    }

    /**
     * Initialize an array of singleton contexts based on an upstream update.
     */
    void initializeBucketedContexts(IterativeChunkedAggregationOperator.BucketedContext[] contexts,
            TableUpdate upstream, boolean keysModified, boolean[] modifiedColumns) {
        final long maxSize = UpdateSizeCalculator.chunkSize(upstream, ChunkedOperatorAggregationHelper.CHUNK_SIZE);
        if (upstream.added().isNonempty() || upstream.removed().isNonempty() || keysModified) {
            initializeBucketedContexts(contexts, maxSize);
            return;
        }
        final boolean[] toInitialize = computeInitializationMaskFromUpdate(upstream, modifiedColumns);
        initializeBucketedContexts(contexts, maxSize, toInitialize);
    }

    private void initializeBucketedContexts(IterativeChunkedAggregationOperator.BucketedContext[] contexts,
            long maxSize, boolean[] mask) {
        final int chunkSize = ChunkedOperatorAggregationHelper.chunkSize(maxSize);
        for (int oi = 0; oi < size(); ++oi) {
            if (mask[oi]) {
                contexts[oi] = operators[oi].makeBucketedContext(chunkSize);
            }
        }
    }

    /**
     * Initialize a working chunks for each input column.
     *
     * @param workingChunks an array to fill with working chunks
     */
    void initializeWorkingChunks(WritableChunk<Values>[] workingChunks, long maxSize) {
        final int chunkSize = ChunkedOperatorAggregationHelper.chunkSize(maxSize);
        for (int ii = 0; ii < inputColumns.length; ++ii) {
            if (inputSlot(ii) == ii) {
                workingChunks[ii] = inputColumns[ii].getChunkType().makeWritableChunk(chunkSize);
            }
        }
    }

    /**
     * Make a suitable permute kernel for each input column.
     */
    PermuteKernel[] makePermuteKernels() {
        final PermuteKernel[] permuteKernels = new PermuteKernel[size()];
        for (int oi = 0; oi < size(); ++oi) {
            if (inputSlot(oi) == oi) {
                permuteKernels[oi] = PermuteKernel.makePermuteKernel(inputColumns[oi].getChunkType());
            }
        }
        return permuteKernels;
    }

    void supplyRowLookup(@NotNull final Supplier<AggregationRowLookup> rowLookupFactory) {
        if (transformers == null) {
            return;
        }
        for (final AggregationContextTransformer aggregationContextTransformer : transformers) {
            aggregationContextTransformer.supplyRowLookup(rowLookupFactory);
        }
    }
}
