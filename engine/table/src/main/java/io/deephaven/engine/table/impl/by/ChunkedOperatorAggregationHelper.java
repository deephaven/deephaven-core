package io.deephaven.engine.table.impl.by;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.impl.GroupingUtils;
import io.deephaven.engine.table.impl.PrevColumnSource;
import io.deephaven.engine.table.impl.indexer.RowSetIndexer;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.sort.findruns.IntFindRunsKernel;
import io.deephaven.engine.table.impl.sort.permute.LongPermuteKernel;
import io.deephaven.engine.table.impl.sort.permute.PermuteKernel;
import io.deephaven.engine.table.impl.sort.timsort.IntIntTimsortKernel;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.SafeCloseableList;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

@SuppressWarnings("rawtypes")
public class ChunkedOperatorAggregationHelper {

    static final int CHUNK_SIZE =
            Configuration.getInstance().getIntegerWithDefault("ChunkedOperatorAggregationHelper.chunkSize", 1 << 12);
    public static final boolean SKIP_RUN_FIND =
            Configuration.getInstance().getBooleanWithDefault("ChunkedOperatorAggregationHelper.skipRunFind", false);
    static final boolean HASHED_RUN_FIND =
            Configuration.getInstance().getBooleanWithDefault("ChunkedOperatorAggregationHelper.hashedRunFind", true);

    public static QueryTable aggregation(AggregationContextFactory aggregationContextFactory, QueryTable queryTable,
            SelectColumn[] groupByColumns) {
        return aggregation(AggregationControl.DEFAULT_FOR_OPERATOR, aggregationContextFactory, queryTable,
                groupByColumns);
    }

    @VisibleForTesting
    public static QueryTable aggregation(AggregationControl control,
            AggregationContextFactory aggregationContextFactory, QueryTable queryTable, SelectColumn[] groupByColumns) {
        final boolean viewRequired = groupByColumns.length > 0
                && Arrays.stream(groupByColumns).anyMatch(selectColumn -> !selectColumn.isRetain());
        final QueryTable withView = !viewRequired ? queryTable : (QueryTable) queryTable.updateView(groupByColumns);

        if (queryTable.hasAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE)) {
            withView.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE,
                    queryTable.getAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE));
        }

        final Mutable<QueryTable> resultHolder = new MutableObject<>();
        final SwapListener swapListener =
                withView.createSwapListenerIfRefreshing(SwapListener::new);
        withView.initializeWithSnapshot(
                "by(" + aggregationContextFactory + ", " + Arrays.toString(groupByColumns) + ")", swapListener,
                (usePrev, beforeClockValue) -> {
                    resultHolder.setValue(aggregation(control, swapListener, aggregationContextFactory, withView,
                            groupByColumns, usePrev));
                    return true;
                });
        return resultHolder.getValue();
    }

    private static QueryTable aggregation(AggregationControl control, SwapListener swapListener,
            AggregationContextFactory aggregationContextFactory, QueryTable withView, SelectColumn[] groupByColumns,
            boolean usePrev) {
        if (groupByColumns.length == 0) {
            return noKeyAggregation(swapListener, aggregationContextFactory, withView, usePrev);
        }

        final String[] keyNames = Arrays.stream(groupByColumns).map(SelectColumn::getName).toArray(String[]::new);
        final ColumnSource<?>[] keySources =
                Arrays.stream(keyNames).map(withView::getColumnSource).toArray(ColumnSource[]::new);
        final ColumnSource<?>[] reinterpretedKeySources = Arrays.stream(keySources)
                .map(ReinterpretUtils::maybeConvertToPrimitive).toArray(ColumnSource[]::new);

        final AggregationContext ac = aggregationContextFactory.makeAggregationContext(withView, keyNames);

        final PermuteKernel[] permuteKernels = ac.makePermuteKernels();

        final boolean useGrouping;
        if (control.considerGrouping(withView, keySources)) {
            Assert.eq(keySources.length, "keySources.length", 1);

            final boolean hasGrouping = RowSetIndexer.of(withView.getRowSet()).hasGrouping(keySources[0]);
            if (!withView.isRefreshing() && hasGrouping) {
                return staticGroupedAggregation(withView, keyNames[0], keySources[0], ac);
            }
            // we have no hasPrevGrouping method
            useGrouping = !usePrev && hasGrouping && Arrays.equals(reinterpretedKeySources, keySources);
        } else {
            useGrouping = false;
        }

        final ChunkedOperatorAggregationStateManager stateManager;
        final IncrementalChunkedOperatorAggregationStateManager incrementalStateManager;
        if (withView.isRefreshing()) {
            stateManager = incrementalStateManager = new IncrementalChunkedOperatorAggregationStateManager(
                    reinterpretedKeySources, control.initialHashTableSize(withView), control.getMaximumLoadFactor(),
                    control.getTargetLoadFactor());
        } else {
            stateManager = new StaticChunkedOperatorAggregationStateManager(reinterpretedKeySources,
                    control.initialHashTableSize(withView), control.getMaximumLoadFactor(),
                    control.getTargetLoadFactor());
            incrementalStateManager = null;
        }
        setReverseLookupFunction(keySources, ac, stateManager);

        final MutableInt outputPosition = new MutableInt();

        if (useGrouping) {
            // This must be incremental, otherwise we would have done this earlier
            initialGroupedKeyAddition(withView, reinterpretedKeySources, ac, incrementalStateManager, outputPosition,
                    usePrev);
        } else {
            initialBucketedKeyAddition(withView, reinterpretedKeySources, ac, permuteKernels, stateManager,
                    outputPosition, usePrev);
        }

        // Construct and return result table
        final ColumnSource[] keyHashTableSources = stateManager.getKeyHashTableSources();
        final Map<String, ColumnSource<?>> resultColumnSourceMap = new LinkedHashMap<>();

        // Gather the result key columns
        final ColumnSource[] keyColumnsRaw = new ColumnSource[keyHashTableSources.length];
        final ArrayBackedColumnSource[] keyColumnsCopied =
                withView.isRefreshing() ? new ArrayBackedColumnSource[keyHashTableSources.length] : null;
        for (int kci = 0; kci < keyHashTableSources.length; ++kci) {
            ColumnSource<?> resultKeyColumnSource = keyHashTableSources[kci];
            if (keySources[kci] != reinterpretedKeySources[kci]) {
                resultKeyColumnSource =
                        ReinterpretUtils.convertToOriginal(keySources[kci].getType(), resultKeyColumnSource);
            }
            keyColumnsRaw[kci] = resultKeyColumnSource;
            if (withView.isRefreshing()) {
                // noinspection ConstantConditions,unchecked
                keyColumnsCopied[kci] = ArrayBackedColumnSource.getMemoryColumnSource(outputPosition.intValue(),
                        keyColumnsRaw[kci].getType());
                resultColumnSourceMap.put(keyNames[kci], keyColumnsCopied[kci]);
            } else {
                resultColumnSourceMap.put(keyNames[kci], keyColumnsRaw[kci]);
            }
        }
        ac.getResultColumns(resultColumnSourceMap);

        final TrackingWritableRowSet resultRowSet =
                RowSetFactory.flat(outputPosition.intValue()).toTracking();
        if (withView.isRefreshing()) {
            copyKeyColumns(keyColumnsRaw, keyColumnsCopied, resultRowSet);
        }

        // Construct the result table
        final QueryTable result = new QueryTable(resultRowSet, resultColumnSourceMap);
        ac.propagateInitialStateToOperators(result);

        if (withView.isRefreshing()) {
            assert stateManager instanceof IncrementalChunkedOperatorAggregationStateManager;
            assert keyColumnsCopied != null;

            ac.startTrackingPrevValues();
            incrementalStateManager.startTrackingPrevValues();

            final boolean isStream = withView.isStream();
            final TableUpdateListener listener =
                    new BaseTable.ListenerImpl("by(" + aggregationContextFactory + ")", withView, result) {
                        @ReferentialIntegrity
                        final SwapListener swapListenerHardReference = swapListener;

                        final ModifiedColumnSet keysUpstreamModifiedColumnSet = withView.newModifiedColumnSet(keyNames);
                        final ModifiedColumnSet[] operatorInputModifiedColumnSets =
                                ac.getInputModifiedColumnSets(withView);
                        final UnaryOperator<ModifiedColumnSet>[] resultModifiedColumnSetFactories =
                                ac.initializeRefreshing(result, this);

                        @Override
                        public void onUpdate(@NotNull final TableUpdate upstream) {
                            final TableUpdate upstreamToUse = isStream ? adjustForStreaming(upstream) : upstream;
                            if (upstreamToUse.empty()) {
                                return;
                            }
                            final TableUpdate downstream;
                            try (final KeyedUpdateContext kuc = new KeyedUpdateContext(ac, incrementalStateManager,
                                    reinterpretedKeySources, permuteKernels, keysUpstreamModifiedColumnSet,
                                    operatorInputModifiedColumnSets,
                                    upstreamToUse, outputPosition)) {
                                downstream = kuc.computeDownstreamIndicesAndCopyKeys(withView.getRowSet(),
                                        keyColumnsRaw,
                                        keyColumnsCopied,
                                        result.getModifiedColumnSetForUpdates(), resultModifiedColumnSetFactories);
                            }
                            result.getRowSet().writableCast().update(downstream.added(), downstream.removed());
                            result.notifyListeners(downstream);
                        }

                        @Override
                        public void onFailureInternal(@NotNull final Throwable originalException, Entry sourceEntry) {
                            ac.propagateFailureToOperators(originalException, sourceEntry);
                            super.onFailureInternal(originalException, sourceEntry);
                        }
                    };

            swapListener.setListenerAndResult(listener, result);
            result.addParentReference(swapListener);
            // In general, result listeners depend on the swap listener for continued liveness, but most
            // operations handle this by having the result table depend on both (in both a reachability sense
            // and a liveness sense). That said, it is arguably very natural for the result listener to manage
            // the swap listener. We do so in this case because partitionBy requires it in order for the
            // sub-tables to continue ticking if the result Table and TableMap are released.
            listener.manage(swapListener);
        }

        return ac.transformResult(result);
    }

    private static TableUpdate adjustForStreaming(@NotNull final TableUpdate upstream) {
        // Streaming aggregations never have modifies or shifts from their parent:
        Assert.assertion(upstream.modified().isEmpty() && upstream.shifted().empty(),
                "upstream.modified.empty() && upstream.shifted.empty()");
        // Streaming aggregations ignore removes:
        if (upstream.removed().isEmpty()) {
            return upstream;
        }
        return new TableUpdateImpl(upstream.added(), RowSetFactory.empty(), upstream.modified(), upstream.shifted(),
                upstream.modifiedColumnSet());
    }

    private static void setReverseLookupFunction(ColumnSource<?>[] keySources, AggregationContext ac,
            ChunkedOperatorAggregationStateManager stateManager) {
        if (keySources.length == 1) {
            if (keySources[0].getType() == DateTime.class) {
                ac.setReverseLookupFunction(key -> stateManager
                        .findPositionForKey(key == null ? null : DateTimeUtils.nanos((DateTime) key)));
            } else if (keySources[0].getType() == Boolean.class) {
                ac.setReverseLookupFunction(
                        key -> stateManager.findPositionForKey(BooleanUtils.booleanAsByte((Boolean) key)));
            } else {
                ac.setReverseLookupFunction(stateManager::findPositionForKey);
            }
        } else {
            final List<Consumer<Object[]>> transformers = new ArrayList<>();
            for (int ii = 0; ii < keySources.length; ++ii) {
                if (keySources[ii].getType() == DateTime.class) {
                    final int fii = ii;
                    transformers.add(reinterpreted -> reinterpreted[fii] =
                            reinterpreted[fii] == null ? null : DateTimeUtils.nanos((DateTime) reinterpreted[fii]));
                } else if (keySources[ii].getType() == Boolean.class) {
                    final int fii = ii;
                    transformers.add(reinterpreted -> reinterpreted[fii] =
                            reinterpreted[fii] == null ? null
                                    : BooleanUtils.booleanAsByte((Boolean) reinterpreted[fii]));
                }
            }
            if (transformers.isEmpty()) {
                ac.setReverseLookupFunction(sk -> stateManager.findPositionForKey(((SmartKey) sk).values_));
            } else {
                ac.setReverseLookupFunction(key -> {
                    final SmartKey smartKey = (SmartKey) key;
                    final Object[] reinterpreted = Arrays.copyOf(smartKey.values_, smartKey.values_.length);
                    for (final Consumer<Object[]> transform : transformers) {
                        transform.accept(reinterpreted);
                    }
                    return stateManager.findPositionForKey(reinterpreted);
                });
            }
        }
    }

    private static class KeyedUpdateContext implements SafeCloseable {

        private final AggregationContext ac;
        private final IncrementalChunkedOperatorAggregationStateManager incrementalStateManager;
        private final ColumnSource[] reinterpretedKeySources;
        private final PermuteKernel[] permuteKernels;
        private final TableUpdate upstream; // Not to be mutated
        private final MutableInt outputPosition;

        private final ModifiedColumnSet updateUpstreamModifiedColumnSet; // Not to be mutated
        private final boolean keysModified;
        private final boolean shifted;
        private final boolean processShifts;
        private final OperatorDivision od;

        private final RowSetBuilderRandom emptiedStatesBuilder;
        private final RowSetBuilderRandom modifiedStatesBuilder;
        private final RowSetBuilderRandom reincarnatedStatesBuilder;
        private final boolean[] modifiedOperators;

        private final SafeCloseableList toClose;

        private final IterativeChunkedAggregationOperator.BucketedContext[] bucketedContexts;
        private final IntIntTimsortKernel.IntIntSortKernelContext<RowKeys, ChunkPositions> sortKernelContext;
        private final HashedRunFinder.HashedRunContext hashedRunContext;

        // These are used for all access when only pre- or post-shift (or previous or current) are needed, else for
        // pre-shift/previous
        private final SharedContext sharedContext;
        private final ChunkSource.GetContext[] getContexts;
        private final WritableChunk<Values>[] workingChunks;
        private final WritableLongChunk<RowKeys> permutedKeyIndices;

        // These are used when post-shift/current values are needed concurrently with pre-shift/previous
        private final SharedContext postSharedContext;
        private final ChunkSource.GetContext[] postGetContexts;
        private final WritableChunk<Values>[] postWorkingChunks;
        private final WritableLongChunk<RowKeys> postPermutedKeyIndices;

        private final WritableIntChunk<ChunkPositions> runStarts;
        private final WritableIntChunk<ChunkLengths> runLengths;
        private final WritableIntChunk<ChunkPositions> chunkPositions;
        private final WritableIntChunk<RowKeys> slots;
        private final WritableBooleanChunk<Values> modifiedSlots;
        private final WritableBooleanChunk<Values> slotsModifiedByOperator;

        private final IncrementalChunkedOperatorAggregationStateManager.BuildContext bc;
        private final WritableIntChunk<RowKeys> reincarnatedSlots;

        private final IncrementalChunkedOperatorAggregationStateManager.ProbeContext pc;
        private final WritableIntChunk<RowKeys> emptiedSlots;

        private KeyedUpdateContext(@NotNull final AggregationContext ac,
                @NotNull final IncrementalChunkedOperatorAggregationStateManager incrementalStateManager,
                @NotNull final ColumnSource[] reinterpretedKeySources,
                @NotNull final PermuteKernel[] permuteKernels,
                @NotNull final ModifiedColumnSet keysUpstreamModifiedColumnSet,
                @NotNull final ModifiedColumnSet[] operatorInputUpstreamModifiedColumnSets,
                @NotNull final TableUpdate upstream,
                @NotNull final MutableInt outputPosition) {
            this.ac = ac;
            this.incrementalStateManager = incrementalStateManager;
            this.reinterpretedKeySources = reinterpretedKeySources;
            this.permuteKernels = permuteKernels;
            this.upstream = upstream;
            this.outputPosition = outputPosition;

            updateUpstreamModifiedColumnSet =
                    upstream.modified().isEmpty() ? ModifiedColumnSet.EMPTY : upstream.modifiedColumnSet();
            keysModified = updateUpstreamModifiedColumnSet.containsAny(keysUpstreamModifiedColumnSet);
            shifted = upstream.shifted().nonempty();
            processShifts = ac.requiresIndices() && shifted;
            od = new OperatorDivision(ac, upstream.modified().isNonempty(), updateUpstreamModifiedColumnSet,
                    operatorInputUpstreamModifiedColumnSets);

            final long buildSize = Math.max(upstream.added().size(), keysModified ? upstream.modified().size() : 0);
            final long probeSizeForModifies =
                    (keysModified || od.anyOperatorHasModifiedInputColumns || ac.requiresIndices())
                            ? upstream.modified().size()
                            : 0;
            final long probeSizeWithoutShifts = Math.max(upstream.removed().size(), probeSizeForModifies);
            final long probeSize =
                    processShifts
                            ? UpdateSizeCalculator.chunkSize(probeSizeWithoutShifts, upstream.shifted(), CHUNK_SIZE)
                            : probeSizeWithoutShifts;
            final int buildChunkSize = chunkSize(buildSize);
            final int probeChunkSize = chunkSize(probeSize);
            final int chunkSize = Math.max(buildChunkSize, probeChunkSize);

            emptiedStatesBuilder = RowSetFactory.builderRandom();
            modifiedStatesBuilder = RowSetFactory.builderRandom();
            reincarnatedStatesBuilder = RowSetFactory.builderRandom();
            modifiedOperators = new boolean[ac.size()];

            toClose = new SafeCloseableList();

            bucketedContexts = toClose.addArray(new IterativeChunkedAggregationOperator.BucketedContext[ac.size()]);
            ac.initializeBucketedContexts(bucketedContexts, upstream, keysModified,
                    od.operatorsWithModifiedInputColumns);
            final boolean findRuns = ac.requiresRunFinds(SKIP_RUN_FIND);
            sortKernelContext =
                    !findRuns || HASHED_RUN_FIND ? null : toClose.add(IntIntTimsortKernel.createContext(chunkSize));
            // even if we are not finding runs because of configuration or operators, we may have a shift in which case
            // we still need to find runs
            hashedRunContext =
                    !HASHED_RUN_FIND ? null : toClose.add(new HashedRunFinder.HashedRunContext(chunkSize));

            sharedContext = toClose.add(SharedContext.makeSharedContext());
            getContexts = toClose.addArray(new ChunkSource.GetContext[ac.size()]);
            ac.initializeGetContexts(sharedContext, getContexts, chunkSize);
            // noinspection unchecked
            workingChunks = toClose.addArray(new WritableChunk[ac.size()]);
            ac.initializeWorkingChunks(workingChunks, chunkSize);
            permutedKeyIndices =
                    ac.requiresIndices() || keysModified ? toClose.add(WritableLongChunk.makeWritableChunk(chunkSize))
                            : null;

            postPermutedKeyIndices = processShifts || keysModified // Note that we need this for modified keys because
                                                                   // we use it to hold removed key indices
                    ? toClose.add(WritableLongChunk.makeWritableChunk(chunkSize))
                    : null;

            if (od.anyOperatorHasModifiedInputColumns || processShifts) {
                postSharedContext = toClose.add(SharedContext.makeSharedContext());
                postGetContexts = toClose.addArray(new ChunkSource.GetContext[ac.size()]);
                ac.initializeGetContexts(postSharedContext, postGetContexts, probeChunkSize);
                // noinspection unchecked
                postWorkingChunks = toClose.addArray(new WritableChunk[ac.size()]);
                ac.initializeWorkingChunks(postWorkingChunks, probeChunkSize);
            } else {
                postSharedContext = null;
                postGetContexts = null;
                postWorkingChunks = null;
            }

            runStarts = toClose.add(WritableIntChunk.makeWritableChunk(chunkSize));
            runLengths = toClose.add(WritableIntChunk.makeWritableChunk(chunkSize));
            chunkPositions = toClose.add(WritableIntChunk.makeWritableChunk(chunkSize));
            slots = toClose.add(WritableIntChunk.makeWritableChunk(chunkSize));
            modifiedSlots = toClose.add(WritableBooleanChunk.makeWritableChunk(chunkSize));
            slotsModifiedByOperator = toClose.add(WritableBooleanChunk.makeWritableChunk(chunkSize));

            if (buildSize > 0) {
                bc = toClose.add(incrementalStateManager.makeBuildContext(reinterpretedKeySources, buildSize));
                reincarnatedSlots = toClose.add(WritableIntChunk.makeWritableChunk(buildChunkSize));
            } else {
                bc = null;
                reincarnatedSlots = null;
            }
            if (probeSize > 0) {
                pc = toClose.add(incrementalStateManager.makeProbeContext(reinterpretedKeySources, probeSize));
            } else {
                pc = null;
            }
            if (upstream.removed().isNonempty() || keysModified) {
                emptiedSlots = toClose.add(WritableIntChunk.makeWritableChunk(probeChunkSize));
            } else {
                emptiedSlots = null;
            }
        }

        @Override
        public final void close() {
            toClose.close();
        }

        private TableUpdate computeDownstreamIndicesAndCopyKeys(
                @NotNull final RowSet upstreamIndex,
                @NotNull final ColumnSource<?>[] keyColumnsRaw,
                @NotNull final WritableColumnSource<?>[] keyColumnsCopied,
                @NotNull final ModifiedColumnSet resultModifiedColumnSet,
                @NotNull final UnaryOperator<ModifiedColumnSet>[] resultModifiedColumnSetFactories) {
            final int previousLastState = outputPosition.intValue();
            ac.resetOperatorsForStep(upstream);

            if (upstream.removed().isNonempty()) {
                doRemoves(upstream.removed());
            }
            if (upstream.modified().isNonempty() && (od.anyOperatorHasModifiedInputColumns
                    || od.anyOperatorWithoutModifiedInputColumnsRequiresIndices || keysModified)) {
                try (final ModifySplitResult split =
                        keysModified ? splitKeyModificationsAndDoKeyChangeRemoves() : null) {
                    if (processShifts) {
                        try (final WritableRowSet postShiftRowSet = upstreamIndex.minus(upstream.added())) {
                            if (keysModified) {
                                postShiftRowSet.remove(split.keyChangeIndicesPostShift);
                            }
                            doShifts(postShiftRowSet); // Also handles shifted same-key modifications for modified-input
                                                       // operators that require indices (if any)
                        }
                        try (final RowSet keysSameUnshiftedModifies =
                                keysModified ? null : getUnshiftedModifies()) {
                            // Do unshifted modifies for everyone
                            assert !keysModified || split.unshiftedSameSlotIndices != null;
                            final RowSet unshiftedSameSlotModifies =
                                    keysModified ? split.unshiftedSameSlotIndices : keysSameUnshiftedModifies;
                            doSameSlotModifies(unshiftedSameSlotModifies, unshiftedSameSlotModifies, true /*
                                                                                                           * We don't
                                                                                                           * process
                                                                                                           * shifts
                                                                                                           * unless some
                                                                                                           * operator
                                                                                                           * requires
                                                                                                           * indices
                                                                                                           */,
                                    od.operatorsWithModifiedInputColumns,
                                    od.operatorsWithoutModifiedInputColumnsThatRequireIndices);

                            if (od.anyOperatorWithModifiedInputColumnsIgnoresIndices) {
                                // Do shifted same-key modifies for RowSet-only and modified-input operators that don't
                                // require indices
                                try (final RowSet removeIndex =
                                        keysModified ? unshiftedSameSlotModifies.union(split.keyChangeIndicesPostShift)
                                                : null;
                                        final RowSet shiftedSameSlotModifiesPost = upstream.modified()
                                                .minus(removeIndex == null ? unshiftedSameSlotModifies : removeIndex);
                                        final WritableRowSet shiftedSameSlotModifiesPre =
                                                shiftedSameSlotModifiesPost.copy()) {
                                    upstream.shifted().unapply(shiftedSameSlotModifiesPre);
                                    doSameSlotModifies(shiftedSameSlotModifiesPre, shiftedSameSlotModifiesPost, true,
                                            od.operatorsWithModifiedInputColumnsThatIgnoreIndices,
                                            od.operatorsThatRequireIndices);
                                }
                            } else if (ac.requiresIndices()) {
                                // Do shifted same-key modifies for RowSet-only operators
                                try (final WritableRowSet shiftedSameSlotModifiesPost =
                                        upstream.modified().minus(unshiftedSameSlotModifies)) {
                                    if (keysModified) {
                                        shiftedSameSlotModifiesPost.remove(split.keyChangeIndicesPostShift);
                                    }
                                    doSameSlotModifyIndicesOnly(shiftedSameSlotModifiesPost,
                                            od.operatorsThatRequireIndices);
                                }
                            }
                        }
                    } else if (od.anyOperatorHasModifiedInputColumns) {
                        assert !keysModified || split.sameSlotIndicesPreShift != null;
                        assert !keysModified || split.sameSlotIndicesPostShift != null;
                        doSameSlotModifies(
                                keysModified ? split.sameSlotIndicesPreShift : upstream.getModifiedPreShift(),
                                keysModified ? split.sameSlotIndicesPostShift : upstream.modified(),
                                ac.requiresIndices(),
                                od.operatorsWithModifiedInputColumns,
                                od.operatorsWithoutModifiedInputColumnsThatRequireIndices);

                    } else {
                        assert !keysModified || split.sameSlotIndicesPostShift != null;
                        doSameSlotModifyIndicesOnly(
                                keysModified ? split.sameSlotIndicesPostShift : upstream.modified(),
                                od.operatorsWithoutModifiedInputColumnsThatRequireIndices);
                    }
                    if (keysModified) {
                        doInserts(split.keyChangeIndicesPostShift, false);
                    }
                }
            } else if (processShifts) {
                try (final RowSet postShiftRowSet = upstreamIndex.minus(upstream.added())) {
                    doShifts(postShiftRowSet);
                }
            }
            if (upstream.added().isNonempty()) {
                doInserts(upstream.added(), true);
            }

            final TableUpdateImpl downstream = new TableUpdateImpl();
            downstream.shifted = RowSetShiftData.EMPTY;

            try (final RowSet newStates = makeNewStatesIndex(previousLastState, outputPosition.intValue() - 1)) {
                downstream.added = reincarnatedStatesBuilder.build();
                downstream.removed = emptiedStatesBuilder.build();

                try (final RowSet addedBack = downstream.added().intersect(downstream.removed())) {
                    downstream.added().writableCast().remove(addedBack);
                    downstream.removed().writableCast().remove(addedBack);

                    if (newStates.isNonempty()) {
                        downstream.added().writableCast().insert(newStates);
                        copyKeyColumns(keyColumnsRaw, keyColumnsCopied, newStates);
                    }

                    downstream.modified = modifiedStatesBuilder.build();
                    downstream.modified().writableCast().remove(downstream.added());
                    downstream.modified().writableCast().remove(downstream.removed());
                    if (ac.addedBackModified()) {
                        downstream.modified().writableCast().insert(addedBack);
                    }
                }

                ac.propagateChangesToOperators(downstream, newStates);
            }

            extractDownstreamModifiedColumnSet(downstream, resultModifiedColumnSet, modifiedOperators,
                    updateUpstreamModifiedColumnSet, resultModifiedColumnSetFactories);

            return downstream;
        }

        private void doRemoves(@NotNull final RowSequence keyIndicesToRemove) {
            if (keyIndicesToRemove.isEmpty()) {
                return;
            }
            try (final RowSequence.Iterator keyIndicesToRemoveIterator = keyIndicesToRemove.getRowSequenceIterator()) {
                while (keyIndicesToRemoveIterator.hasMore()) {
                    doRemovesForChunk(keyIndicesToRemoveIterator.getNextRowSequenceWithLength(CHUNK_SIZE));
                }
            }
        }

        private void doRemovesForChunk(@NotNull final RowSequence keyIndicesToRemoveChunk) {

            incrementalStateManager.remove(pc, keyIndicesToRemoveChunk, reinterpretedKeySources, slots, emptiedSlots);
            emptiedStatesBuilder.addRowKeysChunk(emptiedSlots);

            propagateRemovesToOperators(keyIndicesToRemoveChunk, slots);
        }

        private void propagateRemovesToOperators(@NotNull final RowSequence keyIndicesToRemoveChunk,
                @NotNull final WritableIntChunk<RowKeys> slotsToRemoveFrom) {
            final boolean findRuns = ac.requiresRunFinds(SKIP_RUN_FIND);
            findSlotRuns(sortKernelContext, hashedRunContext, runStarts, runLengths, chunkPositions, slotsToRemoveFrom,
                    findRuns);


            if (ac.requiresIndices()) {
                if (findRuns) {
                    final LongChunk<OrderedRowKeys> keyIndices = keyIndicesToRemoveChunk.asRowKeyChunk();
                    LongPermuteKernel.permuteInput(keyIndices, chunkPositions, permutedKeyIndices);
                } else {
                    keyIndicesToRemoveChunk.fillRowKeyChunk(permutedKeyIndices);
                }
            }

            boolean anyOperatorModified = false;
            boolean firstOperator = true;
            setFalse(modifiedSlots, runStarts.size());

            final Chunk<? extends Values>[] valueChunks = findRuns ? workingChunks : new Chunk[ac.size()];
            sharedContext.reset();
            for (int oi = 0; oi < ac.size(); ++oi) {
                if (!firstOperator) {
                    setFalse(slotsModifiedByOperator, runStarts.size());
                }

                final int inputSlot = ac.inputSlot(oi);
                if (oi == inputSlot) {
                    if (findRuns) {
                        getAndPermuteChunk(ac.inputColumns[oi], getContexts[oi], keyIndicesToRemoveChunk, true,
                                permuteKernels[oi], chunkPositions, workingChunks[oi]);
                    } else {
                        valueChunks[inputSlot] =
                                getChunk(ac.inputColumns[oi], getContexts[oi], keyIndicesToRemoveChunk, true);
                    }
                }
                ac.operators[oi].removeChunk(bucketedContexts[oi], inputSlot >= 0 ? valueChunks[inputSlot] : null,
                        permutedKeyIndices, slotsToRemoveFrom, runStarts, runLengths,
                        firstOperator ? modifiedSlots : slotsModifiedByOperator);

                anyOperatorModified = updateModificationState(modifiedOperators, modifiedSlots, slotsModifiedByOperator,
                        anyOperatorModified, firstOperator, oi);
                firstOperator = false;
            }

            if (anyOperatorModified) {
                modifySlots(modifiedStatesBuilder, runStarts, slotsToRemoveFrom, modifiedSlots);
            }
        }

        private void doInserts(@NotNull final RowSequence keyIndicesToInsert, final boolean addToStateManager) {
            if (keyIndicesToInsert.isEmpty()) {
                return;
            }
            try (final RowSequence.Iterator keyIndicesToInsertIterator = keyIndicesToInsert.getRowSequenceIterator()) {
                while (keyIndicesToInsertIterator.hasMore()) {
                    doInsertsForChunk(keyIndicesToInsertIterator.getNextRowSequenceWithLength(CHUNK_SIZE),
                            addToStateManager);
                }
            }
        }

        private void doInsertsForChunk(@NotNull final RowSequence keyIndicesToInsertChunk,
                final boolean addToStateManager) {
            if (addToStateManager) {
                incrementalStateManager.addForUpdate(bc, keyIndicesToInsertChunk, reinterpretedKeySources,
                        outputPosition, slots, reincarnatedSlots);
                reincarnatedStatesBuilder.addRowKeysChunk(reincarnatedSlots);
            } else {
                incrementalStateManager.findModifications(pc, keyIndicesToInsertChunk, reinterpretedKeySources, slots);
            }

            propagateInsertsToOperators(keyIndicesToInsertChunk, slots);
        }

        private void propagateInsertsToOperators(@NotNull final RowSequence keyIndicesToInsertChunk,
                @NotNull final WritableIntChunk<RowKeys> slotsToAddTo) {
            ac.ensureCapacity(outputPosition.intValue());

            final boolean findRuns = ac.requiresRunFinds(SKIP_RUN_FIND);
            findSlotRuns(sortKernelContext, hashedRunContext, runStarts, runLengths, chunkPositions, slotsToAddTo,
                    findRuns);

            if (ac.requiresIndices()) {
                if (findRuns) {
                    final LongChunk<OrderedRowKeys> keyIndices = keyIndicesToInsertChunk.asRowKeyChunk();
                    permutedKeyIndices.setSize(keyIndices.size());
                    LongPermuteKernel.permuteInput(keyIndices, chunkPositions, permutedKeyIndices);
                } else {
                    keyIndicesToInsertChunk.fillRowKeyChunk(permutedKeyIndices);
                }
            }

            boolean anyOperatorModified = false;
            boolean firstOperator = true;
            setFalse(modifiedSlots, runStarts.size());

            final Chunk<? extends Values>[] valueChunks = findRuns ? workingChunks : new Chunk[ac.size()];

            sharedContext.reset();
            for (int oi = 0; oi < ac.size(); ++oi) {
                if (!firstOperator) {
                    setFalse(slotsModifiedByOperator, runStarts.size());
                }

                final int inputSlot = ac.inputSlot(oi);
                if (inputSlot == oi) {
                    if (findRuns) {
                        getAndPermuteChunk(ac.inputColumns[oi], getContexts[oi], keyIndicesToInsertChunk, false,
                                permuteKernels[oi], chunkPositions, workingChunks[oi]);
                    } else {
                        valueChunks[oi] =
                                getChunk(ac.inputColumns[oi], getContexts[oi], keyIndicesToInsertChunk, false);
                    }
                }
                ac.operators[oi].addChunk(bucketedContexts[oi], inputSlot >= 0 ? valueChunks[inputSlot] : null,
                        permutedKeyIndices, slotsToAddTo, runStarts, runLengths,
                        firstOperator ? modifiedSlots : slotsModifiedByOperator);

                anyOperatorModified = updateModificationState(modifiedOperators, modifiedSlots, slotsModifiedByOperator,
                        anyOperatorModified, firstOperator, oi);
                firstOperator = false;
            }

            if (anyOperatorModified) {
                modifySlots(modifiedStatesBuilder, runStarts, slotsToAddTo, modifiedSlots);
            }
        }

        private void doShifts(@NotNull final RowSet postShiftIndexToProcess) {
            if (postShiftIndexToProcess.isEmpty()) {
                return;
            }
            try (final WritableLongChunk<OrderedRowKeys> preKeyIndices =
                    WritableLongChunk.makeWritableChunk(pc.chunkSize);
                    final WritableLongChunk<OrderedRowKeys> postKeyIndices =
                            WritableLongChunk.makeWritableChunk(pc.chunkSize)) {
                final Runnable applyChunkedShift = () -> doProcessShiftBucketed(preKeyIndices, postKeyIndices);
                processUpstreamShifts(upstream, postShiftIndexToProcess, preKeyIndices, postKeyIndices,
                        applyChunkedShift);
            }
        }

        private void doProcessShiftBucketed(@NotNull final WritableLongChunk<OrderedRowKeys> preKeyIndices,
                @NotNull final WritableLongChunk<OrderedRowKeys> postKeyIndices) {

            final boolean[] chunkInitialized = new boolean[ac.size()];

            try (final RowSequence preShiftChunkKeys =
                    RowSequenceFactory.wrapRowKeysChunkAsRowSequence(WritableLongChunk.downcast(preKeyIndices));
                    final RowSequence postShiftChunkKeys =
                            RowSequenceFactory
                                    .wrapRowKeysChunkAsRowSequence(WritableLongChunk.downcast(postKeyIndices))) {
                sharedContext.reset();
                postSharedContext.reset();
                Arrays.fill(chunkInitialized, false);

                incrementalStateManager.findModifications(pc, postShiftChunkKeys, reinterpretedKeySources, slots);
                // We must accumulate shifts into runs for the same slot, if we bounce from slot 1 to 2 and back to 1,
                // then the polarity checking logic can have us overwrite things because we wouldn't remove all the
                // values from a slot at the same time. Suppose you had
                // Slot RowKey
                // 1 1
                // 2 2
                // 1 3
                // And a shift of {1-3} + 2. We do not want to allow the 1 to shift over the three by removing 1, adding
                // 3; then the 3 would shift to 5 by removing 3 and adding 5. When runs are found you would have the
                // 1,3 removed and then 3,5 inserted without conflict.
                findSlotRuns(sortKernelContext, hashedRunContext, runStarts, runLengths, chunkPositions, slots, true);

                permutedKeyIndices.setSize(preKeyIndices.size());
                postPermutedKeyIndices.setSize(postKeyIndices.size());

                LongPermuteKernel.permuteInput(preKeyIndices, chunkPositions, permutedKeyIndices);
                LongPermuteKernel.permuteInput(postKeyIndices, chunkPositions, postPermutedKeyIndices);

                boolean anyOperatorModified = false;
                boolean firstOperator = true;
                setFalse(modifiedSlots, runStarts.size());

                for (int oi = 0; oi < ac.size(); ++oi) {
                    if (!ac.operators[oi].requiresRowKeys()) {
                        continue;
                    }
                    if (!firstOperator) {
                        setFalse(slotsModifiedByOperator, runStarts.size());
                    }
                    final int inputSlot = ac.inputSlot(oi);
                    if (inputSlot >= 0 && !chunkInitialized[inputSlot]) {
                        getAndPermuteChunk(ac.inputColumns[inputSlot], getContexts[inputSlot], preShiftChunkKeys, true,
                                permuteKernels[inputSlot], chunkPositions, workingChunks[inputSlot]);
                        getAndPermuteChunk(ac.inputColumns[inputSlot], postGetContexts[inputSlot], postShiftChunkKeys,
                                false, permuteKernels[inputSlot], chunkPositions, postWorkingChunks[inputSlot]);
                        chunkInitialized[inputSlot] = true;
                    }
                    ac.operators[oi].shiftChunk(bucketedContexts[oi], inputSlot >= 0 ? workingChunks[inputSlot] : null,
                            inputSlot >= 0 ? postWorkingChunks[inputSlot] : null, permutedKeyIndices,
                            postPermutedKeyIndices, slots, runStarts, runLengths,
                            firstOperator ? modifiedSlots : slotsModifiedByOperator);
                    anyOperatorModified = updateModificationState(modifiedOperators, modifiedSlots,
                            slotsModifiedByOperator, anyOperatorModified, firstOperator, oi);
                    firstOperator = false;
                }

                if (anyOperatorModified) {
                    modifySlots(modifiedStatesBuilder, runStarts, slots, modifiedSlots);
                }
            }
        }

        private void doSameSlotModifies(@NotNull final RowSequence preShiftKeyIndicesToModify,
                @NotNull final RowSequence postShiftKeyIndicesToModify,
                final boolean supplyPostIndices, @NotNull final boolean[] operatorsToProcess,
                @NotNull final boolean[] operatorsToProcessIndicesOnly) {
            final boolean findRuns = ac.requiresRunFinds(SKIP_RUN_FIND);

            final boolean shifted = preShiftKeyIndicesToModify != postShiftKeyIndicesToModify;

            final Chunk<? extends Values>[] valueChunks = findRuns ? workingChunks : new Chunk[ac.size()];
            final Chunk<? extends Values>[] postValueChunks = findRuns ? postWorkingChunks : new Chunk[ac.size()];

            try (final RowSequence.Iterator preShiftIterator = preShiftKeyIndicesToModify.getRowSequenceIterator();
                    final RowSequence.Iterator postShiftIterator =
                            shifted ? postShiftKeyIndicesToModify.getRowSequenceIterator() : null) {
                final boolean[] chunkInitialized = new boolean[ac.size()];
                while (preShiftIterator.hasMore()) {
                    final RowSequence preShiftKeyIndicesChunk =
                            preShiftIterator.getNextRowSequenceWithLength(CHUNK_SIZE);
                    final RowSequence postShiftKeyIndicesChunk =
                            shifted ? postShiftIterator.getNextRowSequenceWithLength(CHUNK_SIZE)
                                    : preShiftKeyIndicesChunk;
                    sharedContext.reset();
                    postSharedContext.reset();
                    Arrays.fill(chunkInitialized, false);

                    incrementalStateManager.findModifications(pc, postShiftKeyIndicesChunk, reinterpretedKeySources,
                            slots);
                    findSlotRuns(sortKernelContext, hashedRunContext, runStarts, runLengths, chunkPositions, slots,
                            findRuns);

                    if (supplyPostIndices) {
                        if (findRuns) {
                            final LongChunk<OrderedRowKeys> postKeyIndices =
                                    postShiftKeyIndicesChunk.asRowKeyChunk();
                            permutedKeyIndices.setSize(postKeyIndices.size());
                            LongPermuteKernel.permuteInput(postKeyIndices, chunkPositions, permutedKeyIndices);
                        } else {
                            postShiftKeyIndicesChunk.fillRowKeyChunk(permutedKeyIndices);
                        }
                    }

                    boolean anyOperatorModified = false;
                    boolean firstOperator = true;
                    setFalse(modifiedSlots, runStarts.size());

                    for (int oi = 0; oi < ac.size(); ++oi) {
                        if (!operatorsToProcessIndicesOnly[oi] && !operatorsToProcess[oi]) {
                            continue;
                        }

                        if (!firstOperator) {
                            setFalse(slotsModifiedByOperator, runStarts.size());
                        }

                        if (operatorsToProcessIndicesOnly[oi]) {
                            ac.operators[oi].modifyRowKeys(bucketedContexts[oi], permutedKeyIndices, slots, runStarts,
                                    runLengths, firstOperator ? modifiedSlots : slotsModifiedByOperator);
                        } else /* operatorsToProcess[oi] */ {
                            final int inputSlot = ac.inputSlot(oi);
                            if (inputSlot >= 0 && !chunkInitialized[inputSlot]) {
                                if (findRuns) {
                                    getAndPermuteChunk(ac.inputColumns[inputSlot], getContexts[inputSlot],
                                            preShiftKeyIndicesChunk, true, permuteKernels[inputSlot], chunkPositions,
                                            workingChunks[inputSlot]);
                                    getAndPermuteChunk(ac.inputColumns[inputSlot], postGetContexts[inputSlot],
                                            postShiftKeyIndicesChunk, false, permuteKernels[inputSlot], chunkPositions,
                                            postWorkingChunks[inputSlot]);
                                } else {
                                    valueChunks[inputSlot] =
                                            getChunk(ac.inputColumns[inputSlot], getContexts[inputSlot],
                                                    preShiftKeyIndicesChunk, true);
                                    postValueChunks[inputSlot] =
                                            getChunk(ac.inputColumns[inputSlot], postGetContexts[inputSlot],
                                                    postShiftKeyIndicesChunk, false);
                                }
                                chunkInitialized[inputSlot] = true;
                            }

                            ac.operators[oi].modifyChunk(bucketedContexts[oi],
                                    inputSlot >= 0 ? valueChunks[inputSlot] : null,
                                    inputSlot >= 0 ? postValueChunks[inputSlot] : null, permutedKeyIndices, slots,
                                    runStarts, runLengths, firstOperator ? modifiedSlots : slotsModifiedByOperator);
                        }

                        anyOperatorModified = updateModificationState(modifiedOperators, modifiedSlots,
                                slotsModifiedByOperator, anyOperatorModified, firstOperator, oi);
                        firstOperator = false;
                    }

                    if (anyOperatorModified) {
                        modifySlots(modifiedStatesBuilder, runStarts, slots, modifiedSlots);
                    }
                }
            }
        }

        private void doSameSlotModifyIndicesOnly(@NotNull final RowSequence postShiftKeyIndicesToModify,
                @NotNull final boolean[] operatorsToProcessIndicesOnly) {
            final boolean findRuns = ac.requiresRunFinds(SKIP_RUN_FIND);

            try (final RowSequence.Iterator postShiftIterator = postShiftKeyIndicesToModify.getRowSequenceIterator()) {
                while (postShiftIterator.hasMore()) {
                    final RowSequence postShiftKeyIndicesChunk =
                            postShiftIterator.getNextRowSequenceWithLength(CHUNK_SIZE);

                    incrementalStateManager.findModifications(pc, postShiftKeyIndicesChunk, reinterpretedKeySources,
                            slots);
                    findSlotRuns(sortKernelContext, hashedRunContext, runStarts, runLengths, chunkPositions, slots,
                            findRuns);

                    if (findRuns) {
                        final LongChunk<OrderedRowKeys> postKeyIndices = postShiftKeyIndicesChunk.asRowKeyChunk();
                        permutedKeyIndices.setSize(postKeyIndices.size());
                        LongPermuteKernel.permuteInput(postKeyIndices, chunkPositions, permutedKeyIndices);
                    } else {
                        postShiftKeyIndicesChunk.fillRowKeyChunk(permutedKeyIndices);
                    }

                    boolean anyOperatorModified = false;
                    boolean firstOperator = true;
                    setFalse(modifiedSlots, runStarts.size());

                    for (int oi = 0; oi < ac.size(); ++oi) {
                        if (!operatorsToProcessIndicesOnly[oi]) {
                            continue;
                        }

                        if (!firstOperator) {
                            setFalse(slotsModifiedByOperator, runStarts.size());
                        }

                        ac.operators[oi].modifyRowKeys(bucketedContexts[oi], permutedKeyIndices, slots, runStarts,
                                runLengths, firstOperator ? modifiedSlots : slotsModifiedByOperator);

                        anyOperatorModified = updateModificationState(modifiedOperators, modifiedSlots,
                                slotsModifiedByOperator, anyOperatorModified, firstOperator, oi);
                        firstOperator = false;
                    }

                    if (anyOperatorModified) {
                        modifySlots(modifiedStatesBuilder, runStarts, slots, modifiedSlots);
                    }
                }
            }
        }

        private static class ModifySplitResult implements SafeCloseable {

            /**
             * This is a partition of same-slot modifies for row keys that were not shifted. Needed for modifyChunk of
             * input-modified operators that require indices, since they handle the shifted same-slot modifies in
             * shiftChunk.
             */
            @Nullable
            private final RowSet unshiftedSameSlotIndices;
            /**
             * This is all of same-slot modified, with row keys in pre-shift space. Needed for modifyChunk of
             * input-modified operators that don't require indices.
             */
            @Nullable
            private final RowSet sameSlotIndicesPreShift;
            /**
             * This is all of same-slot modified, with row keys in post-shift space. Needed for modifyChunk of
             * input-modified operators that don't require indices, and for modifyRowKeys of operators that require
             * indices but don't have any inputs modified.
             */
            @Nullable
            private final RowSet sameSlotIndicesPostShift;
            /**
             * This is all key change modifies, with row keys in post-shift space. Needed for addChunk to process key
             * changes for all operators.
             */
            @NotNull
            private final RowSet keyChangeIndicesPostShift;

            private ModifySplitResult(@Nullable final RowSet unshiftedSameSlotIndices,
                    @Nullable final RowSet sameSlotIndicesPreShift,
                    @Nullable final RowSet sameSlotIndicesPostShift,
                    @NotNull final RowSet keyChangeIndicesPostShift) {
                this.unshiftedSameSlotIndices = unshiftedSameSlotIndices;
                this.sameSlotIndicesPreShift = sameSlotIndicesPreShift;
                this.sameSlotIndicesPostShift = sameSlotIndicesPostShift;
                this.keyChangeIndicesPostShift = keyChangeIndicesPostShift;
            }

            @Override
            public final void close() {
                if (unshiftedSameSlotIndices != null) {
                    unshiftedSameSlotIndices.close();
                }
                if (sameSlotIndicesPreShift != null) {
                    sameSlotIndicesPreShift.close();
                }
                if (sameSlotIndicesPostShift != null) {
                    sameSlotIndicesPostShift.close();
                }
                keyChangeIndicesPostShift.close();
            }
        }

        private ModifySplitResult splitKeyModificationsAndDoKeyChangeRemoves() {
            Require.requirement(keysModified, "keysModified");

            final boolean needUnshiftedSameSlotIndices = processShifts;
            final boolean needSameSlotIndicesPreShift = !processShifts && od.anyOperatorHasModifiedInputColumns;
            final boolean needSameSlotIndicesPostShift = !processShifts && (od.anyOperatorHasModifiedInputColumns
                    || od.anyOperatorWithoutModifiedInputColumnsRequiresIndices || keysModified);

            final RowSetBuilderSequential unshiftedSameSlotIndicesBuilder =
                    needUnshiftedSameSlotIndices ? RowSetFactory.builderSequential() : null;
            final RowSetBuilderSequential sameSlotIndicesPreShiftBuilder =
                    needSameSlotIndicesPreShift ? RowSetFactory.builderSequential() : null;
            final RowSetBuilderSequential sameSlotIndicesPostShiftBuilder =
                    needSameSlotIndicesPostShift ? RowSetFactory.builderSequential() : null;
            final RowSetBuilderSequential keyChangeIndicesPostShiftBuilder =
                    RowSetFactory.builderSequential();

            try (final RowSequence.Iterator modifiedPreShiftIterator =
                    upstream.getModifiedPreShift().getRowSequenceIterator();
                    final RowSequence.Iterator modifiedPostShiftIterator =
                            shifted ? upstream.modified().getRowSequenceIterator() : null;
                    final WritableIntChunk<RowKeys> postSlots = WritableIntChunk.makeWritableChunk(bc.chunkSize)) {

                // Hijacking postPermutedKeyIndices because it's not used in this loop; the rename hopefully makes the
                // code much clearer!
                final WritableLongChunk<OrderedRowKeys> removedKeyIndices =
                        WritableLongChunk.downcast(postPermutedKeyIndices);

                while (modifiedPreShiftIterator.hasMore()) {
                    final RowSequence modifiedPreShiftChunk =
                            modifiedPreShiftIterator.getNextRowSequenceWithLength(CHUNK_SIZE);
                    final RowSequence modifiedPostShiftChunk =
                            shifted ? modifiedPostShiftIterator.getNextRowSequenceWithLength(CHUNK_SIZE)
                                    : modifiedPreShiftChunk;

                    incrementalStateManager.remove(pc, modifiedPreShiftChunk, reinterpretedKeySources, slots,
                            emptiedSlots);
                    emptiedStatesBuilder.addRowKeysChunk(emptiedSlots);
                    incrementalStateManager.addForUpdate(bc, modifiedPostShiftChunk, reinterpretedKeySources,
                            outputPosition, postSlots, reincarnatedSlots);
                    reincarnatedStatesBuilder.addRowKeysChunk(reincarnatedSlots);

                    final LongChunk<OrderedRowKeys> preShiftIndices = modifiedPreShiftChunk.asRowKeyChunk();
                    final LongChunk<OrderedRowKeys> postShiftIndices =
                            shifted ? modifiedPostShiftChunk.asRowKeyChunk() : preShiftIndices;

                    final int chunkSize = slots.size();
                    int numKeyChanges = 0;
                    for (int si = 0; si < chunkSize; ++si) {
                        final int previousSlot = slots.get(si);
                        final int currentSlot = postSlots.get(si);
                        final long previousIndex = preShiftIndices.get(si);
                        final long currentIndex = postShiftIndices.get(si);

                        if (previousSlot == currentSlot) {
                            if (previousIndex == currentIndex && needUnshiftedSameSlotIndices) {
                                unshiftedSameSlotIndicesBuilder.appendKey(currentIndex);
                            }
                            if (needSameSlotIndicesPreShift) {
                                sameSlotIndicesPreShiftBuilder.appendKey(previousIndex);
                            }
                            if (needSameSlotIndicesPostShift) {
                                sameSlotIndicesPostShiftBuilder.appendKey(currentIndex);
                            }
                        } else {
                            slots.set(numKeyChanges, previousSlot);
                            removedKeyIndices.set(numKeyChanges++, previousIndex);
                            keyChangeIndicesPostShiftBuilder.appendKey(currentIndex);
                        }
                    }
                    if (numKeyChanges > 0) {
                        slots.setSize(numKeyChanges);
                        removedKeyIndices.setSize(numKeyChanges);
                        try (final RowSequence keyIndicesToRemoveChunk =
                                RowSequenceFactory.wrapRowKeysChunkAsRowSequence(removedKeyIndices)) {
                            propagateRemovesToOperators(keyIndicesToRemoveChunk, slots);
                        }
                    }
                }
            }

            return new ModifySplitResult(
                    needUnshiftedSameSlotIndices ? unshiftedSameSlotIndicesBuilder.build() : null,
                    needSameSlotIndicesPreShift ? sameSlotIndicesPreShiftBuilder.build() : null,
                    needSameSlotIndicesPostShift ? sameSlotIndicesPostShiftBuilder.build() : null,
                    keyChangeIndicesPostShiftBuilder.build());
        }

        private RowSet getUnshiftedModifies() {
            Require.requirement(!keysModified, "!keysModified");
            Require.requirement(shifted, "shifted");
            return extractUnshiftedModifiesFromUpstream(upstream);
        }

        private static boolean updateModificationState(@NotNull final boolean[] modifiedOperators,
                @NotNull final WritableBooleanChunk<Values> modifiedSlots,
                @NotNull final BooleanChunk<Values> slotsModifiedByOperator, boolean operatorModified,
                final boolean firstOperator, final int operatorIndex) {
            final boolean chunkModifiedSlots;
            if (firstOperator) {
                chunkModifiedSlots = anyTrue(modifiedSlots);
                operatorModified = chunkModifiedSlots;
            } else {
                chunkModifiedSlots = orInto(slotsModifiedByOperator, modifiedSlots);
                operatorModified |= chunkModifiedSlots;
            }
            modifiedOperators[operatorIndex] |= chunkModifiedSlots;
            return operatorModified;
        }
    }

    private static RowSet extractUnshiftedModifiesFromUpstream(@NotNull final TableUpdate upstream) {
        final RowSetBuilderSequential unshiftedModifiesBuilder = RowSetFactory.builderSequential();

        try (final RowSequence.Iterator modifiedPreShiftIterator =
                upstream.getModifiedPreShift().getRowSequenceIterator();
                final RowSequence.Iterator modifiedPostShiftIterator = upstream.modified().getRowSequenceIterator()) {
            while (modifiedPreShiftIterator.hasMore()) {
                final RowSequence modifiedPreShiftChunk =
                        modifiedPreShiftIterator.getNextRowSequenceWithLength(CHUNK_SIZE);
                final RowSequence modifiedPostShiftChunk =
                        modifiedPostShiftIterator.getNextRowSequenceWithLength(CHUNK_SIZE);

                final LongChunk<OrderedRowKeys> preShiftIndices = modifiedPreShiftChunk.asRowKeyChunk();
                final LongChunk<OrderedRowKeys> postShiftIndices = modifiedPostShiftChunk.asRowKeyChunk();

                final int chunkSize = preShiftIndices.size();
                for (int ki = 0; ki < chunkSize; ++ki) {
                    final long previousIndex = preShiftIndices.get(ki);
                    final long currentIndex = postShiftIndices.get(ki);

                    if (previousIndex == currentIndex) {
                        unshiftedModifiesBuilder.appendKey(currentIndex);
                    }
                }
            }
        }

        return unshiftedModifiesBuilder.build();
    }

    private static void extractDownstreamModifiedColumnSet(@NotNull final TableUpdateImpl downstream,
            @NotNull final ModifiedColumnSet resultModifiedColumnSet,
            @NotNull final boolean[] modifiedOperators,
            @NotNull final ModifiedColumnSet updateUpstreamModifiedColumnSet,
            @NotNull final UnaryOperator<ModifiedColumnSet>[] resultModifiedColumnSetFactories) {
        if (downstream.modified().isNonempty()) {
            downstream.modifiedColumnSet = resultModifiedColumnSet;
            downstream.modifiedColumnSet().clear();
            for (int oi = 0; oi < modifiedOperators.length; ++oi) {
                if (modifiedOperators[oi]) {
                    downstream.modifiedColumnSet()
                            .setAll(resultModifiedColumnSetFactories[oi].apply(updateUpstreamModifiedColumnSet));
                }
            }
        } else {
            downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
        }
        if (downstream.modifiedColumnSet().empty() && downstream.modified().isNonempty()) {
            downstream.modified().close();
            downstream.modified = RowSetFactory.empty();
        }
    }

    private static class OperatorDivision {

        private final boolean anyOperatorHasModifiedInputColumns;
        private final boolean[] operatorsWithModifiedInputColumns;

        private final boolean anyOperatorWithModifiedInputColumnsIgnoresIndices;
        private final boolean[] operatorsWithModifiedInputColumnsThatIgnoreIndices;

        private final boolean anyOperatorWithoutModifiedInputColumnsRequiresIndices;
        private final boolean[] operatorsWithoutModifiedInputColumnsThatRequireIndices;

        private final boolean[] operatorsThatRequireIndices;

        private OperatorDivision(@NotNull final AggregationContext ac,
                final boolean upstreamModified,
                @NotNull final ModifiedColumnSet updateUpstreamModifiedColumnSet,
                @NotNull final ModifiedColumnSet[] operatorInputUpstreamModifiedColumnSets) {
            operatorsThatRequireIndices = new boolean[ac.size()];
            for (int oi = 0; oi < ac.size(); ++oi) {
                operatorsThatRequireIndices[oi] = ac.operators[oi].requiresRowKeys();
            }

            operatorsWithModifiedInputColumns = new boolean[ac.size()];
            operatorsWithModifiedInputColumnsThatIgnoreIndices = new boolean[ac.size()];
            operatorsWithoutModifiedInputColumnsThatRequireIndices = new boolean[ac.size()];
            boolean anyOperatorHasModifiedInputColumnsTemp = false;
            boolean anyOperatorWithModifiedInputColumnsIgnoresIndicesTemp = false;
            boolean anyOperatorWithoutModifiedInputColumnsRequiresIndicesTemp = false;
            if (upstreamModified) {
                for (int oi = 0; oi < ac.size(); ++oi) {

                    if (updateUpstreamModifiedColumnSet.containsAny(operatorInputUpstreamModifiedColumnSets[oi])) {
                        operatorsWithModifiedInputColumns[oi] = true;
                        anyOperatorHasModifiedInputColumnsTemp = true;
                        if (!ac.operators[oi].requiresRowKeys()) {
                            operatorsWithModifiedInputColumnsThatIgnoreIndices[oi] = true;
                            anyOperatorWithModifiedInputColumnsIgnoresIndicesTemp = true;
                        }
                    } else if (ac.operators[oi].requiresRowKeys()) {
                        operatorsWithoutModifiedInputColumnsThatRequireIndices[oi] = true;
                        anyOperatorWithoutModifiedInputColumnsRequiresIndicesTemp = true;
                    }
                }
            }

            anyOperatorHasModifiedInputColumns = anyOperatorHasModifiedInputColumnsTemp;
            anyOperatorWithModifiedInputColumnsIgnoresIndices = anyOperatorWithModifiedInputColumnsIgnoresIndicesTemp;
            anyOperatorWithoutModifiedInputColumnsRequiresIndices =
                    anyOperatorWithoutModifiedInputColumnsRequiresIndicesTemp;
        }
    }

    private static void processUpstreamShifts(TableUpdate upstream, RowSet useIndex,
            WritableLongChunk<OrderedRowKeys> preKeyIndices, WritableLongChunk<OrderedRowKeys> postKeyIndices,
            Runnable applyChunkedShift) {
        RowSet.SearchIterator postOkForward = null;
        RowSet.SearchIterator postOkReverse = null;

        boolean lastPolarityReversed = false; // the initial value doesn't matter, because we'll just have a noop apply
                                              // in the worst case
        int writePosition = resetWritePosition(lastPolarityReversed, preKeyIndices, postKeyIndices);

        final RowSetShiftData.Iterator shiftIt = upstream.shifted().applyIterator();
        while (shiftIt.hasNext()) {
            shiftIt.next();

            final boolean polarityReversed = shiftIt.polarityReversed();
            if (polarityReversed != lastPolarityReversed) {
                // if our polarity changed, we must flush out the shifts that are pending
                maybeApplyChunkedShift(applyChunkedShift, preKeyIndices, postKeyIndices, lastPolarityReversed,
                        writePosition);
                writePosition = resetWritePosition(polarityReversed, preKeyIndices, postKeyIndices);
            }

            final long delta = shiftIt.shiftDelta();

            final long endRange = shiftIt.endRange() + delta;
            final long beginRange = shiftIt.beginRange() + delta;

            if (polarityReversed) {
                // we must apply these shifts reversed
                if (postOkReverse == null) {
                    postOkReverse = useIndex.reverseIterator();
                }
                if (postOkReverse.advance(endRange)) {
                    long idx;
                    while ((idx = postOkReverse.currentValue()) >= beginRange) {
                        postKeyIndices.set(--writePosition, idx);
                        preKeyIndices.set(writePosition, idx - delta);

                        if (writePosition == 0) {
                            // once we fill a chunk, we must process the shifts
                            maybeApplyChunkedShift(applyChunkedShift, preKeyIndices, postKeyIndices, polarityReversed,
                                    writePosition);
                            writePosition = resetWritePosition(polarityReversed, preKeyIndices, postKeyIndices);
                        }

                        if (postOkReverse.hasNext()) {
                            postOkReverse.nextLong();
                        } else {
                            break;
                        }
                    }
                }
            } else {
                if (postOkReverse != null) {
                    postOkReverse.close();
                    postOkReverse = null;
                }
                if (postOkForward == null) {
                    postOkForward = useIndex.searchIterator();
                }
                // we can apply these in a forward direction as normal, we just need to accumulate into our key chunks
                if (postOkForward.advance(beginRange)) {
                    long idx;
                    while ((idx = postOkForward.currentValue()) <= endRange) {
                        postKeyIndices.add(idx);
                        preKeyIndices.add(idx - delta);

                        if (postKeyIndices.size() == postKeyIndices.capacity()) {
                            // once we fill a chunk, we must process the shifts
                            maybeApplyChunkedShift(applyChunkedShift, preKeyIndices, postKeyIndices, polarityReversed,
                                    writePosition);
                            writePosition = resetWritePosition(polarityReversed, preKeyIndices, postKeyIndices);
                        }

                        if (postOkForward.hasNext()) {
                            postOkForward.nextLong();
                        } else {
                            break;
                        }
                    }

                }
            }
            lastPolarityReversed = polarityReversed;
        }
        // after we are done, we should process the shift
        maybeApplyChunkedShift(applyChunkedShift, preKeyIndices, postKeyIndices, lastPolarityReversed, writePosition);
        // close our iterators
        if (postOkReverse != null) {
            postOkReverse.close();
        }
        if (postOkForward != null) {
            postOkForward.close();
        }
    }

    private static int resetWritePosition(boolean polarityReversed, WritableLongChunk<OrderedRowKeys> preKeyIndices,
            WritableLongChunk<OrderedRowKeys> postKeyIndices) {
        if (polarityReversed) {
            postKeyIndices.setSize(postKeyIndices.capacity());
            if (preKeyIndices != null) {
                preKeyIndices.setSize(postKeyIndices.capacity());
            }
            return postKeyIndices.capacity();
        } else {
            postKeyIndices.setSize(0);
            if (preKeyIndices != null) {
                preKeyIndices.setSize(0);
            }
            return 0;
        }
    }

    private static void maybeApplyChunkedShift(Runnable applyChunkedShift,
            WritableLongChunk<OrderedRowKeys> preKeyIndices, WritableLongChunk<OrderedRowKeys> postKeyIndices,
            boolean polarityReversed, int writePosition) {
        if (polarityReversed) {
            int chunkSize = postKeyIndices.capacity();
            if (writePosition == chunkSize) {
                return;
            }
            if (writePosition > 0) {
                postKeyIndices.copyFromTypedChunk(postKeyIndices, writePosition, 0, chunkSize - writePosition);
                postKeyIndices.setSize(chunkSize - writePosition);
                if (preKeyIndices != null) {
                    preKeyIndices.copyFromTypedChunk(preKeyIndices, writePosition, 0, chunkSize - writePosition);
                    preKeyIndices.setSize(chunkSize - writePosition);
                }
            }
        } else {
            if (postKeyIndices.size() == 0) {
                return;
            }
        }
        applyChunkedShift.run();
    }

    private static void setFalse(WritableBooleanChunk modifiedSlots, int size) {
        modifiedSlots.fillWithValue(0, size, false);
        modifiedSlots.setSize(size);
    }

    private static boolean orInto(BooleanChunk operatorSlots, WritableBooleanChunk modifiedSlots) {
        boolean anyTrue = false;
        for (int ii = 0; ii < operatorSlots.size(); ++ii) {
            if (operatorSlots.get(ii)) {
                anyTrue = true;
                modifiedSlots.set(ii, true);
            }
        }
        return anyTrue;
    }

    private static boolean anyTrue(BooleanChunk operatorSlots) {
        for (int ii = 0; ii < operatorSlots.size(); ++ii) {
            if (operatorSlots.get(ii)) {
                return true;
            }
        }
        return false;
    }

    private static void findSlotRuns(
            IntIntTimsortKernel.IntIntSortKernelContext<RowKeys, ChunkPositions> sortKernelContext,
            HashedRunFinder.HashedRunContext hashedRunContext,
            WritableIntChunk<ChunkPositions> runStarts, WritableIntChunk<ChunkLengths> runLengths,
            WritableIntChunk<ChunkPositions> chunkPosition, WritableIntChunk<RowKeys> slots,
            boolean findRuns) {
        if (!findRuns) {
            chunkPosition.setSize(slots.size());
            ChunkUtils.fillInOrder(chunkPosition);
            IntFindRunsKernel.findRunsSingles(slots, runStarts, runLengths);
        } else if (HASHED_RUN_FIND) {
            HashedRunFinder.findRunsHashed(hashedRunContext, runStarts, runLengths, chunkPosition, slots);
        } else {
            chunkPosition.setSize(slots.size());
            ChunkUtils.fillInOrder(chunkPosition);
            IntIntTimsortKernel.sort(sortKernelContext, chunkPosition, slots);
            IntFindRunsKernel.findRunsSingles(slots, runStarts, runLengths);
        }
    }

    /**
     * Get values from the inputColumn, and permute them into workingChunk.
     */
    private static void getAndPermuteChunk(ChunkSource.WithPrev<Values> inputColumn, ChunkSource.GetContext getContext,
            RowSequence chunkOk, boolean usePrev, PermuteKernel permuteKernel, IntChunk<ChunkPositions> chunkPosition,
            WritableChunk<Values> workingChunk) {
        final Chunk<? extends Values> values = getChunk(inputColumn, getContext, chunkOk, usePrev);

        // permute the chunk based on the chunkPosition, so that we have values from a slot together
        if (values != null) {
            workingChunk.setSize(values.size());
            permuteKernel.permuteInput(values, chunkPosition, workingChunk);
        }
    }

    @Nullable
    private static Chunk<? extends Values> getChunk(ChunkSource.WithPrev<Values> inputColumn,
            ChunkSource.GetContext getContext, RowSequence chunkOk, boolean usePrev) {
        final Chunk<? extends Values> values;
        if (inputColumn == null) {
            values = null;
        } else if (usePrev) {
            values = inputColumn.getPrevChunk(getContext, chunkOk);
        } else {
            values = inputColumn.getChunk(getContext, chunkOk);
        }
        return values;
    }

    private static void modifySlots(RowSetBuilderRandom modifiedBuilder, IntChunk<ChunkPositions> runStarts,
            WritableIntChunk<RowKeys> slots, BooleanChunk modified) {
        int outIndex = 0;
        for (int runIndex = 0; runIndex < runStarts.size(); ++runIndex) {
            if (modified.get(runIndex)) {
                final int slotStart = runStarts.get(runIndex);
                final int slot = slots.get(slotStart);
                slots.set(outIndex++, slot);
            }
        }
        slots.setSize(outIndex);
        modifiedBuilder.addRowKeysChunk(slots);
    }

    @NotNull
    private static QueryTable staticGroupedAggregation(QueryTable withView, String keyName, ColumnSource<?> keySource,
            AggregationContext ac) {
        final Pair<ArrayBackedColumnSource, ObjectArraySource<RowSet>> groupKeyIndexTable;
        final Map<Object, RowSet> grouping = RowSetIndexer.of(withView.getRowSet()).getGrouping(keySource);
        // noinspection unchecked
        groupKeyIndexTable = GroupingUtils.groupingToFlatSources((ColumnSource) keySource, grouping);
        final int responsiveGroups = grouping.size();

        final Map<String, ColumnSource<?>> resultColumnSourceMap = new LinkedHashMap<>();
        resultColumnSourceMap.put(keyName, groupKeyIndexTable.first);
        ac.getResultColumns(resultColumnSourceMap);

        doGroupedAddition(ac, groupKeyIndexTable, responsiveGroups);

        final QueryTable result = new QueryTable(RowSetFactory.flat(responsiveGroups).toTracking(),
                resultColumnSourceMap);
        ac.propagateInitialStateToOperators(result);

        final ReverseLookupListener rll = ReverseLookupListener.makeReverseLookupListenerWithSnapshot(result, keyName);
        ac.setReverseLookupFunction(k -> (int) rll.get(k));

        return ac.transformResult(result);
    }

    private static void doGroupedAddition(AggregationContext ac,
            Pair<ArrayBackedColumnSource, ObjectArraySource<RowSet>> groupKeyIndexTable, int responsiveGroups) {
        final boolean indicesRequired = ac.requiresIndices();

        final ColumnSource.GetContext[] getContexts = new ColumnSource.GetContext[ac.size()];
        final IterativeChunkedAggregationOperator.SingletonContext[] operatorContexts =
                new IterativeChunkedAggregationOperator.SingletonContext[ac.size()];
        try (final SafeCloseableArray ignored = new SafeCloseableArray<>(getContexts);
                final SafeCloseable ignored2 = new SafeCloseableArray<>(operatorContexts);
                final SharedContext sharedContext = SharedContext.makeSharedContext()) {
            ac.ensureCapacity(responsiveGroups);
            // we don't know how many things are in the groups, so we have to allocate a large chunk
            ac.initializeGetContexts(sharedContext, getContexts, CHUNK_SIZE);
            ac.initializeSingletonContexts(operatorContexts, CHUNK_SIZE);

            final boolean unchunked = !ac.requiresInputs() && ac.unchunkedIndices();
            if (unchunked) {
                for (int ii = 0; ii < responsiveGroups; ++ii) {
                    final RowSet rowSet = groupKeyIndexTable.second.get(ii);
                    for (int oi = 0; oi < ac.size(); ++oi) {
                        ac.operators[oi].addRowSet(operatorContexts[oi], rowSet, ii);
                    }
                }
            } else {
                for (int ii = 0; ii < responsiveGroups; ++ii) {
                    final RowSet rowSet = groupKeyIndexTable.second.get(ii);
                    // noinspection ConstantConditions
                    try (final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator()) {
                        // noinspection unchecked
                        final Chunk<? extends Values>[] workingChunks = new Chunk[ac.size()];

                        while (rsIt.hasMore()) {
                            final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                            final int chunkSize = chunkOk.intSize();
                            final LongChunk<OrderedRowKeys> keyIndices =
                                    indicesRequired ? chunkOk.asRowKeyChunk() : null;
                            sharedContext.reset();

                            Arrays.fill(workingChunks, null);

                            for (int oi = 0; oi < ac.size(); ++oi) {
                                final int inputSlot = ac.inputSlot(oi);
                                if (inputSlot == oi) {
                                    workingChunks[inputSlot] = ac.inputColumns[oi] == null ? null
                                            : ac.inputColumns[oi].getChunk(getContexts[oi], chunkOk);
                                }
                                ac.operators[oi].addChunk(operatorContexts[oi], chunkSize,
                                        inputSlot < 0 ? null : workingChunks[inputSlot], keyIndices, ii);
                            }
                        }
                    }
                }
            }
        }
    }

    private static void initialBucketedKeyAddition(QueryTable withView,
            ColumnSource<?>[] reinterpretedKeySources,
            AggregationContext ac,
            PermuteKernel[] permuteKernels,
            ChunkedOperatorAggregationStateManager stateManager,
            MutableInt outputPosition,
            boolean usePrev) {
        final boolean findRuns = ac.requiresRunFinds(SKIP_RUN_FIND);

        final ChunkSource.GetContext[] getContexts = new ChunkSource.GetContext[ac.size()];
        // noinspection unchecked
        final WritableChunk<Values>[] workingChunks = findRuns ? new WritableChunk[ac.size()] : null;
        final Chunk<? extends Values>[] valueChunks = findRuns ? workingChunks : new Chunk[ac.size()];
        final IterativeChunkedAggregationOperator.BucketedContext[] bucketedContexts =
                new IterativeChunkedAggregationOperator.BucketedContext[ac.size()];

        final ColumnSource<?>[] buildSources;
        if (usePrev) {
            buildSources = Arrays.stream(reinterpretedKeySources)
                    .map((UnaryOperator<ColumnSource<?>>) PrevColumnSource::new).toArray(ColumnSource[]::new);
        } else {
            buildSources = reinterpretedKeySources;
        }

        final RowSet rowSet = usePrev ? withView.getRowSet().copyPrev() : withView.getRowSet();

        if (rowSet.isEmpty()) {
            return;
        }

        final int chunkSize = chunkSize(rowSet.size());

        try (final SafeCloseable bc = stateManager.makeAggregationStateBuildContext(buildSources, chunkSize);
                final SafeCloseable ignored1 = usePrev ? rowSet : null;
                final SafeCloseable ignored2 = new SafeCloseableArray<>(getContexts);
                final SafeCloseable ignored3 = findRuns ? new SafeCloseableArray<>(workingChunks) : null;
                final SafeCloseable ignored4 = new SafeCloseableArray<>(bucketedContexts);
                final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator();
                final WritableIntChunk<RowKeys> outputPositions = WritableIntChunk.makeWritableChunk(chunkSize);
                final WritableIntChunk<ChunkPositions> chunkPosition = WritableIntChunk.makeWritableChunk(chunkSize);
                final SharedContext sharedContext = SharedContext.makeSharedContext();
                final IntIntTimsortKernel.IntIntSortKernelContext<RowKeys, ChunkPositions> sortKernelContext =
                        !findRuns || HASHED_RUN_FIND ? null : IntIntTimsortKernel.createContext(chunkSize);
                final HashedRunFinder.HashedRunContext hashedRunContext =
                        !findRuns || !HASHED_RUN_FIND ? null : new HashedRunFinder.HashedRunContext(chunkSize);
                final WritableIntChunk<ChunkPositions> runStarts = WritableIntChunk.makeWritableChunk(chunkSize);
                final WritableIntChunk<ChunkLengths> runLengths = WritableIntChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<RowKeys> permutedKeyIndices =
                        ac.requiresIndices() ? WritableLongChunk.makeWritableChunk(chunkSize) : null;
                final WritableBooleanChunk<Values> unusedModifiedSlots =
                        WritableBooleanChunk.makeWritableChunk(chunkSize)) {
            ac.initializeGetContexts(sharedContext, getContexts, chunkSize);
            if (findRuns) {
                ac.initializeWorkingChunks(workingChunks, chunkSize);
            }
            ac.initializeBucketedContexts(bucketedContexts, chunkSize);

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(chunkSize);
                sharedContext.reset();

                stateManager.add(bc, chunkOk, buildSources, outputPosition, outputPositions);

                ac.ensureCapacity(outputPosition.intValue());

                findSlotRuns(sortKernelContext, hashedRunContext, runStarts, runLengths, chunkPosition, outputPositions,
                        findRuns);

                if (permutedKeyIndices != null) {
                    if (findRuns) {
                        final LongChunk<OrderedRowKeys> keyIndices = chunkOk.asRowKeyChunk();
                        LongPermuteKernel.permuteInput(keyIndices, chunkPosition, permutedKeyIndices);
                    } else {
                        chunkOk.fillRowKeyChunk(permutedKeyIndices);
                    }
                }

                for (int ii = 0; ii < ac.size(); ++ii) {
                    final int inputSlot = ac.inputSlot(ii);
                    if (ii == inputSlot) {
                        if (!findRuns) {
                            valueChunks[inputSlot] = getChunk(ac.inputColumns[ii], getContexts[ii], chunkOk, usePrev);
                        } else {
                            getAndPermuteChunk(ac.inputColumns[ii], getContexts[ii], chunkOk, usePrev,
                                    permuteKernels[ii],
                                    chunkPosition, workingChunks[ii]);
                        }
                    }
                    ac.operators[ii].addChunk(bucketedContexts[ii],
                            inputSlot >= 0 ? valueChunks[inputSlot] : null,
                            permutedKeyIndices, outputPositions, runStarts, runLengths, unusedModifiedSlots);
                }
            }
        }
    }

    private static void initialGroupedKeyAddition(QueryTable withView,
            ColumnSource<?>[] reinterpretedKeySources,
            AggregationContext ac,
            IncrementalChunkedOperatorAggregationStateManager stateManager,
            MutableInt outputPosition,
            boolean usePrev) {
        final Pair<ArrayBackedColumnSource, ObjectArraySource<RowSet>> groupKeyIndexTable;
        final RowSetIndexer indexer = RowSetIndexer.of(withView.getRowSet());
        final Map<Object, RowSet> grouping = usePrev ? indexer.getPrevGrouping(reinterpretedKeySources[0])
                : indexer.getGrouping(reinterpretedKeySources[0]);
        // noinspection unchecked
        groupKeyIndexTable =
                GroupingUtils.groupingToFlatSources((ColumnSource) reinterpretedKeySources[0], grouping);
        final int responsiveGroups = grouping.size();

        if (responsiveGroups == 0) {
            return;
        }

        ac.ensureCapacity(responsiveGroups);

        final ColumnSource[] groupedFlatKeySource = {groupKeyIndexTable.first};

        try (final SafeCloseable bc =
                stateManager.makeAggregationStateBuildContext(groupedFlatKeySource, responsiveGroups);
                final RowSequence rs = RowSequenceFactory.forRange(0, responsiveGroups - 1);
                final RowSequence.Iterator rsIt = rs.getRowSequenceIterator();
                final WritableIntChunk<RowKeys> outputPositions =
                        WritableIntChunk.makeWritableChunk(responsiveGroups)) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                stateManager.add(bc, chunkOk, groupedFlatKeySource, outputPosition, outputPositions);
            }
            Assert.eq(outputPosition.intValue(), "outputPosition.intValue()", responsiveGroups, "responsiveGroups");
        }

        for (int ii = 0; ii < responsiveGroups; ++ii) {
            // noinspection ConstantConditions
            final long groupSize = groupKeyIndexTable.second.get(ii).size();
            stateManager.setRowSize(ii, groupSize);
        }

        doGroupedAddition(ac, groupKeyIndexTable, responsiveGroups);
    }

    private static RowSet makeNewStatesIndex(final int first, final int last) {
        return first > last ? RowSetFactory.empty()
                : RowSetFactory.fromRange(first, last);
    }

    private static void copyKeyColumns(ColumnSource<?>[] keyColumnsRaw, WritableColumnSource<?>[] keyColumnsCopied,
            final RowSet copyValues) {
        if (copyValues.isEmpty()) {
            return;
        }
        final int chunkSize = chunkSize(copyValues.size());
        final ColumnSource.GetContext[] getContext = new ColumnSource.GetContext[keyColumnsRaw.length];
        final ChunkSink.FillFromContext[] fillFromContexts =
                new ChunkSink.FillFromContext[keyColumnsRaw.length];
        try (final RowSequence.Iterator rsIt = copyValues.getRowSequenceIterator();
                final SharedContext sharedContext = SharedContext.makeSharedContext();
                final SafeCloseableArray ignored = new SafeCloseableArray<>(getContext);
                final SafeCloseableArray ignored2 = new SafeCloseableArray<>(fillFromContexts)) {
            for (int ii = 0; ii < keyColumnsRaw.length; ++ii) {
                getContext[ii] = keyColumnsRaw[ii].makeGetContext(chunkSize, sharedContext);
                fillFromContexts[ii] = keyColumnsCopied[ii].makeFillFromContext(chunkSize);
                keyColumnsCopied[ii].ensureCapacity(copyValues.lastRowKey() + 1);
            }

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(chunkSize);
                sharedContext.reset();
                for (int ci = 0; ci < keyColumnsRaw.length; ci++) {
                    final Chunk<? extends Values> values = keyColumnsRaw[ci].getChunk(getContext[ci], chunkOk);
                    keyColumnsCopied[ci].fillFromChunk(fillFromContexts[ci], values, chunkOk);
                }
            }
        }
    }

    private static QueryTable noKeyAggregation(SwapListener swapListener,
            AggregationContextFactory aggregationContextFactory, QueryTable table, boolean usePrev) {

        final AggregationContext ac = aggregationContextFactory.makeAggregationContext(table);
        final Map<String, ColumnSource<?>> resultColumnSourceMap = new LinkedHashMap<>();
        ac.getResultColumns(resultColumnSourceMap);

        final boolean[] allColumns = new boolean[ac.size()];
        Arrays.fill(allColumns, true);

        // We intentionally update the 0 key even if we have no rows, so we'd better have capacity for it.
        ac.ensureCapacity(1);

        // we don't actually care about the modified columns here and it will never set anything to false, so it is safe
        // to use allColumns as the modified columns parameter
        final IterativeChunkedAggregationOperator.SingletonContext[] opContexts =
                new IterativeChunkedAggregationOperator.SingletonContext[ac.size()];
        final RowSet rowSet = usePrev ? table.getRowSet().copyPrev() : table.getRowSet();
        final int initialResultSize;
        try (final SafeCloseable ignored1 = new SafeCloseableArray<>(opContexts);
                final SafeCloseable ignored2 = usePrev ? rowSet : null) {
            initialResultSize = rowSet.size() == 0 ? 0 : 1;
            ac.initializeSingletonContexts(opContexts, rowSet.size());
            doNoKeyAddition(rowSet, ac, opContexts, allColumns, usePrev, allColumns);
        }

        final QueryTable result = new QueryTable(RowSetFactory.flat(initialResultSize).toTracking(),
                resultColumnSourceMap);
        ac.propagateInitialStateToOperators(result);

        if (table.isRefreshing()) {
            ac.startTrackingPrevValues();

            final boolean isStream = table.isStream();
            final TableUpdateListener listener =
                    new BaseTable.ListenerImpl("groupBy(" + aggregationContextFactory + ")", table, result) {

                        final ModifiedColumnSet[] inputModifiedColumnSet = ac.getInputModifiedColumnSets(table);
                        final UnaryOperator<ModifiedColumnSet>[] resultModifiedColumnSetFactories =
                                ac.initializeRefreshing(result, this);

                        int lastSize = initialResultSize;
                        int statesCreated = initialResultSize;

                        @Override
                        public void onUpdate(@NotNull final TableUpdate upstream) {
                            final TableUpdate upstreamToUse = isStream ? adjustForStreaming(upstream) : upstream;
                            if (upstreamToUse.empty()) {
                                return;
                            }
                            processNoKeyUpdate(upstreamToUse);
                        }

                        private void processNoKeyUpdate(@NotNull final TableUpdate upstream) {
                            ac.resetOperatorsForStep(upstream);

                            final ModifiedColumnSet upstreamModifiedColumnSet =
                                    upstream.modified().isEmpty() ? ModifiedColumnSet.EMPTY
                                            : upstream.modifiedColumnSet();

                            final IterativeChunkedAggregationOperator.SingletonContext[] opContexts =
                                    new IterativeChunkedAggregationOperator.SingletonContext[ac.size()];
                            try (final SafeCloseable ignored = new SafeCloseableArray<>(opContexts)) {
                                final OperatorDivision od = new OperatorDivision(ac, upstream.modified().isNonempty(),
                                        upstreamModifiedColumnSet, inputModifiedColumnSet);
                                ac.initializeSingletonContexts(opContexts, upstream,
                                        od.operatorsWithModifiedInputColumns);

                                final boolean[] modifiedOperators = new boolean[ac.size()];
                                // remove all the removals
                                if (upstream.removed().isNonempty()) {
                                    doNoKeyRemoval(upstream.removed(), ac, opContexts, allColumns, modifiedOperators);
                                }

                                final boolean processShifts = upstream.shifted().nonempty() && ac.requiresIndices();

                                if (upstream.modified().isNonempty() && (od.anyOperatorHasModifiedInputColumns
                                        || od.anyOperatorWithoutModifiedInputColumnsRequiresIndices)) {
                                    if (processShifts) {
                                        // Also handles shifted modifications for modified-input operators that require
                                        // indices (if any)
                                        doNoKeyShifts(table, upstream, ac, opContexts, od.operatorsThatRequireIndices,
                                                modifiedOperators);

                                        try (final RowSet unshiftedModifies =
                                                extractUnshiftedModifiesFromUpstream(upstream)) {
                                            // Do unshifted modifies for everyone
                                            doNoKeyModifications(unshiftedModifies, unshiftedModifies, ac, opContexts,
                                                    true,
                                                    od.operatorsWithModifiedInputColumns,
                                                    od.operatorsWithoutModifiedInputColumnsThatRequireIndices,
                                                    modifiedOperators);

                                            if (od.anyOperatorWithModifiedInputColumnsIgnoresIndices) {
                                                // Do shifted modifies for RowSet-only and modified-input operators that
                                                // don't require indices
                                                try (final RowSet shiftedModifiesPost =
                                                        upstream.modified().minus(unshiftedModifies);
                                                        final WritableRowSet shiftedModifiesPre =
                                                                shiftedModifiesPost.copy()) {
                                                    upstream.shifted().unapply(shiftedModifiesPre);
                                                    doNoKeyModifications(shiftedModifiesPre, shiftedModifiesPost, ac,
                                                            opContexts, true,
                                                            od.operatorsWithModifiedInputColumnsThatIgnoreIndices,
                                                            od.operatorsThatRequireIndices,
                                                            modifiedOperators);
                                                }
                                            } else if (ac.requiresIndices()) {
                                                // Do shifted modifies for RowSet-only operators
                                                try (final RowSet shiftedModifiesPost =
                                                        upstream.modified().minus(unshiftedModifies)) {
                                                    doIndexOnlyNoKeyModifications(shiftedModifiesPost, ac, opContexts,
                                                            od.operatorsThatRequireIndices, modifiedOperators);
                                                }
                                            }
                                        }
                                    } else if (od.anyOperatorHasModifiedInputColumns) {
                                        doNoKeyModifications(upstream.getModifiedPreShift(), upstream.modified(), ac,
                                                opContexts, ac.requiresIndices(),
                                                od.operatorsWithModifiedInputColumns,
                                                od.operatorsWithoutModifiedInputColumnsThatRequireIndices,
                                                modifiedOperators);

                                    } else {
                                        doIndexOnlyNoKeyModifications(upstream.modified(), ac, opContexts,
                                                od.operatorsWithoutModifiedInputColumnsThatRequireIndices,
                                                modifiedOperators);
                                    }
                                } else if (processShifts) {
                                    doNoKeyShifts(table, upstream, ac, opContexts, od.operatorsThatRequireIndices,
                                            modifiedOperators);
                                }

                                if (upstream.added().isNonempty()) {
                                    doNoKeyAddition(upstream.added(), ac, opContexts, allColumns, false,
                                            modifiedOperators);
                                }

                                final int newResultSize = (!isStream || lastSize == 0) && table.size() == 0 ? 0 : 1;
                                final TableUpdateImpl downstream = new TableUpdateImpl();
                                downstream.shifted = RowSetShiftData.EMPTY;
                                if ((lastSize == 0 && newResultSize == 1)) {
                                    downstream.added = RowSetFactory.fromKeys(0);
                                    downstream.removed = RowSetFactory.empty();
                                    downstream.modified = RowSetFactory.empty();
                                    result.getRowSet().writableCast().insert(0);
                                } else if (lastSize == 1 && newResultSize == 0) {
                                    downstream.added = RowSetFactory.empty();
                                    downstream.removed = RowSetFactory.fromKeys(0);
                                    downstream.modified = RowSetFactory.empty();
                                    result.getRowSet().writableCast().remove(0);
                                } else {
                                    if (!anyTrue(BooleanChunk.chunkWrap(modifiedOperators))) {
                                        return;
                                    }
                                    downstream.added = RowSetFactory.empty();
                                    downstream.removed = RowSetFactory.empty();
                                    downstream.modified = RowSetFactory.fromKeys(0);
                                }
                                lastSize = newResultSize;

                                final int newStatesCreated = Math.max(statesCreated, newResultSize);
                                try (final RowSet newStates =
                                        makeNewStatesIndex(statesCreated, newStatesCreated - 1)) {
                                    ac.propagateChangesToOperators(downstream, newStates);
                                }
                                statesCreated = newStatesCreated;

                                extractDownstreamModifiedColumnSet(downstream, result.getModifiedColumnSetForUpdates(),
                                        modifiedOperators, upstreamModifiedColumnSet, resultModifiedColumnSetFactories);

                                result.notifyListeners(downstream);
                            }
                        }

                        @Override
                        public void onFailureInternal(@NotNull final Throwable originalException,
                                final Entry sourceEntry) {
                            ac.propagateFailureToOperators(originalException, sourceEntry);
                            super.onFailureInternal(originalException, sourceEntry);
                        }
                    };
            swapListener.setListenerAndResult(listener, result);
            result.addParentReference(swapListener);
            listener.manage(swapListener); // See note on keyed version
        }

        ac.setReverseLookupFunction(key -> SmartKey.EMPTY.equals(key) ? 0 : -1);

        return ac.transformResult(result);
    }

    private static void doNoKeyAddition(RowSequence index, AggregationContext ac,
            IterativeChunkedAggregationOperator.SingletonContext[] opContexts, boolean[] operatorsToProcess,
            boolean usePrev, boolean[] modifiedOperators) {
        doNoKeyUpdate(index, ac, opContexts, operatorsToProcess, usePrev, false, modifiedOperators);
    }

    private static void doNoKeyRemoval(RowSequence index, AggregationContext ac,
            IterativeChunkedAggregationOperator.SingletonContext[] opContexts, boolean[] operatorsToProcess,
            boolean[] modifiedOperators) {
        doNoKeyUpdate(index, ac, opContexts, operatorsToProcess, true, true, modifiedOperators);
    }

    private static void doNoKeyModifications(RowSequence preIndex, RowSequence postIndex, AggregationContext ac,
            IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
            final boolean supplyPostIndices, @NotNull final boolean[] operatorsToProcess,
            @NotNull final boolean[] operatorsToProcessIndicesOnly, @NotNull final boolean[] modifiedOperators) {
        final ColumnSource.GetContext[] preGetContexts = new ColumnSource.GetContext[ac.size()];
        final ColumnSource.GetContext[] postGetContexts = new ColumnSource.GetContext[ac.size()];

        try (final SafeCloseableArray ignored = new SafeCloseableArray<>(preGetContexts);
                final SafeCloseableArray ignored2 = new SafeCloseableArray<>(postGetContexts);
                final SharedContext preSharedContext = SharedContext.makeSharedContext();
                final SharedContext postSharedContext = SharedContext.makeSharedContext();
                final RowSequence.Iterator preIt = preIndex.getRowSequenceIterator();
                final RowSequence.Iterator postIt = postIndex.getRowSequenceIterator()) {
            ac.initializeGetContexts(preSharedContext, preGetContexts, preIndex.size(), operatorsToProcess);
            ac.initializeGetContexts(postSharedContext, postGetContexts, postIndex.size(), operatorsToProcess);

            final Chunk<? extends Values>[] workingPreChunks = new Chunk[ac.size()];
            final Chunk<? extends Values>[] workingPostChunks = new Chunk[ac.size()];

            while (postIt.hasMore()) {
                final RowSequence postChunkOk = postIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                final RowSequence preChunkOk = preIt.getNextRowSequenceWithLength(CHUNK_SIZE);

                final int chunkSize = postChunkOk.intSize();

                preSharedContext.reset();
                postSharedContext.reset();

                final LongChunk<OrderedRowKeys> postKeyIndices =
                        supplyPostIndices ? postChunkOk.asRowKeyChunk() : null;

                Arrays.fill(workingPreChunks, null);
                Arrays.fill(workingPostChunks, null);

                for (int ii = 0; ii < ac.size(); ++ii) {
                    if (operatorsToProcessIndicesOnly[ii]) {
                        modifiedOperators[ii] |= ac.operators[ii].modifyRowKeys(opContexts[ii], postKeyIndices, 0);
                        continue;
                    }
                    if (operatorsToProcess[ii]) {
                        final Chunk<? extends Values> preValues;
                        final Chunk<? extends Values> postValues;
                        if (ac.inputColumns[ii] == null) {
                            preValues = postValues = null;
                        } else {
                            final int inputSlot = ac.inputSlot(ii);
                            if (workingPreChunks[inputSlot] == null) {
                                workingPreChunks[inputSlot] =
                                        ac.inputColumns[inputSlot].getPrevChunk(preGetContexts[inputSlot], preChunkOk);
                                workingPostChunks[inputSlot] =
                                        ac.inputColumns[inputSlot].getChunk(postGetContexts[inputSlot], postChunkOk);
                            }
                            preValues = workingPreChunks[inputSlot];
                            postValues = workingPostChunks[inputSlot];
                        }
                        modifiedOperators[ii] |= ac.operators[ii].modifyChunk(opContexts[ii], chunkSize, preValues,
                                postValues, postKeyIndices, 0);
                    }
                }
            }
        }
    }

    private static void doIndexOnlyNoKeyModifications(@NotNull final RowSequence postIndex,
            @NotNull final AggregationContext ac,
            @NotNull final IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
            @NotNull final boolean[] operatorsToProcessIndicesOnly, @NotNull final boolean[] modifiedOperators) {
        try (final RowSequence.Iterator postIt = postIndex.getRowSequenceIterator()) {
            while (postIt.hasMore()) {
                final RowSequence postChunkOk = postIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                final LongChunk<OrderedRowKeys> postKeyIndices = postChunkOk.asRowKeyChunk();
                for (int ii = 0; ii < ac.size(); ++ii) {
                    if (operatorsToProcessIndicesOnly[ii]) {
                        modifiedOperators[ii] |= ac.operators[ii].modifyRowKeys(opContexts[ii], postKeyIndices, 0);
                    }
                }
            }
        }
    }

    private static void doNoKeyUpdate(RowSequence index, AggregationContext ac,
            IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
            boolean[] operatorsToProcess, boolean usePrev, boolean remove, boolean[] modifiedOperators) {
        final ColumnSource.GetContext[] getContexts = new ColumnSource.GetContext[ac.size()];
        final boolean indicesRequired = ac.requiresIndices(operatorsToProcess);

        try (final SafeCloseableArray ignored = new SafeCloseableArray<>(getContexts);
                final SharedContext sharedContext = SharedContext.makeSharedContext();
                final RowSequence.Iterator rsIt = index.getRowSequenceIterator()) {
            ac.initializeGetContexts(sharedContext, getContexts, index.size(), operatorsToProcess);

            // noinspection unchecked
            final Chunk<? extends Values>[] workingChunks = new Chunk[ac.size()];

            // on an empty initial pass we want to go through the operator anyway, so that we initialize things
            // correctly for the aggregation of zero keys
            do {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                sharedContext.reset();

                final LongChunk<OrderedRowKeys> keyIndices = indicesRequired ? chunkOk.asRowKeyChunk() : null;

                Arrays.fill(workingChunks, null);

                for (int ii = 0; ii < ac.size(); ++ii) {
                    if (!operatorsToProcess[ii]) {
                        continue;
                    }
                    final int inputSlot = ac.inputSlot(ii);

                    if (inputSlot >= 0 && workingChunks[inputSlot] == null) {
                        workingChunks[inputSlot] =
                                fetchValues(usePrev, chunkOk, ac.inputColumns[inputSlot], getContexts[inputSlot]);
                    }

                    modifiedOperators[ii] |=
                            processColumnNoKey(remove, chunkOk, inputSlot >= 0 ? workingChunks[inputSlot] : null,
                                    ac.operators[ii], opContexts[ii], keyIndices);
                }
            } while (rsIt.hasMore());
        }
    }

    private static void doNoKeyShifts(QueryTable source, TableUpdate upstream, AggregationContext ac,
            final IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
            boolean[] operatorsToShift, boolean[] modifiedOperators) {
        final ColumnSource.GetContext[] getContexts = new ColumnSource.GetContext[ac.size()];
        final ColumnSource.GetContext[] postGetContexts = new ColumnSource.GetContext[ac.size()];

        try (final RowSet useRowSet = source.getRowSet().minus(upstream.added())) {
            if (useRowSet.isEmpty()) {
                return;
            }
            final int chunkSize = chunkSize(useRowSet.size());
            try (final SafeCloseableArray ignored = new SafeCloseableArray<>(getContexts);
                    final SafeCloseableArray ignored2 = new SafeCloseableArray<>(postGetContexts);
                    final SharedContext sharedContext = SharedContext.makeSharedContext();
                    final SharedContext postSharedContext = SharedContext.makeSharedContext();
                    final WritableLongChunk<OrderedRowKeys> preKeyIndices =
                            WritableLongChunk.makeWritableChunk(chunkSize);
                    final WritableLongChunk<OrderedRowKeys> postKeyIndices =
                            WritableLongChunk.makeWritableChunk(chunkSize)) {
                ac.initializeGetContexts(sharedContext, getContexts, chunkSize, operatorsToShift);
                ac.initializeGetContexts(postSharedContext, postGetContexts, chunkSize, operatorsToShift);

                final Runnable applyChunkedShift =
                        () -> doProcessShiftNoKey(ac, opContexts, operatorsToShift, sharedContext, postSharedContext,
                                getContexts, postGetContexts, preKeyIndices, postKeyIndices, modifiedOperators);
                processUpstreamShifts(upstream, useRowSet, preKeyIndices, postKeyIndices, applyChunkedShift);
            }
        }
    }

    private static void doProcessShiftNoKey(AggregationContext ac,
            IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
            boolean[] operatorsToShift, SharedContext sharedContext, SharedContext postSharedContext,
            ColumnSource.GetContext[] getContexts, ColumnSource.GetContext[] postGetContexts,
            WritableLongChunk<OrderedRowKeys> preKeyIndices, WritableLongChunk<OrderedRowKeys> postKeyIndices,
            boolean[] modifiedOperators) {
        // noinspection unchecked
        final Chunk<? extends Values>[] workingPreChunks = new Chunk[ac.size()];
        // noinspection unchecked
        final Chunk<? extends Values>[] workingPostChunks = new Chunk[ac.size()];

        try (final RowSequence preChunkOk = RowSequenceFactory.wrapRowKeysChunkAsRowSequence(preKeyIndices);
                final RowSequence postChunkOk = RowSequenceFactory.wrapRowKeysChunkAsRowSequence(postKeyIndices)) {
            sharedContext.reset();
            postSharedContext.reset();
            Arrays.fill(workingPreChunks, null);
            Arrays.fill(workingPostChunks, null);

            for (int ii = 0; ii < ac.size(); ++ii) {
                if (operatorsToShift[ii]) {
                    final Chunk<? extends Values> previousValues;
                    final Chunk<? extends Values> newValues;

                    final int inputSlot = ac.inputSlot(ii);
                    if (ac.inputColumns[ii] == null) {
                        previousValues = newValues = null;
                    } else {
                        if (workingPreChunks[inputSlot] == null) {
                            workingPreChunks[inputSlot] = ac.inputColumns[ii].getPrevChunk(getContexts[ii], preChunkOk);
                            workingPostChunks[inputSlot] =
                                    ac.inputColumns[ii].getChunk(postGetContexts[ii], postChunkOk);
                        }
                        previousValues = workingPreChunks[inputSlot];
                        newValues = workingPostChunks[inputSlot];
                    }

                    modifiedOperators[ii] |= ac.operators[ii].shiftChunk(opContexts[ii], previousValues, newValues,
                            preKeyIndices, postKeyIndices, 0);
                }
            }
        }
    }

    private static boolean processColumnNoKey(boolean remove, RowSequence chunkOk, Chunk<? extends Values> values,
            IterativeChunkedAggregationOperator operator,
            IterativeChunkedAggregationOperator.SingletonContext opContext,
            LongChunk<? extends RowKeys> keyIndices) {
        if (remove) {
            return operator.removeChunk(opContext, chunkOk.intSize(), values, keyIndices, 0);
        } else {
            return operator.addChunk(opContext, chunkOk.intSize(), values, keyIndices, 0);
        }
    }

    @Nullable
    private static Chunk<? extends Values> fetchValues(boolean usePrev, RowSequence chunkOk,
            ChunkSource.WithPrev inputColumn, ChunkSource.GetContext getContext) {
        final Chunk<? extends Values> values;
        if (inputColumn == null) {
            values = null;
        } else if (usePrev) {
            // noinspection unchecked
            values = inputColumn.getPrevChunk(getContext, chunkOk);
        } else {
            // noinspection unchecked
            values = inputColumn.getChunk(getContext, chunkOk);
        }
        return values;
    }

    /**
     * Return the minimum of our chunk size and the size of an input.
     *
     * @param size the input size
     * @return an appropriate chunk size to use
     */
    public static int chunkSize(long size) {
        return (int) Math.min(size, CHUNK_SIZE);
    }
}
