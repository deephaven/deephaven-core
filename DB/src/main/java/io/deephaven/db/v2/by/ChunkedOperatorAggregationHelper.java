package io.deephaven.db.v2.by;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.BooleanUtils;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.ShiftAwareListener.Update;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.sort.findruns.IntFindRunsKernel;
import io.deephaven.db.v2.sort.permute.LongPermuteKernel;
import io.deephaven.db.v2.sort.permute.PermuteKernel;
import io.deephaven.db.v2.sort.timsort.IntIntTimsortKernel;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.*;
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

    @VisibleForTesting
    public static boolean KEY_ONLY_SUBSTITUTION_ENABLED =
        Configuration.getInstance().getBooleanWithDefault(
            "ChunkedOperatorAggregationHelper.enableKeyOnlySubstitution", true);

    static final int CHUNK_SIZE = 1 << 12;

    public static QueryTable aggregation(AggregationContextFactory aggregationContextFactory,
        QueryTable queryTable, SelectColumn[] groupByColumns) {
        return aggregation(AggregationControl.DEFAULT_FOR_OPERATOR, aggregationContextFactory,
            queryTable, groupByColumns);
    }

    @VisibleForTesting
    public static QueryTable aggregation(AggregationControl control,
        AggregationContextFactory aggregationContextFactory, QueryTable queryTable,
        SelectColumn[] groupByColumns) {
        final boolean viewRequired = groupByColumns.length > 0
            && Arrays.stream(groupByColumns).anyMatch(selectColumn -> !selectColumn.isRetain());
        final QueryTable withView =
            !viewRequired ? queryTable : (QueryTable) queryTable.updateView(groupByColumns);

        final AggregationContextFactory aggregationContextFactoryToUse;
        if (KEY_ONLY_SUBSTITUTION_ENABLED
            && withView.getDefinition().getColumns().length == groupByColumns.length
            && aggregationContextFactory.allowKeyOnlySubstitution()) {
            aggregationContextFactoryToUse = new KeyOnlyAggregationFactory();
        } else {
            aggregationContextFactoryToUse = aggregationContextFactory;
        }

        if (queryTable.hasAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE)) {
            withView.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE,
                queryTable.getAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE));
        }

        final Mutable<QueryTable> resultHolder = new MutableObject<>();
        final ShiftAwareSwapListener swapListener =
            withView.createSwapListenerIfRefreshing(ShiftAwareSwapListener::new);
        withView.initializeWithSnapshot(
            "by(" + aggregationContextFactoryToUse + ", " + Arrays.toString(groupByColumns) + ")",
            swapListener, (usePrev, beforeClockValue) -> {
                resultHolder.setValue(aggregation(control, swapListener,
                    aggregationContextFactoryToUse, withView, groupByColumns, usePrev));
                return true;
            });
        return resultHolder.getValue();
    }

    private static QueryTable aggregation(AggregationControl control,
        ShiftAwareSwapListener swapListener, AggregationContextFactory aggregationContextFactory,
        QueryTable withView, SelectColumn[] groupByColumns, boolean usePrev) {
        if (groupByColumns.length == 0) {
            return noKeyAggregation(swapListener, aggregationContextFactory, withView, usePrev);
        }

        final String[] keyNames =
            Arrays.stream(groupByColumns).map(SelectColumn::getName).toArray(String[]::new);
        final ColumnSource<?>[] keySources =
            Arrays.stream(keyNames).map(withView::getColumnSource).toArray(ColumnSource[]::new);
        final ColumnSource<?>[] reinterpretedKeySources = Arrays.stream(keySources)
            .map(ReinterpretUtilities::maybeConvertToPrimitive).toArray(ColumnSource[]::new);

        final AggregationContext ac =
            aggregationContextFactory.makeAggregationContext(withView, keyNames);

        final PermuteKernel[] permuteKernels = ac.makePermuteKernels();

        final boolean useGrouping;
        if (control.considerGrouping(withView, keySources)) {
            Assert.eq(keySources.length, "keySources.length", 1);

            final boolean hasGrouping = withView.getIndex().hasGrouping(keySources[0]);
            if (!withView.isRefreshing() && hasGrouping) {
                return staticGroupedAggregation(withView, keyNames[0], keySources[0], ac);
            }
            // we have no hasPrevGrouping method
            useGrouping =
                !usePrev && hasGrouping && Arrays.equals(reinterpretedKeySources, keySources);
        } else {
            useGrouping = false;
        }

        final ChunkedOperatorAggregationStateManager stateManager;
        final IncrementalChunkedOperatorAggregationStateManager incrementalStateManager;
        if (withView.isRefreshing()) {
            stateManager =
                incrementalStateManager = new IncrementalChunkedOperatorAggregationStateManager(
                    reinterpretedKeySources, control.initialHashTableSize(withView),
                    control.getMaximumLoadFactor(), control.getTargetLoadFactor());
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
            initialGroupedKeyAddition(withView, reinterpretedKeySources, ac,
                incrementalStateManager, outputPosition, usePrev);
        } else {
            initialBucketedKeyAddition(withView, reinterpretedKeySources, ac, permuteKernels,
                stateManager, outputPosition, usePrev);
        }

        // Construct and return result table
        final ColumnSource[] keyHashTableSources = stateManager.getKeyHashTableSources();
        final Map<String, ColumnSource<?>> resultColumnSourceMap = new LinkedHashMap<>();

        // Gather the result key columns
        final ColumnSource[] keyColumnsRaw = new ColumnSource[keyHashTableSources.length];
        final ArrayBackedColumnSource[] keyColumnsCopied =
            withView.isRefreshing() ? new ArrayBackedColumnSource[keyHashTableSources.length]
                : null;
        for (int kci = 0; kci < keyHashTableSources.length; ++kci) {
            ColumnSource<?> resultKeyColumnSource = keyHashTableSources[kci];
            if (keySources[kci] != reinterpretedKeySources[kci]) {
                resultKeyColumnSource = ReinterpretUtilities
                    .convertToOriginal(keySources[kci].getType(), resultKeyColumnSource);
            }
            keyColumnsRaw[kci] = resultKeyColumnSource;
            if (withView.isRefreshing()) {
                // noinspection ConstantConditions,unchecked
                keyColumnsCopied[kci] = ArrayBackedColumnSource
                    .getMemoryColumnSource(outputPosition.intValue(), keyColumnsRaw[kci].getType());
                resultColumnSourceMap.put(keyNames[kci], keyColumnsCopied[kci]);
            } else {
                resultColumnSourceMap.put(keyNames[kci], keyColumnsRaw[kci]);
            }
        }
        ac.getResultColumns(resultColumnSourceMap);

        final Index resultIndex = Index.FACTORY.getFlatIndex(outputPosition.intValue());
        if (withView.isRefreshing()) {
            copyKeyColumns(keyColumnsRaw, keyColumnsCopied, resultIndex);
        }

        // Construct the result table
        final QueryTable result = new QueryTable(resultIndex, resultColumnSourceMap);
        ac.propagateInitialStateToOperators(result);

        if (withView.isRefreshing()) {
            assert stateManager instanceof IncrementalChunkedOperatorAggregationStateManager;
            assert keyColumnsCopied != null;

            ac.startTrackingPrevValues();
            incrementalStateManager.startTrackingPrevValues();

            final boolean isStream = withView.isStream();
            final ShiftAwareListener listener = new BaseTable.ShiftAwareListenerImpl(
                "by(" + aggregationContextFactory + ")", withView, result) {
                @ReferentialIntegrity
                final ShiftAwareSwapListener swapListenerHardReference = swapListener;

                final ModifiedColumnSet keysUpstreamModifiedColumnSet =
                    withView.newModifiedColumnSet(keyNames);
                final ModifiedColumnSet[] operatorInputModifiedColumnSets =
                    ac.getInputModifiedColumnSets(withView);
                final UnaryOperator<ModifiedColumnSet>[] resultModifiedColumnSetFactories =
                    ac.initializeRefreshing(result, this);

                @Override
                public void onUpdate(@NotNull final Update upstream) {
                    final Update upstreamToUse = isStream ? adjustForStreaming(upstream) : upstream;
                    if (upstreamToUse.empty()) {
                        return;
                    }
                    final Update downstream;
                    try (final KeyedUpdateContext kuc =
                        new KeyedUpdateContext(ac, incrementalStateManager,
                            reinterpretedKeySources, permuteKernels, keysUpstreamModifiedColumnSet,
                            operatorInputModifiedColumnSets,
                            upstreamToUse, outputPosition)) {
                        downstream = kuc.computeDownstreamIndicesAndCopyKeys(withView.getIndex(),
                            keyColumnsRaw, keyColumnsCopied,
                            result.getModifiedColumnSetForUpdates(),
                            resultModifiedColumnSetFactories);
                    }
                    result.getIndex().update(downstream.added, downstream.removed);
                    result.notifyListeners(downstream);
                }

                @Override
                public void onFailureInternal(@NotNull final Throwable originalException,
                    final UpdatePerformanceTracker.Entry sourceEntry) {
                    ac.propagateFailureToOperators(originalException, sourceEntry);
                    super.onFailureInternal(originalException, sourceEntry);
                }
            };

            swapListener.setListenerAndResult(listener, result);
            result.addParentReference(swapListener);
            // In general, result listeners depend on the swap listener for continued liveness, but
            // most
            // operations handle this by having the result table depend on both (in both a
            // reachability sense
            // and a liveness sense). That said, it is arguably very natural for the result listener
            // to manage
            // the swap listener. We do so in this case because byExternal requires it in order for
            // the
            // sub-tables to continue ticking if the result Table and TableMap are released.
            listener.manage(swapListener);
        }

        return ac.transformResult(result);
    }

    private static Update adjustForStreaming(@NotNull final Update upstream) {
        // Streaming aggregations never have modifies or shifts from their parent:
        Assert.assertion(upstream.modified.empty() && upstream.shifted.empty(),
            "upstream.modified.empty() && upstream.shifted.empty()");
        // Streaming aggregations ignore removes:
        if (upstream.removed.empty()) {
            return upstream;
        }
        return new Update(upstream.added, Index.CURRENT_FACTORY.getEmptyIndex(), upstream.modified,
            upstream.shifted, upstream.modifiedColumnSet);
    }

    private static void setReverseLookupFunction(ColumnSource<?>[] keySources,
        AggregationContext ac, ChunkedOperatorAggregationStateManager stateManager) {
        if (keySources.length == 1) {
            if (keySources[0].getType() == DBDateTime.class) {
                ac.setReverseLookupFunction(key -> stateManager
                    .findPositionForKey(key == null ? null : DBTimeUtils.nanos((DBDateTime) key)));
            } else if (keySources[0].getType() == Boolean.class) {
                ac.setReverseLookupFunction(key -> stateManager
                    .findPositionForKey(BooleanUtils.booleanAsByte((Boolean) key)));
            } else {
                ac.setReverseLookupFunction(stateManager::findPositionForKey);
            }
        } else {
            final List<Consumer<Object[]>> transformers = new ArrayList<>();
            for (int ii = 0; ii < keySources.length; ++ii) {
                if (keySources[ii].getType() == DBDateTime.class) {
                    final int fii = ii;
                    transformers
                        .add(reinterpreted -> reinterpreted[fii] = reinterpreted[fii] == null ? null
                            : DBTimeUtils.nanos((DBDateTime) reinterpreted[fii]));
                } else if (keySources[ii].getType() == Boolean.class) {
                    final int fii = ii;
                    transformers.add(reinterpreted -> reinterpreted[fii] =
                        BooleanUtils.booleanAsByte((Boolean) reinterpreted[fii]));
                }
            }
            if (transformers.isEmpty()) {
                ac.setReverseLookupFunction(
                    sk -> stateManager.findPositionForKey(((SmartKey) sk).values_));
            } else {
                ac.setReverseLookupFunction(key -> {
                    final SmartKey smartKey = (SmartKey) key;
                    final Object[] reinterpreted =
                        Arrays.copyOf(smartKey.values_, smartKey.values_.length);
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
        private final Update upstream; // Not to be mutated
        private final MutableInt outputPosition;

        private final ModifiedColumnSet updateUpstreamModifiedColumnSet; // Not to be mutated
        private final boolean keysModified;
        private final boolean shifted;
        private final boolean processShifts;
        private final OperatorDivision od;

        private final Index.RandomBuilder emptiedStatesBuilder;
        private final Index.RandomBuilder modifiedStatesBuilder;
        private final Index.RandomBuilder reincarnatedStatesBuilder;
        private final boolean[] modifiedOperators;

        private final SafeCloseableList toClose;

        private final IterativeChunkedAggregationOperator.BucketedContext[] bucketedContexts;
        private final IntIntTimsortKernel.IntIntSortKernelContext<KeyIndices, ChunkPositions> sortKernelContext;

        // These are used for all access when only pre- or post-shift (or previous or current) are
        // needed, else for pre-shift/previous
        private final SharedContext sharedContext;
        private final ChunkSource.GetContext[] getContexts;
        private final WritableChunk<Values>[] workingChunks;
        private final WritableLongChunk<KeyIndices> permutedKeyIndices;

        // These are used when post-shift/current values are needed concurrently with
        // pre-shift/previous
        private final SharedContext postSharedContext;
        private final ChunkSource.GetContext[] postGetContexts;
        private final WritableChunk<Values>[] postWorkingChunks;
        private final WritableLongChunk<KeyIndices> postPermutedKeyIndices;

        private final WritableIntChunk<ChunkPositions> runStarts;
        private final WritableIntChunk<ChunkLengths> runLengths;
        private final WritableIntChunk<ChunkPositions> chunkPositions;
        private final WritableIntChunk<KeyIndices> slots;
        private final WritableBooleanChunk<Values> modifiedSlots;
        private final WritableBooleanChunk<Values> slotsModifiedByOperator;

        private final IncrementalChunkedOperatorAggregationStateManager.BuildContext bc;
        private final WritableIntChunk<KeyIndices> reincarnatedSlots;

        private final IncrementalChunkedOperatorAggregationStateManager.ProbeContext pc;
        private final WritableIntChunk<KeyIndices> emptiedSlots;

        private KeyedUpdateContext(@NotNull final AggregationContext ac,
            @NotNull final IncrementalChunkedOperatorAggregationStateManager incrementalStateManager,
            @NotNull final ColumnSource[] reinterpretedKeySources,
            @NotNull final PermuteKernel[] permuteKernels,
            @NotNull final ModifiedColumnSet keysUpstreamModifiedColumnSet,
            @NotNull final ModifiedColumnSet[] operatorInputUpstreamModifiedColumnSets,
            @NotNull final Update upstream,
            @NotNull final MutableInt outputPosition) {
            this.ac = ac;
            this.incrementalStateManager = incrementalStateManager;
            this.reinterpretedKeySources = reinterpretedKeySources;
            this.permuteKernels = permuteKernels;
            this.upstream = upstream;
            this.outputPosition = outputPosition;

            updateUpstreamModifiedColumnSet =
                upstream.modified.isEmpty() ? ModifiedColumnSet.EMPTY : upstream.modifiedColumnSet;
            keysModified =
                updateUpstreamModifiedColumnSet.containsAny(keysUpstreamModifiedColumnSet);
            shifted = upstream.shifted.nonempty();
            processShifts = ac.requiresIndices() && shifted;
            od = new OperatorDivision(ac, upstream.modified.nonempty(),
                updateUpstreamModifiedColumnSet, operatorInputUpstreamModifiedColumnSets);

            final long buildSize =
                Math.max(upstream.added.size(), keysModified ? upstream.modified.size() : 0);
            final long probeSizeForModifies =
                (keysModified || od.anyOperatorHasModifiedInputColumns || ac.requiresIndices())
                    ? upstream.modified.size()
                    : 0;
            final long probeSizeWithoutShifts =
                Math.max(upstream.removed.size(), probeSizeForModifies);
            final long probeSize = processShifts
                ? UpdateSizeCalculator.chunkSize(probeSizeWithoutShifts, upstream.shifted,
                    CHUNK_SIZE)
                : probeSizeWithoutShifts;
            final int buildChunkSize = chunkSize(buildSize);
            final int probeChunkSize = chunkSize(probeSize);
            final int chunkSize = Math.max(buildChunkSize, probeChunkSize);

            emptiedStatesBuilder = Index.FACTORY.getRandomBuilder();
            modifiedStatesBuilder = Index.FACTORY.getRandomBuilder();
            reincarnatedStatesBuilder = Index.FACTORY.getRandomBuilder();
            modifiedOperators = new boolean[ac.size()];

            toClose = new SafeCloseableList();

            bucketedContexts = toClose
                .addArray(new IterativeChunkedAggregationOperator.BucketedContext[ac.size()]);
            ac.initializeBucketedContexts(bucketedContexts, upstream, keysModified,
                od.operatorsWithModifiedInputColumns);
            sortKernelContext = toClose.add(IntIntTimsortKernel.createContext(chunkSize));

            sharedContext = toClose.add(SharedContext.makeSharedContext());
            getContexts = toClose.addArray(new ChunkSource.GetContext[ac.size()]);
            ac.initializeGetContexts(sharedContext, getContexts, chunkSize);
            // noinspection unchecked
            workingChunks = toClose.addArray(new WritableChunk[ac.size()]);
            ac.initializeWorkingChunks(workingChunks, chunkSize);
            permutedKeyIndices = ac.requiresIndices() || keysModified
                ? toClose.add(WritableLongChunk.makeWritableChunk(chunkSize))
                : null;

            postPermutedKeyIndices = processShifts || keysModified // Note that we need this for
                                                                   // modified keys because we use
                                                                   // it to hold removed key indices
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
            slotsModifiedByOperator =
                toClose.add(WritableBooleanChunk.makeWritableChunk(chunkSize));

            if (buildSize > 0) {
                bc = toClose.add(
                    incrementalStateManager.makeBuildContext(reinterpretedKeySources, buildSize));
                reincarnatedSlots = toClose.add(WritableIntChunk.makeWritableChunk(buildChunkSize));
            } else {
                bc = null;
                reincarnatedSlots = null;
            }
            if (probeSize > 0) {
                pc = toClose.add(
                    incrementalStateManager.makeProbeContext(reinterpretedKeySources, probeSize));
            } else {
                pc = null;
            }
            if (upstream.removed.nonempty() || keysModified) {
                emptiedSlots = toClose.add(WritableIntChunk.makeWritableChunk(probeChunkSize));
            } else {
                emptiedSlots = null;
            }
        }

        @Override
        public final void close() {
            toClose.close();
        }

        private Update computeDownstreamIndicesAndCopyKeys(
            @NotNull final ReadOnlyIndex upstreamIndex,
            @NotNull final ColumnSource<?>[] keyColumnsRaw,
            @NotNull final WritableSource<?>[] keyColumnsCopied,
            @NotNull final ModifiedColumnSet resultModifiedColumnSet,
            @NotNull final UnaryOperator<ModifiedColumnSet>[] resultModifiedColumnSetFactories) {
            final int previousLastState = outputPosition.intValue();
            ac.resetOperatorsForStep(upstream);

            if (upstream.removed.nonempty()) {
                doRemoves(upstream.removed);
            }
            if (upstream.modified.nonempty() && (od.anyOperatorHasModifiedInputColumns
                || od.anyOperatorWithoutModifiedInputColumnsRequiresIndices || keysModified)) {
                try (final ModifySplitResult split =
                    keysModified ? splitKeyModificationsAndDoKeyChangeRemoves() : null) {
                    if (processShifts) {
                        try (final Index postShiftIndex = upstreamIndex.minus(upstream.added)) {
                            if (keysModified) {
                                postShiftIndex.remove(split.keyChangeIndicesPostShift);
                            }
                            doShifts(postShiftIndex); // Also handles shifted same-key modifications
                                                      // for modified-input operators that require
                                                      // indices (if any)
                        }
                        try (final ReadOnlyIndex keysSameUnshiftedModifies =
                            keysModified ? null : getUnshiftedModifies()) {
                            // Do unshifted modifies for everyone
                            assert !keysModified || split.unshiftedSameSlotIndices != null;
                            final ReadOnlyIndex unshiftedSameSlotModifies =
                                keysModified ? split.unshiftedSameSlotIndices
                                    : keysSameUnshiftedModifies;
                            doSameSlotModifies(unshiftedSameSlotModifies, unshiftedSameSlotModifies,
                                true /*
                                      * We don't process shifts unless some operator requires
                                      * indices
                                      */,
                                od.operatorsWithModifiedInputColumns,
                                od.operatorsWithoutModifiedInputColumnsThatRequireIndices);

                            if (od.anyOperatorWithModifiedInputColumnsIgnoresIndices) {
                                // Do shifted same-key modifies for index-only and modified-input
                                // operators that don't require indices
                                try (
                                    final ReadOnlyIndex removeIndex =
                                        keysModified
                                            ? unshiftedSameSlotModifies
                                                .union(split.keyChangeIndicesPostShift)
                                            : null;
                                    final ReadOnlyIndex shiftedSameSlotModifiesPost =
                                        upstream.modified
                                            .minus(removeIndex == null ? unshiftedSameSlotModifies
                                                : removeIndex);
                                    final Index shiftedSameSlotModifiesPre =
                                        shiftedSameSlotModifiesPost.clone()) {
                                    upstream.shifted.unapply(shiftedSameSlotModifiesPre);
                                    doSameSlotModifies(shiftedSameSlotModifiesPre,
                                        shiftedSameSlotModifiesPost, true,
                                        od.operatorsWithModifiedInputColumnsThatIgnoreIndices,
                                        od.operatorsThatRequireIndices);
                                }
                            } else if (ac.requiresIndices()) {
                                // Do shifted same-key modifies for index-only operators
                                try (final Index shiftedSameSlotModifiesPost =
                                    upstream.modified.minus(unshiftedSameSlotModifies)) {
                                    if (keysModified) {
                                        shiftedSameSlotModifiesPost
                                            .remove(split.keyChangeIndicesPostShift);
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
                            keysModified ? split.sameSlotIndicesPreShift
                                : upstream.getModifiedPreShift(),
                            keysModified ? split.sameSlotIndicesPostShift : upstream.modified,
                            ac.requiresIndices(),
                            od.operatorsWithModifiedInputColumns,
                            od.operatorsWithoutModifiedInputColumnsThatRequireIndices);

                    } else {
                        assert !keysModified || split.sameSlotIndicesPostShift != null;
                        doSameSlotModifyIndicesOnly(
                            keysModified ? split.sameSlotIndicesPostShift : upstream.modified,
                            od.operatorsWithoutModifiedInputColumnsThatRequireIndices);
                    }
                    if (keysModified) {
                        doInserts(split.keyChangeIndicesPostShift, false);
                    }
                }
            } else if (processShifts) {
                try (final Index postShiftIndex = upstreamIndex.minus(upstream.added)) {
                    doShifts(postShiftIndex);
                }
            }
            if (upstream.added.nonempty()) {
                doInserts(upstream.added, true);
            }

            final Update downstream = new Update();
            downstream.shifted = IndexShiftData.EMPTY;

            try (final ReadOnlyIndex newStates =
                makeNewStatesIndex(previousLastState, outputPosition.intValue() - 1)) {
                downstream.added = reincarnatedStatesBuilder.getIndex();
                downstream.removed = emptiedStatesBuilder.getIndex();

                try (final Index addedBack = downstream.added.intersect(downstream.removed)) {
                    downstream.added.remove(addedBack);
                    downstream.removed.remove(addedBack);

                    if (newStates.nonempty()) {
                        downstream.added.insert(newStates);
                        copyKeyColumns(keyColumnsRaw, keyColumnsCopied, newStates);
                    }

                    downstream.modified = modifiedStatesBuilder.getIndex();
                    downstream.modified.remove(downstream.added);
                    downstream.modified.remove(downstream.removed);
                    if (ac.addedBackModified()) {
                        downstream.modified.insert(addedBack);
                    }
                }

                ac.propagateChangesToOperators(downstream, newStates);
            }

            extractDownstreamModifiedColumnSet(downstream, resultModifiedColumnSet,
                modifiedOperators, updateUpstreamModifiedColumnSet,
                resultModifiedColumnSetFactories);

            return downstream;
        }

        private void doRemoves(@NotNull final OrderedKeys keyIndicesToRemove) {
            if (keyIndicesToRemove.isEmpty()) {
                return;
            }
            try (final OrderedKeys.Iterator keyIndicesToRemoveIterator =
                keyIndicesToRemove.getOrderedKeysIterator()) {
                while (keyIndicesToRemoveIterator.hasMore()) {
                    doRemovesForChunk(
                        keyIndicesToRemoveIterator.getNextOrderedKeysWithLength(CHUNK_SIZE));
                }
            }
        }

        private void doRemovesForChunk(@NotNull final OrderedKeys keyIndicesToRemoveChunk) {

            incrementalStateManager.remove(pc, keyIndicesToRemoveChunk, reinterpretedKeySources,
                slots, emptiedSlots);
            emptiedStatesBuilder.addKeyIndicesChunk(emptiedSlots);

            propagateRemovesToOperators(keyIndicesToRemoveChunk, slots);
        }

        private void propagateRemovesToOperators(@NotNull final OrderedKeys keyIndicesToRemoveChunk,
            @NotNull final WritableIntChunk<KeyIndices> slotsToRemoveFrom) {
            findSlotRuns(sortKernelContext, runStarts, runLengths, chunkPositions,
                slotsToRemoveFrom);

            if (ac.requiresIndices()) {
                final LongChunk<OrderedKeyIndices> keyIndices =
                    keyIndicesToRemoveChunk.asKeyIndicesChunk();
                LongPermuteKernel.permuteInput(keyIndices, chunkPositions, permutedKeyIndices);
            }

            boolean anyOperatorModified = false;
            boolean firstOperator = true;
            setFalse(modifiedSlots, runStarts.size());

            sharedContext.reset();
            for (int oi = 0; oi < ac.size(); ++oi) {
                if (!firstOperator) {
                    setFalse(slotsModifiedByOperator, runStarts.size());
                }

                final int inputSlot = ac.inputSlot(oi);
                if (oi == inputSlot) {
                    getAndPermuteChunk(ac.inputColumns[oi], getContexts[oi],
                        keyIndicesToRemoveChunk, true, permuteKernels[oi], chunkPositions,
                        workingChunks[oi]);
                }
                ac.operators[oi].removeChunk(bucketedContexts[oi],
                    inputSlot >= 0 ? workingChunks[inputSlot] : null, permutedKeyIndices,
                    slotsToRemoveFrom, runStarts, runLengths,
                    firstOperator ? modifiedSlots : slotsModifiedByOperator);

                anyOperatorModified = updateModificationState(modifiedOperators, modifiedSlots,
                    slotsModifiedByOperator, anyOperatorModified, firstOperator, oi);
                firstOperator = false;
            }

            if (anyOperatorModified) {
                modifySlots(modifiedStatesBuilder, runStarts, slotsToRemoveFrom, modifiedSlots);
            }
        }

        private void doInserts(@NotNull final OrderedKeys keyIndicesToInsert,
            final boolean addToStateManager) {
            if (keyIndicesToInsert.isEmpty()) {
                return;
            }
            try (final OrderedKeys.Iterator keyIndicesToInsertIterator =
                keyIndicesToInsert.getOrderedKeysIterator()) {
                while (keyIndicesToInsertIterator.hasMore()) {
                    doInsertsForChunk(
                        keyIndicesToInsertIterator.getNextOrderedKeysWithLength(CHUNK_SIZE),
                        addToStateManager);
                }
            }
        }

        private void doInsertsForChunk(@NotNull final OrderedKeys keyIndicesToInsertChunk,
            final boolean addToStateManager) {
            if (addToStateManager) {
                incrementalStateManager.addForUpdate(bc, keyIndicesToInsertChunk,
                    reinterpretedKeySources, outputPosition, slots, reincarnatedSlots);
                reincarnatedStatesBuilder.addKeyIndicesChunk(reincarnatedSlots);
            } else {
                incrementalStateManager.findModifications(pc, keyIndicesToInsertChunk,
                    reinterpretedKeySources, slots);
            }

            propagateInsertsToOperators(keyIndicesToInsertChunk, slots);
        }

        private void propagateInsertsToOperators(@NotNull final OrderedKeys keyIndicesToInsertChunk,
            @NotNull final WritableIntChunk<KeyIndices> slotsToAddTo) {
            ac.ensureCapacity(outputPosition.intValue());

            findSlotRuns(sortKernelContext, runStarts, runLengths, chunkPositions, slotsToAddTo);

            if (ac.requiresIndices()) {
                final LongChunk<OrderedKeyIndices> keyIndices =
                    keyIndicesToInsertChunk.asKeyIndicesChunk();
                permutedKeyIndices.setSize(keyIndices.size());
                LongPermuteKernel.permuteInput(keyIndices, chunkPositions, permutedKeyIndices);
            }

            boolean anyOperatorModified = false;
            boolean firstOperator = true;
            setFalse(modifiedSlots, runStarts.size());

            sharedContext.reset();
            for (int oi = 0; oi < ac.size(); ++oi) {
                if (!firstOperator) {
                    setFalse(slotsModifiedByOperator, runStarts.size());
                }

                final int inputSlot = ac.inputSlot(oi);
                if (inputSlot == oi) {
                    getAndPermuteChunk(ac.inputColumns[oi], getContexts[oi],
                        keyIndicesToInsertChunk, false, permuteKernels[oi], chunkPositions,
                        workingChunks[oi]);
                }
                ac.operators[oi].addChunk(bucketedContexts[oi],
                    inputSlot >= 0 ? workingChunks[inputSlot] : null, permutedKeyIndices,
                    slotsToAddTo, runStarts, runLengths,
                    firstOperator ? modifiedSlots : slotsModifiedByOperator);

                anyOperatorModified = updateModificationState(modifiedOperators, modifiedSlots,
                    slotsModifiedByOperator, anyOperatorModified, firstOperator, oi);
                firstOperator = false;
            }

            if (anyOperatorModified) {
                modifySlots(modifiedStatesBuilder, runStarts, slotsToAddTo, modifiedSlots);
            }
        }

        private void doShifts(@NotNull final ReadOnlyIndex postShiftIndexToProcess) {
            if (postShiftIndexToProcess.isEmpty()) {
                return;
            }
            try (
                final WritableLongChunk<OrderedKeyIndices> preKeyIndices =
                    WritableLongChunk.makeWritableChunk(pc.chunkSize);
                final WritableLongChunk<OrderedKeyIndices> postKeyIndices =
                    WritableLongChunk.makeWritableChunk(pc.chunkSize)) {
                final Runnable applyChunkedShift =
                    () -> doProcessShiftBucketed(preKeyIndices, postKeyIndices);
                processUpstreamShifts(upstream, postShiftIndexToProcess, preKeyIndices,
                    postKeyIndices, applyChunkedShift);
            }
        }

        private void doProcessShiftBucketed(
            @NotNull final WritableLongChunk<OrderedKeyIndices> preKeyIndices,
            @NotNull final WritableLongChunk<OrderedKeyIndices> postKeyIndices) {

            final boolean[] chunkInitialized = new boolean[ac.size()];

            try (
                final OrderedKeys preShiftChunkKeys = OrderedKeys
                    .wrapKeyIndicesChunkAsOrderedKeys(WritableLongChunk.downcast(preKeyIndices));
                final OrderedKeys postShiftChunkKeys = OrderedKeys
                    .wrapKeyIndicesChunkAsOrderedKeys(WritableLongChunk.downcast(postKeyIndices))) {
                sharedContext.reset();
                postSharedContext.reset();
                Arrays.fill(chunkInitialized, false);

                incrementalStateManager.findModifications(pc, postShiftChunkKeys,
                    reinterpretedKeySources, slots);
                findSlotRuns(sortKernelContext, runStarts, runLengths, chunkPositions, slots);

                permutedKeyIndices.setSize(preKeyIndices.size());
                postPermutedKeyIndices.setSize(postKeyIndices.size());

                LongPermuteKernel.permuteInput(preKeyIndices, chunkPositions, permutedKeyIndices);
                LongPermuteKernel.permuteInput(postKeyIndices, chunkPositions,
                    postPermutedKeyIndices);

                boolean anyOperatorModified = false;
                boolean firstOperator = true;
                setFalse(modifiedSlots, runStarts.size());

                for (int oi = 0; oi < ac.size(); ++oi) {
                    if (!ac.operators[oi].requiresIndices()) {
                        continue;
                    }
                    if (!firstOperator) {
                        setFalse(slotsModifiedByOperator, runStarts.size());
                    }
                    final int inputSlot = ac.inputSlot(oi);
                    if (inputSlot >= 0 && !chunkInitialized[inputSlot]) {
                        getAndPermuteChunk(ac.inputColumns[inputSlot], getContexts[inputSlot],
                            preShiftChunkKeys, true, permuteKernels[inputSlot], chunkPositions,
                            workingChunks[inputSlot]);
                        getAndPermuteChunk(ac.inputColumns[inputSlot], postGetContexts[inputSlot],
                            postShiftChunkKeys, false, permuteKernels[inputSlot], chunkPositions,
                            postWorkingChunks[inputSlot]);
                        chunkInitialized[inputSlot] = true;
                    }
                    ac.operators[oi].shiftChunk(bucketedContexts[oi],
                        inputSlot >= 0 ? workingChunks[inputSlot] : null,
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

        private void doSameSlotModifies(@NotNull final OrderedKeys preShiftKeyIndicesToModify,
            @NotNull final OrderedKeys postShiftKeyIndicesToModify,
            final boolean supplyPostIndices, @NotNull final boolean[] operatorsToProcess,
            @NotNull final boolean[] operatorsToProcessIndicesOnly) {
            final boolean shifted = preShiftKeyIndicesToModify != postShiftKeyIndicesToModify;
            try (
                final OrderedKeys.Iterator preShiftIterator =
                    preShiftKeyIndicesToModify.getOrderedKeysIterator();
                final OrderedKeys.Iterator postShiftIterator =
                    shifted ? postShiftKeyIndicesToModify.getOrderedKeysIterator() : null) {
                final boolean[] chunkInitialized = new boolean[ac.size()];
                while (preShiftIterator.hasMore()) {
                    final OrderedKeys preShiftKeyIndicesChunk =
                        preShiftIterator.getNextOrderedKeysWithLength(CHUNK_SIZE);
                    final OrderedKeys postShiftKeyIndicesChunk =
                        shifted ? postShiftIterator.getNextOrderedKeysWithLength(CHUNK_SIZE)
                            : preShiftKeyIndicesChunk;
                    sharedContext.reset();
                    postSharedContext.reset();
                    Arrays.fill(chunkInitialized, false);

                    incrementalStateManager.findModifications(pc, postShiftKeyIndicesChunk,
                        reinterpretedKeySources, slots);
                    findSlotRuns(sortKernelContext, runStarts, runLengths, chunkPositions, slots);

                    if (supplyPostIndices) {
                        final LongChunk<OrderedKeyIndices> postKeyIndices =
                            postShiftKeyIndicesChunk.asKeyIndicesChunk();
                        permutedKeyIndices.setSize(postKeyIndices.size());
                        LongPermuteKernel.permuteInput(postKeyIndices, chunkPositions,
                            permutedKeyIndices);
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
                            ac.operators[oi].modifyIndices(bucketedContexts[oi], permutedKeyIndices,
                                slots, runStarts, runLengths,
                                firstOperator ? modifiedSlots : slotsModifiedByOperator);
                        } else /* operatorsToProcess[oi] */ {
                            final int inputSlot = ac.inputSlot(oi);
                            if (inputSlot >= 0 && !chunkInitialized[inputSlot]) {
                                getAndPermuteChunk(ac.inputColumns[inputSlot],
                                    getContexts[inputSlot], preShiftKeyIndicesChunk, true,
                                    permuteKernels[inputSlot], chunkPositions,
                                    workingChunks[inputSlot]);
                                getAndPermuteChunk(ac.inputColumns[inputSlot],
                                    postGetContexts[inputSlot], postShiftKeyIndicesChunk, false,
                                    permuteKernels[inputSlot], chunkPositions,
                                    postWorkingChunks[inputSlot]);
                                chunkInitialized[inputSlot] = true;
                            }

                            ac.operators[oi].modifyChunk(bucketedContexts[oi],
                                inputSlot >= 0 ? workingChunks[inputSlot] : null,
                                inputSlot >= 0 ? postWorkingChunks[inputSlot] : null,
                                permutedKeyIndices, slots, runStarts, runLengths,
                                firstOperator ? modifiedSlots : slotsModifiedByOperator);
                        }

                        anyOperatorModified =
                            updateModificationState(modifiedOperators, modifiedSlots,
                                slotsModifiedByOperator, anyOperatorModified, firstOperator, oi);
                        firstOperator = false;
                    }

                    if (anyOperatorModified) {
                        modifySlots(modifiedStatesBuilder, runStarts, slots, modifiedSlots);
                    }
                }
            }
        }

        private void doSameSlotModifyIndicesOnly(
            @NotNull final OrderedKeys postShiftKeyIndicesToModify,
            @NotNull final boolean[] operatorsToProcessIndicesOnly) {
            try (final OrderedKeys.Iterator postShiftIterator =
                postShiftKeyIndicesToModify.getOrderedKeysIterator()) {
                while (postShiftIterator.hasMore()) {
                    final OrderedKeys postShiftKeyIndicesChunk =
                        postShiftIterator.getNextOrderedKeysWithLength(CHUNK_SIZE);

                    incrementalStateManager.findModifications(pc, postShiftKeyIndicesChunk,
                        reinterpretedKeySources, slots);
                    findSlotRuns(sortKernelContext, runStarts, runLengths, chunkPositions, slots);

                    final LongChunk<OrderedKeyIndices> postKeyIndices =
                        postShiftKeyIndicesChunk.asKeyIndicesChunk();
                    permutedKeyIndices.setSize(postKeyIndices.size());
                    LongPermuteKernel.permuteInput(postKeyIndices, chunkPositions,
                        permutedKeyIndices);

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

                        ac.operators[oi].modifyIndices(bucketedContexts[oi], permutedKeyIndices,
                            slots, runStarts, runLengths,
                            firstOperator ? modifiedSlots : slotsModifiedByOperator);

                        anyOperatorModified =
                            updateModificationState(modifiedOperators, modifiedSlots,
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
             * This is a partition of same-slot modifies for index keys that were not shifted.
             * Needed for modifyChunk of input-modified operators that require indices, since they
             * handle the shifted same-slot modifies in shiftChunk.
             */
            @Nullable
            private final ReadOnlyIndex unshiftedSameSlotIndices;
            /**
             * This is all of same-slot modified, with index keys in pre-shift space. Needed for
             * modifyChunk of input-modified operators that don't require indices.
             */
            @Nullable
            private final ReadOnlyIndex sameSlotIndicesPreShift;
            /**
             * This is all of same-slot modified, with index keys in post-shift space. Needed for
             * modifyChunk of input-modified operators that don't require indices, and for
             * modifyIndices of operators that require indices but don't have any inputs modified.
             */
            @Nullable
            private final ReadOnlyIndex sameSlotIndicesPostShift;
            /**
             * This is all key change modifies, with index keys in post-shift space. Needed for
             * addChunk to process key changes for all operators.
             */
            @NotNull
            private final ReadOnlyIndex keyChangeIndicesPostShift;

            private ModifySplitResult(@Nullable final ReadOnlyIndex unshiftedSameSlotIndices,
                @Nullable final ReadOnlyIndex sameSlotIndicesPreShift,
                @Nullable final ReadOnlyIndex sameSlotIndicesPostShift,
                @NotNull final ReadOnlyIndex keyChangeIndicesPostShift) {
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
            final boolean needSameSlotIndicesPreShift =
                !processShifts && od.anyOperatorHasModifiedInputColumns;
            final boolean needSameSlotIndicesPostShift =
                !processShifts && (od.anyOperatorHasModifiedInputColumns
                    || od.anyOperatorWithoutModifiedInputColumnsRequiresIndices || keysModified);

            final Index.SequentialBuilder unshiftedSameSlotIndicesBuilder =
                needUnshiftedSameSlotIndices ? Index.CURRENT_FACTORY.getSequentialBuilder() : null;
            final Index.SequentialBuilder sameSlotIndicesPreShiftBuilder =
                needSameSlotIndicesPreShift ? Index.CURRENT_FACTORY.getSequentialBuilder() : null;
            final Index.SequentialBuilder sameSlotIndicesPostShiftBuilder =
                needSameSlotIndicesPostShift ? Index.CURRENT_FACTORY.getSequentialBuilder() : null;
            final Index.SequentialBuilder keyChangeIndicesPostShiftBuilder =
                Index.CURRENT_FACTORY.getSequentialBuilder();

            try (
                final OrderedKeys.Iterator modifiedPreShiftIterator =
                    upstream.getModifiedPreShift().getOrderedKeysIterator();
                final OrderedKeys.Iterator modifiedPostShiftIterator =
                    shifted ? upstream.modified.getOrderedKeysIterator() : null;
                final WritableIntChunk<KeyIndices> postSlots =
                    WritableIntChunk.makeWritableChunk(bc.chunkSize)) {

                // Hijacking postPermutedKeyIndices because it's not used in this loop; the rename
                // hopefully makes the code much clearer!
                final WritableLongChunk<OrderedKeyIndices> removedKeyIndices =
                    WritableLongChunk.downcast(postPermutedKeyIndices);

                while (modifiedPreShiftIterator.hasMore()) {
                    final OrderedKeys modifiedPreShiftChunk =
                        modifiedPreShiftIterator.getNextOrderedKeysWithLength(CHUNK_SIZE);
                    final OrderedKeys modifiedPostShiftChunk =
                        shifted ? modifiedPostShiftIterator.getNextOrderedKeysWithLength(CHUNK_SIZE)
                            : modifiedPreShiftChunk;

                    incrementalStateManager.remove(pc, modifiedPreShiftChunk,
                        reinterpretedKeySources, slots, emptiedSlots);
                    emptiedStatesBuilder.addKeyIndicesChunk(emptiedSlots);
                    incrementalStateManager.addForUpdate(bc, modifiedPostShiftChunk,
                        reinterpretedKeySources, outputPosition, postSlots, reincarnatedSlots);
                    reincarnatedStatesBuilder.addKeyIndicesChunk(reincarnatedSlots);

                    final LongChunk<OrderedKeyIndices> preShiftIndices =
                        modifiedPreShiftChunk.asKeyIndicesChunk();
                    final LongChunk<OrderedKeyIndices> postShiftIndices =
                        shifted ? modifiedPostShiftChunk.asKeyIndicesChunk() : preShiftIndices;

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
                    slots.setSize(numKeyChanges);
                    removedKeyIndices.setSize(numKeyChanges);
                    try (final OrderedKeys keyIndicesToRemoveChunk =
                        OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(removedKeyIndices)) {
                        propagateRemovesToOperators(keyIndicesToRemoveChunk, slots);
                    }
                }
            }

            return new ModifySplitResult(
                needUnshiftedSameSlotIndices ? unshiftedSameSlotIndicesBuilder.getIndex() : null,
                needSameSlotIndicesPreShift ? sameSlotIndicesPreShiftBuilder.getIndex() : null,
                needSameSlotIndicesPostShift ? sameSlotIndicesPostShiftBuilder.getIndex() : null,
                keyChangeIndicesPostShiftBuilder.getIndex());
        }

        private ReadOnlyIndex getUnshiftedModifies() {
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

    private static ReadOnlyIndex extractUnshiftedModifiesFromUpstream(
        @NotNull final Update upstream) {
        final Index.SequentialBuilder unshiftedModifiesBuilder =
            Index.CURRENT_FACTORY.getSequentialBuilder();

        try (
            final OrderedKeys.Iterator modifiedPreShiftIterator =
                upstream.getModifiedPreShift().getOrderedKeysIterator();
            final OrderedKeys.Iterator modifiedPostShiftIterator =
                upstream.modified.getOrderedKeysIterator()) {
            while (modifiedPreShiftIterator.hasMore()) {
                final OrderedKeys modifiedPreShiftChunk =
                    modifiedPreShiftIterator.getNextOrderedKeysWithLength(CHUNK_SIZE);
                final OrderedKeys modifiedPostShiftChunk =
                    modifiedPostShiftIterator.getNextOrderedKeysWithLength(CHUNK_SIZE);

                final LongChunk<OrderedKeyIndices> preShiftIndices =
                    modifiedPreShiftChunk.asKeyIndicesChunk();
                final LongChunk<OrderedKeyIndices> postShiftIndices =
                    modifiedPostShiftChunk.asKeyIndicesChunk();

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

        return unshiftedModifiesBuilder.getIndex();
    }

    private static void extractDownstreamModifiedColumnSet(@NotNull final Update downstream,
        @NotNull final ModifiedColumnSet resultModifiedColumnSet,
        @NotNull final boolean[] modifiedOperators,
        @NotNull final ModifiedColumnSet updateUpstreamModifiedColumnSet,
        @NotNull final UnaryOperator<ModifiedColumnSet>[] resultModifiedColumnSetFactories) {
        if (downstream.modified.nonempty()) {
            downstream.modifiedColumnSet = resultModifiedColumnSet;
            downstream.modifiedColumnSet.clear();
            for (int oi = 0; oi < modifiedOperators.length; ++oi) {
                if (modifiedOperators[oi]) {
                    downstream.modifiedColumnSet.setAll(resultModifiedColumnSetFactories[oi]
                        .apply(updateUpstreamModifiedColumnSet));
                }
            }
        } else {
            downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
        }
        if (downstream.modifiedColumnSet.empty() && downstream.modified.nonempty()) {
            downstream.modified.close();
            downstream.modified = Index.CURRENT_FACTORY.getEmptyIndex();
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
                operatorsThatRequireIndices[oi] = ac.operators[oi].requiresIndices();
            }

            operatorsWithModifiedInputColumns = new boolean[ac.size()];
            operatorsWithModifiedInputColumnsThatIgnoreIndices = new boolean[ac.size()];
            operatorsWithoutModifiedInputColumnsThatRequireIndices = new boolean[ac.size()];
            boolean anyOperatorHasModifiedInputColumnsTemp = false;
            boolean anyOperatorWithModifiedInputColumnsIgnoresIndicesTemp = false;
            boolean anyOperatorWithoutModifiedInputColumnsRequiresIndicesTemp = false;
            if (upstreamModified) {
                for (int oi = 0; oi < ac.size(); ++oi) {

                    if (updateUpstreamModifiedColumnSet
                        .containsAny(operatorInputUpstreamModifiedColumnSets[oi])) {
                        operatorsWithModifiedInputColumns[oi] = true;
                        anyOperatorHasModifiedInputColumnsTemp = true;
                        if (!ac.operators[oi].requiresIndices()) {
                            operatorsWithModifiedInputColumnsThatIgnoreIndices[oi] = true;
                            anyOperatorWithModifiedInputColumnsIgnoresIndicesTemp = true;
                        }
                    } else if (ac.operators[oi].requiresIndices()) {
                        operatorsWithoutModifiedInputColumnsThatRequireIndices[oi] = true;
                        anyOperatorWithoutModifiedInputColumnsRequiresIndicesTemp = true;
                    }
                }
            }

            anyOperatorHasModifiedInputColumns = anyOperatorHasModifiedInputColumnsTemp;
            anyOperatorWithModifiedInputColumnsIgnoresIndices =
                anyOperatorWithModifiedInputColumnsIgnoresIndicesTemp;
            anyOperatorWithoutModifiedInputColumnsRequiresIndices =
                anyOperatorWithoutModifiedInputColumnsRequiresIndicesTemp;
        }
    }

    private static void processUpstreamShifts(Update upstream, ReadOnlyIndex useIndex,
        WritableLongChunk<OrderedKeyIndices> preKeyIndices,
        WritableLongChunk<OrderedKeyIndices> postKeyIndices, Runnable applyChunkedShift) {
        Index.SearchIterator postOkForward = null;
        Index.SearchIterator postOkReverse = null;

        boolean lastPolarityReversed = false; // the initial value doesn't matter, because we'll
                                              // just have a noop apply in the worst case
        int writePosition = resetWritePosition(lastPolarityReversed, preKeyIndices, postKeyIndices);

        final IndexShiftData.Iterator shiftIt = upstream.shifted.applyIterator();
        while (shiftIt.hasNext()) {
            shiftIt.next();

            final boolean polarityReversed = shiftIt.polarityReversed();
            if (polarityReversed != lastPolarityReversed) {
                // if our polarity changed, we must flush out the shifts that are pending
                maybeApplyChunkedShift(applyChunkedShift, preKeyIndices, postKeyIndices,
                    lastPolarityReversed, writePosition);
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
                            maybeApplyChunkedShift(applyChunkedShift, preKeyIndices, postKeyIndices,
                                polarityReversed, writePosition);
                            writePosition =
                                resetWritePosition(polarityReversed, preKeyIndices, postKeyIndices);
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
                // we can apply these in a forward direction as normal, we just need to accumulate
                // into our key chunks
                if (postOkForward.advance(beginRange)) {
                    long idx;
                    while ((idx = postOkForward.currentValue()) <= endRange) {
                        postKeyIndices.add(idx);
                        preKeyIndices.add(idx - delta);

                        if (postKeyIndices.size() == postKeyIndices.capacity()) {
                            // once we fill a chunk, we must process the shifts
                            maybeApplyChunkedShift(applyChunkedShift, preKeyIndices, postKeyIndices,
                                polarityReversed, writePosition);
                            writePosition =
                                resetWritePosition(polarityReversed, preKeyIndices, postKeyIndices);
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
        maybeApplyChunkedShift(applyChunkedShift, preKeyIndices, postKeyIndices,
            lastPolarityReversed, writePosition);
        // close our iterators
        if (postOkReverse != null) {
            postOkReverse.close();
        }
        if (postOkForward != null) {
            postOkForward.close();
        }
    }

    private static int resetWritePosition(boolean polarityReversed,
        WritableLongChunk<OrderedKeyIndices> preKeyIndices,
        WritableLongChunk<OrderedKeyIndices> postKeyIndices) {
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
        WritableLongChunk<OrderedKeyIndices> preKeyIndices,
        WritableLongChunk<OrderedKeyIndices> postKeyIndices, boolean polarityReversed,
        int writePosition) {
        if (polarityReversed) {
            int chunkSize = postKeyIndices.capacity();
            if (writePosition == chunkSize) {
                return;
            }
            if (writePosition > 0) {
                postKeyIndices.copyFromTypedChunk(postKeyIndices, writePosition, 0,
                    chunkSize - writePosition);
                postKeyIndices.setSize(chunkSize - writePosition);
                if (preKeyIndices != null) {
                    preKeyIndices.copyFromTypedChunk(preKeyIndices, writePosition, 0,
                        chunkSize - writePosition);
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
        IntIntTimsortKernel.IntIntSortKernelContext<KeyIndices, ChunkPositions> sortKernelContext,
        WritableIntChunk<ChunkPositions> runStarts, WritableIntChunk<ChunkLengths> runLengths,
        WritableIntChunk<ChunkPositions> chunkPosition, WritableIntChunk<KeyIndices> slots) {
        chunkPosition.setSize(slots.size());
        ChunkUtils.fillInOrder(chunkPosition);
        IntIntTimsortKernel.sort(sortKernelContext, chunkPosition, slots);
        IntFindRunsKernel.findRunsSingles(slots, runStarts, runLengths);
    }

    /**
     * Get values from the inputColumn, and permute them into workingChunk.
     */
    private static void getAndPermuteChunk(ChunkSource.WithPrev<Values> inputColumn,
        ChunkSource.GetContext getContext, OrderedKeys chunkOk, boolean usePrev,
        PermuteKernel permuteKernel, IntChunk<ChunkPositions> chunkPosition,
        WritableChunk<Values> workingChunk) {
        final Chunk<? extends Values> values;
        if (inputColumn == null) {
            values = null;
        } else if (usePrev) {
            values = inputColumn.getPrevChunk(getContext, chunkOk);
        } else {
            values = inputColumn.getChunk(getContext, chunkOk);
        }

        // permute the chunk based on the chunkPosition, so that we have values from a slot together
        if (values != null) {
            workingChunk.setSize(values.size());
            permuteKernel.permuteInput(values, chunkPosition, workingChunk);
        }
    }

    private static void modifySlots(Index.RandomBuilder modifiedBuilder,
        IntChunk<ChunkPositions> runStarts, WritableIntChunk<KeyIndices> slots,
        BooleanChunk modified) {
        int outIndex = 0;
        for (int runIndex = 0; runIndex < runStarts.size(); ++runIndex) {
            if (modified.get(runIndex)) {
                final int slotStart = runStarts.get(runIndex);
                final int slot = slots.get(slotStart);
                slots.set(outIndex++, slot);
            }
        }
        slots.setSize(outIndex);
        modifiedBuilder.addKeyIndicesChunk(slots);
    }

    @NotNull
    private static QueryTable staticGroupedAggregation(QueryTable withView, String keyName,
        ColumnSource<?> keySource, AggregationContext ac) {
        final Pair<ArrayBackedColumnSource, ObjectArraySource<Index>> groupKeyIndexTable;
        final Map<Object, Index> grouping = withView.getIndex().getGrouping(keySource);
        // noinspection unchecked
        groupKeyIndexTable =
            AbstractColumnSource.groupingToFlatSources((ColumnSource) keySource, grouping);
        final int responsiveGroups = grouping.size();

        final Map<String, ColumnSource<?>> resultColumnSourceMap = new LinkedHashMap<>();
        resultColumnSourceMap.put(keyName, groupKeyIndexTable.first);
        ac.getResultColumns(resultColumnSourceMap);

        doGroupedAddition(ac, groupKeyIndexTable, responsiveGroups);

        final QueryTable result =
            new QueryTable(Index.FACTORY.getFlatIndex(responsiveGroups), resultColumnSourceMap);
        ac.propagateInitialStateToOperators(result);

        final ReverseLookupListener rll =
            ReverseLookupListener.makeReverseLookupListenerWithSnapshot(result, keyName);
        ac.setReverseLookupFunction(k -> (int) rll.get(k));

        return ac.transformResult(result);
    }

    private static void doGroupedAddition(AggregationContext ac,
        Pair<ArrayBackedColumnSource, ObjectArraySource<Index>> groupKeyIndexTable,
        int responsiveGroups) {
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
                    final Index index = groupKeyIndexTable.second.get(ii);
                    for (int oi = 0; oi < ac.size(); ++oi) {
                        ac.operators[oi].addIndex(operatorContexts[oi], index, ii);
                    }
                }
            } else {
                for (int ii = 0; ii < responsiveGroups; ++ii) {
                    final Index index = groupKeyIndexTable.second.get(ii);
                    // noinspection ConstantConditions
                    try (final OrderedKeys.Iterator okit = index.getOrderedKeysIterator()) {
                        // noinspection unchecked
                        final Chunk<? extends Values>[] workingChunks = new Chunk[ac.size()];

                        while (okit.hasMore()) {
                            final OrderedKeys chunkOk =
                                okit.getNextOrderedKeysWithLength(CHUNK_SIZE);
                            final int chunkSize = chunkOk.intSize();
                            final LongChunk<OrderedKeyIndices> keyIndices =
                                indicesRequired ? chunkOk.asKeyIndicesChunk() : null;
                            sharedContext.reset();

                            Arrays.fill(workingChunks, null);

                            for (int oi = 0; oi < ac.size(); ++oi) {
                                final int inputSlot = ac.inputSlot(oi);
                                if (inputSlot == oi) {
                                    workingChunks[inputSlot] = ac.inputColumns[oi] == null ? null
                                        : ac.inputColumns[oi].getChunk(getContexts[oi], chunkOk);
                                }
                                ac.operators[oi].addChunk(operatorContexts[oi], chunkSize,
                                    inputSlot < 0 ? null : workingChunks[inputSlot], keyIndices,
                                    ii);
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
        final ChunkSource.GetContext[] getContexts = new ChunkSource.GetContext[ac.size()];
        // noinspection unchecked
        final WritableChunk<Values>[] workingChunks = new WritableChunk[ac.size()];
        final IterativeChunkedAggregationOperator.BucketedContext[] bucketedContexts =
            new IterativeChunkedAggregationOperator.BucketedContext[ac.size()];

        final ColumnSource<?>[] buildSources;
        if (usePrev) {
            buildSources = Arrays.stream(reinterpretedKeySources)
                .map((UnaryOperator<ColumnSource<?>>) PrevColumnSource::new)
                .toArray(ColumnSource[]::new);
        } else {
            buildSources = reinterpretedKeySources;
        }

        final Index index = usePrev ? withView.getIndex().getPrevIndex() : withView.getIndex();

        if (index.isEmpty()) {
            return;
        }

        final int chunkSize = chunkSize(index.size());

        try (
            final SafeCloseable bc =
                stateManager.makeAggregationStateBuildContext(buildSources, chunkSize);
            final SafeCloseable ignored1 = usePrev ? index : null;
            final SafeCloseable ignored2 = new SafeCloseableArray<>(getContexts);
            final SafeCloseable ignored3 = new SafeCloseableArray<>(workingChunks);
            final SafeCloseable ignored4 = new SafeCloseableArray<>(bucketedContexts);
            final OrderedKeys.Iterator okIt = index.getOrderedKeysIterator();
            final WritableIntChunk<KeyIndices> outputPositions =
                WritableIntChunk.makeWritableChunk(chunkSize);
            final WritableIntChunk<ChunkPositions> chunkPosition =
                WritableIntChunk.makeWritableChunk(chunkSize);
            final SharedContext sharedContext = SharedContext.makeSharedContext();
            final IntIntTimsortKernel.IntIntSortKernelContext<KeyIndices, ChunkPositions> sortKernelContext =
                IntIntTimsortKernel.createContext(chunkSize);
            final WritableIntChunk<ChunkPositions> runStarts =
                WritableIntChunk.makeWritableChunk(chunkSize);
            final WritableIntChunk<ChunkLengths> runLengths =
                WritableIntChunk.makeWritableChunk(chunkSize);
            final WritableLongChunk<KeyIndices> permutedKeyIndices =
                ac.requiresIndices() ? WritableLongChunk.makeWritableChunk(chunkSize) : null;
            final WritableBooleanChunk<Values> unusedModifiedSlots =
                WritableBooleanChunk.makeWritableChunk(chunkSize)) {
            ac.initializeGetContexts(sharedContext, getContexts, chunkSize);
            ac.initializeWorkingChunks(workingChunks, chunkSize);
            ac.initializeBucketedContexts(bucketedContexts, chunkSize);

            while (okIt.hasMore()) {
                final OrderedKeys chunkOk = okIt.getNextOrderedKeysWithLength(chunkSize);
                sharedContext.reset();

                stateManager.add(bc, chunkOk, buildSources, outputPosition, outputPositions);

                ac.ensureCapacity(outputPosition.intValue());

                findSlotRuns(sortKernelContext, runStarts, runLengths, chunkPosition,
                    outputPositions);

                if (permutedKeyIndices != null) {
                    final LongChunk<OrderedKeyIndices> keyIndices = chunkOk.asKeyIndicesChunk();
                    LongPermuteKernel.permuteInput(keyIndices, chunkPosition, permutedKeyIndices);
                }

                for (int ii = 0; ii < ac.size(); ++ii) {
                    final int inputSlot = ac.inputSlot(ii);
                    if (ii == inputSlot) {
                        getAndPermuteChunk(ac.inputColumns[ii], getContexts[ii], chunkOk, usePrev,
                            permuteKernels[ii], chunkPosition, workingChunks[ii]);
                    }
                    ac.operators[ii].addChunk(bucketedContexts[ii],
                        inputSlot >= 0 ? workingChunks[inputSlot] : null, permutedKeyIndices,
                        outputPositions, runStarts, runLengths, unusedModifiedSlots);
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
        final Pair<ArrayBackedColumnSource, ObjectArraySource<Index>> groupKeyIndexTable;
        final Map<Object, Index> grouping =
            usePrev ? withView.getIndex().getPrevGrouping(reinterpretedKeySources[0])
                : withView.getIndex().getGrouping(reinterpretedKeySources[0]);
        // noinspection unchecked
        groupKeyIndexTable = AbstractColumnSource
            .groupingToFlatSources((ColumnSource) reinterpretedKeySources[0], grouping);
        final int responsiveGroups = grouping.size();

        if (responsiveGroups == 0) {
            return;
        }

        ac.ensureCapacity(responsiveGroups);

        final ColumnSource[] groupedFlatKeySource = {groupKeyIndexTable.first};

        try (
            final SafeCloseable bc = stateManager
                .makeAggregationStateBuildContext(groupedFlatKeySource, responsiveGroups);
            final OrderedKeys ok = OrderedKeys.forRange(0, responsiveGroups - 1);
            final OrderedKeys.Iterator okIt = ok.getOrderedKeysIterator();
            final WritableIntChunk<KeyIndices> outputPositions =
                WritableIntChunk.makeWritableChunk(responsiveGroups)) {
            while (okIt.hasMore()) {
                final OrderedKeys chunkOk = okIt.getNextOrderedKeysWithLength(CHUNK_SIZE);
                stateManager.add(bc, chunkOk, groupedFlatKeySource, outputPosition,
                    outputPositions);
            }
            Assert.eq(outputPosition.intValue(), "outputPosition.intValue()", responsiveGroups,
                "responsiveGroups");
        }

        for (int ii = 0; ii < responsiveGroups; ++ii) {
            // noinspection ConstantConditions
            final long groupSize = groupKeyIndexTable.second.get(ii).size();
            stateManager.setRowSize(ii, groupSize);
        }

        doGroupedAddition(ac, groupKeyIndexTable, responsiveGroups);
    }

    private static ReadOnlyIndex makeNewStatesIndex(final int first, final int last) {
        return first > last ? Index.CURRENT_FACTORY.getEmptyIndex()
            : Index.CURRENT_FACTORY.getIndexByRange(first, last);
    }

    private static void copyKeyColumns(ColumnSource<?>[] keyColumnsRaw,
        WritableSource<?>[] keyColumnsCopied, final ReadOnlyIndex copyValues) {
        if (copyValues.isEmpty()) {
            return;
        }
        final int chunkSize = chunkSize(copyValues.size());
        final ColumnSource.GetContext[] getContext =
            new ColumnSource.GetContext[keyColumnsRaw.length];
        final WritableChunkSink.FillFromContext[] fillFromContexts =
            new WritableChunkSink.FillFromContext[keyColumnsRaw.length];
        try (final OrderedKeys.Iterator okit = copyValues.getOrderedKeysIterator();
            final SharedContext sharedContext = SharedContext.makeSharedContext();
            final SafeCloseableArray ignored = new SafeCloseableArray<>(getContext);
            final SafeCloseableArray ignored2 = new SafeCloseableArray<>(fillFromContexts)) {
            for (int ii = 0; ii < keyColumnsRaw.length; ++ii) {
                getContext[ii] = keyColumnsRaw[ii].makeGetContext(chunkSize, sharedContext);
                fillFromContexts[ii] = keyColumnsCopied[ii].makeFillFromContext(chunkSize);
                keyColumnsCopied[ii].ensureCapacity(copyValues.lastKey() + 1);
            }

            while (okit.hasMore()) {
                final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(chunkSize);
                sharedContext.reset();
                for (int ci = 0; ci < keyColumnsRaw.length; ci++) {
                    final Chunk<? extends Values> values =
                        keyColumnsRaw[ci].getChunk(getContext[ci], chunkOk);
                    keyColumnsCopied[ci].fillFromChunk(fillFromContexts[ci], values, chunkOk);
                }
            }
        }
    }

    private static QueryTable noKeyAggregation(ShiftAwareSwapListener swapListener,
        AggregationContextFactory aggregationContextFactory, QueryTable table, boolean usePrev) {

        final AggregationContext ac = aggregationContextFactory.makeAggregationContext(table);
        final Map<String, ColumnSource<?>> resultColumnSourceMap = new LinkedHashMap<>();
        ac.getResultColumns(resultColumnSourceMap);

        final boolean[] allColumns = new boolean[ac.size()];
        Arrays.fill(allColumns, true);

        // We intentionally update the 0 key even if we have no rows, so we'd better have capacity
        // for it.
        ac.ensureCapacity(1);

        // we don't actually care about the modified columns here and it will never set anything to
        // false, so it is safe
        // to use allColumns as the modified columns parameter
        final IterativeChunkedAggregationOperator.SingletonContext[] opContexts =
            new IterativeChunkedAggregationOperator.SingletonContext[ac.size()];
        final Index index = usePrev ? table.getIndex().getPrevIndex() : table.getIndex();
        final int initialResultSize;
        try (final SafeCloseable ignored1 = new SafeCloseableArray<>(opContexts);
            final SafeCloseable ignored2 = usePrev ? index : null) {
            initialResultSize = index.size() == 0 ? 0 : 1;
            ac.initializeSingletonContexts(opContexts, index.size());
            doNoKeyAddition(index, ac, opContexts, allColumns, usePrev, allColumns);
        }

        final QueryTable result =
            new QueryTable(Index.FACTORY.getFlatIndex(initialResultSize), resultColumnSourceMap);
        ac.propagateInitialStateToOperators(result);

        if (table.isRefreshing()) {
            ac.startTrackingPrevValues();

            final boolean isStream = table.isStream();
            final ShiftAwareListener listener = new BaseTable.ShiftAwareListenerImpl(
                "by(" + aggregationContextFactory + ")", table, result) {

                final ModifiedColumnSet[] inputModifiedColumnSet =
                    ac.getInputModifiedColumnSets(table);
                final UnaryOperator<ModifiedColumnSet>[] resultModifiedColumnSetFactories =
                    ac.initializeRefreshing(result, this);

                int lastSize = initialResultSize;
                int statesCreated = initialResultSize;

                @Override
                public void onUpdate(@NotNull final Update upstream) {
                    final Update upstreamToUse = isStream ? adjustForStreaming(upstream) : upstream;
                    if (upstreamToUse.empty()) {
                        return;
                    }
                    processNoKeyUpdate(upstreamToUse);
                }

                private void processNoKeyUpdate(@NotNull final Update upstream) {
                    ac.resetOperatorsForStep(upstream);

                    final ModifiedColumnSet upstreamModifiedColumnSet =
                        upstream.modified.isEmpty() ? ModifiedColumnSet.EMPTY
                            : upstream.modifiedColumnSet;

                    final IterativeChunkedAggregationOperator.SingletonContext[] opContexts =
                        new IterativeChunkedAggregationOperator.SingletonContext[ac.size()];
                    try (final SafeCloseable ignored = new SafeCloseableArray<>(opContexts)) {
                        final OperatorDivision od =
                            new OperatorDivision(ac, upstream.modified.nonempty(),
                                upstreamModifiedColumnSet, inputModifiedColumnSet);
                        ac.initializeSingletonContexts(opContexts, upstream,
                            od.operatorsWithModifiedInputColumns);

                        final boolean[] modifiedOperators = new boolean[ac.size()];
                        // remove all the removals
                        if (upstream.removed.nonempty()) {
                            doNoKeyRemoval(upstream.removed, ac, opContexts, allColumns,
                                modifiedOperators);
                        }

                        final boolean processShifts =
                            upstream.shifted.nonempty() && ac.requiresIndices();

                        if (upstream.modified.nonempty() && (od.anyOperatorHasModifiedInputColumns
                            || od.anyOperatorWithoutModifiedInputColumnsRequiresIndices)) {
                            if (processShifts) {
                                // Also handles shifted modifications for modified-input operators
                                // that require indices (if any)
                                doNoKeyShifts(table, upstream, ac, opContexts,
                                    od.operatorsThatRequireIndices, modifiedOperators);

                                try (final ReadOnlyIndex unshiftedModifies =
                                    extractUnshiftedModifiesFromUpstream(upstream)) {
                                    // Do unshifted modifies for everyone
                                    doNoKeyModifications(unshiftedModifies, unshiftedModifies, ac,
                                        opContexts, true,
                                        od.operatorsWithModifiedInputColumns,
                                        od.operatorsWithoutModifiedInputColumnsThatRequireIndices,
                                        modifiedOperators);

                                    if (od.anyOperatorWithModifiedInputColumnsIgnoresIndices) {
                                        // Do shifted modifies for index-only and modified-input
                                        // operators that don't require indices
                                        try (
                                            final ReadOnlyIndex shiftedModifiesPost =
                                                upstream.modified.minus(unshiftedModifies);
                                            final Index shiftedModifiesPre =
                                                shiftedModifiesPost.clone()) {
                                            upstream.shifted.unapply(shiftedModifiesPre);
                                            doNoKeyModifications(shiftedModifiesPre,
                                                shiftedModifiesPost, ac, opContexts, true,
                                                od.operatorsWithModifiedInputColumnsThatIgnoreIndices,
                                                od.operatorsThatRequireIndices,
                                                modifiedOperators);
                                        }
                                    } else if (ac.requiresIndices()) {
                                        // Do shifted modifies for index-only operators
                                        try (final ReadOnlyIndex shiftedModifiesPost =
                                            upstream.modified.minus(unshiftedModifies)) {
                                            doIndexOnlyNoKeyModifications(shiftedModifiesPost, ac,
                                                opContexts,
                                                od.operatorsThatRequireIndices, modifiedOperators);
                                        }
                                    }
                                }
                            } else if (od.anyOperatorHasModifiedInputColumns) {
                                doNoKeyModifications(upstream.getModifiedPreShift(),
                                    upstream.modified, ac, opContexts, ac.requiresIndices(),
                                    od.operatorsWithModifiedInputColumns,
                                    od.operatorsWithoutModifiedInputColumnsThatRequireIndices,
                                    modifiedOperators);

                            } else {
                                doIndexOnlyNoKeyModifications(upstream.modified, ac, opContexts,
                                    od.operatorsWithoutModifiedInputColumnsThatRequireIndices,
                                    modifiedOperators);
                            }
                        } else if (processShifts) {
                            doNoKeyShifts(table, upstream, ac, opContexts,
                                od.operatorsThatRequireIndices, modifiedOperators);
                        }

                        if (upstream.added.nonempty()) {
                            doNoKeyAddition(upstream.added, ac, opContexts, allColumns, false,
                                modifiedOperators);
                        }

                        final int newResultSize =
                            (!isStream || lastSize == 0) && table.size() == 0 ? 0 : 1;
                        final Update downstream = new Update();
                        downstream.shifted = IndexShiftData.EMPTY;
                        if ((lastSize == 0 && newResultSize == 1)) {
                            downstream.added = Index.FACTORY.getIndexByValues(0);
                            downstream.removed = Index.FACTORY.getEmptyIndex();
                            downstream.modified = Index.FACTORY.getEmptyIndex();
                            result.getIndex().insert(0);
                        } else if (lastSize == 1 && newResultSize == 0) {
                            downstream.added = Index.FACTORY.getEmptyIndex();
                            downstream.removed = Index.FACTORY.getIndexByValues(0);
                            downstream.modified = Index.FACTORY.getEmptyIndex();
                            result.getIndex().remove(0);
                        } else {
                            if (!anyTrue(BooleanChunk.chunkWrap(modifiedOperators))) {
                                return;
                            }
                            downstream.added = Index.FACTORY.getEmptyIndex();
                            downstream.removed = Index.FACTORY.getEmptyIndex();
                            downstream.modified = Index.FACTORY.getIndexByValues(0);
                        }
                        lastSize = newResultSize;

                        final int newStatesCreated = Math.max(statesCreated, newResultSize);
                        try (final ReadOnlyIndex newStates =
                            makeNewStatesIndex(statesCreated, newStatesCreated - 1)) {
                            ac.propagateChangesToOperators(downstream, newStates);
                        }
                        statesCreated = newStatesCreated;

                        extractDownstreamModifiedColumnSet(downstream,
                            result.getModifiedColumnSetForUpdates(), modifiedOperators,
                            upstreamModifiedColumnSet, resultModifiedColumnSetFactories);

                        result.notifyListeners(downstream);
                    }
                }

                @Override
                public void onFailureInternal(@NotNull final Throwable originalException,
                    final UpdatePerformanceTracker.Entry sourceEntry) {
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

    private static void doNoKeyAddition(OrderedKeys index, AggregationContext ac,
        IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
        boolean[] operatorsToProcess, boolean usePrev, boolean[] modifiedOperators) {
        doNoKeyUpdate(index, ac, opContexts, operatorsToProcess, usePrev, false, modifiedOperators);
    }

    private static void doNoKeyRemoval(OrderedKeys index, AggregationContext ac,
        IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
        boolean[] operatorsToProcess, boolean[] modifiedOperators) {
        doNoKeyUpdate(index, ac, opContexts, operatorsToProcess, true, true, modifiedOperators);
    }

    private static void doNoKeyModifications(OrderedKeys preIndex, OrderedKeys postIndex,
        AggregationContext ac, IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
        final boolean supplyPostIndices, @NotNull final boolean[] operatorsToProcess,
        @NotNull final boolean[] operatorsToProcessIndicesOnly,
        @NotNull final boolean[] modifiedOperators) {
        final ColumnSource.GetContext[] preGetContexts = new ColumnSource.GetContext[ac.size()];
        final ColumnSource.GetContext[] postGetContexts = new ColumnSource.GetContext[ac.size()];

        try (final SafeCloseableArray ignored = new SafeCloseableArray<>(preGetContexts);
            final SafeCloseableArray ignored2 = new SafeCloseableArray<>(postGetContexts);
            final SharedContext preSharedContext = SharedContext.makeSharedContext();
            final SharedContext postSharedContext = SharedContext.makeSharedContext();
            final OrderedKeys.Iterator preIt = preIndex.getOrderedKeysIterator();
            final OrderedKeys.Iterator postIt = postIndex.getOrderedKeysIterator()) {
            ac.initializeGetContexts(preSharedContext, preGetContexts, preIndex.size(),
                operatorsToProcess);
            ac.initializeGetContexts(postSharedContext, postGetContexts, postIndex.size(),
                operatorsToProcess);

            final Chunk<? extends Values>[] workingPreChunks = new Chunk[ac.size()];
            final Chunk<? extends Values>[] workingPostChunks = new Chunk[ac.size()];

            while (postIt.hasMore()) {
                final OrderedKeys postChunkOk = postIt.getNextOrderedKeysWithLength(CHUNK_SIZE);
                final OrderedKeys preChunkOk = preIt.getNextOrderedKeysWithLength(CHUNK_SIZE);

                final int chunkSize = postChunkOk.intSize();

                preSharedContext.reset();
                postSharedContext.reset();

                final LongChunk<OrderedKeyIndices> postKeyIndices =
                    supplyPostIndices ? postChunkOk.asKeyIndicesChunk() : null;

                Arrays.fill(workingPreChunks, null);
                Arrays.fill(workingPostChunks, null);

                for (int ii = 0; ii < ac.size(); ++ii) {
                    if (operatorsToProcessIndicesOnly[ii]) {
                        modifiedOperators[ii] |=
                            ac.operators[ii].modifyIndices(opContexts[ii], postKeyIndices, 0);
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
                                workingPreChunks[inputSlot] = ac.inputColumns[inputSlot]
                                    .getPrevChunk(preGetContexts[inputSlot], preChunkOk);
                                workingPostChunks[inputSlot] = ac.inputColumns[inputSlot]
                                    .getChunk(postGetContexts[inputSlot], postChunkOk);
                            }
                            preValues = workingPreChunks[inputSlot];
                            postValues = workingPostChunks[inputSlot];
                        }
                        modifiedOperators[ii] |= ac.operators[ii].modifyChunk(opContexts[ii],
                            chunkSize, preValues, postValues, postKeyIndices, 0);
                    }
                }
            }
        }
    }

    private static void doIndexOnlyNoKeyModifications(@NotNull final OrderedKeys postIndex,
        @NotNull final AggregationContext ac,
        @NotNull final IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
        @NotNull final boolean[] operatorsToProcessIndicesOnly,
        @NotNull final boolean[] modifiedOperators) {
        try (final OrderedKeys.Iterator postIt = postIndex.getOrderedKeysIterator()) {
            while (postIt.hasMore()) {
                final OrderedKeys postChunkOk = postIt.getNextOrderedKeysWithLength(CHUNK_SIZE);
                final LongChunk<OrderedKeyIndices> postKeyIndices = postChunkOk.asKeyIndicesChunk();
                for (int ii = 0; ii < ac.size(); ++ii) {
                    if (operatorsToProcessIndicesOnly[ii]) {
                        modifiedOperators[ii] |=
                            ac.operators[ii].modifyIndices(opContexts[ii], postKeyIndices, 0);
                    }
                }
            }
        }
    }

    private static void doNoKeyUpdate(OrderedKeys index, AggregationContext ac,
        IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
        boolean[] operatorsToProcess, boolean usePrev, boolean remove,
        boolean[] modifiedOperators) {
        final ColumnSource.GetContext[] getContexts = new ColumnSource.GetContext[ac.size()];
        final boolean indicesRequired = ac.requiresIndices(operatorsToProcess);

        try (final SafeCloseableArray ignored = new SafeCloseableArray<>(getContexts);
            final SharedContext sharedContext = SharedContext.makeSharedContext();
            final OrderedKeys.Iterator okIt = index.getOrderedKeysIterator()) {
            ac.initializeGetContexts(sharedContext, getContexts, index.size(), operatorsToProcess);

            // noinspection unchecked
            final Chunk<? extends Values>[] workingChunks = new Chunk[ac.size()];

            // on an empty initial pass we want to go through the operator anyway, so that we
            // initialize things correctly for the aggregation of zero keys
            do {
                final OrderedKeys chunkOk = okIt.getNextOrderedKeysWithLength(CHUNK_SIZE);
                sharedContext.reset();

                final LongChunk<OrderedKeyIndices> keyIndices =
                    indicesRequired ? chunkOk.asKeyIndicesChunk() : null;

                Arrays.fill(workingChunks, null);

                for (int ii = 0; ii < ac.size(); ++ii) {
                    if (!operatorsToProcess[ii]) {
                        continue;
                    }
                    final int inputSlot = ac.inputSlot(ii);

                    if (inputSlot >= 0 && workingChunks[inputSlot] == null) {
                        workingChunks[inputSlot] = fetchValues(usePrev, chunkOk,
                            ac.inputColumns[inputSlot], getContexts[inputSlot]);
                    }

                    modifiedOperators[ii] |= processColumnNoKey(remove, chunkOk,
                        inputSlot >= 0 ? workingChunks[inputSlot] : null, ac.operators[ii],
                        opContexts[ii], keyIndices);
                }
            } while (okIt.hasMore());
        }
    }

    private static void doNoKeyShifts(QueryTable source, Update upstream, AggregationContext ac,
        final IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
        boolean[] operatorsToShift, boolean[] modifiedOperators) {
        final ColumnSource.GetContext[] getContexts = new ColumnSource.GetContext[ac.size()];
        final ColumnSource.GetContext[] postGetContexts = new ColumnSource.GetContext[ac.size()];

        try (final Index useIndex = source.getIndex().minus(upstream.added)) {
            if (useIndex.empty()) {
                return;
            }
            final int chunkSize = chunkSize(useIndex.size());
            try (final SafeCloseableArray ignored = new SafeCloseableArray<>(getContexts);
                final SafeCloseableArray ignored2 = new SafeCloseableArray<>(postGetContexts);
                final SharedContext sharedContext = SharedContext.makeSharedContext();
                final SharedContext postSharedContext = SharedContext.makeSharedContext();
                final WritableLongChunk<OrderedKeyIndices> preKeyIndices =
                    WritableLongChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<OrderedKeyIndices> postKeyIndices =
                    WritableLongChunk.makeWritableChunk(chunkSize)) {
                ac.initializeGetContexts(sharedContext, getContexts, chunkSize, operatorsToShift);
                ac.initializeGetContexts(postSharedContext, postGetContexts, chunkSize,
                    operatorsToShift);

                final Runnable applyChunkedShift = () -> doProcessShiftNoKey(ac, opContexts,
                    operatorsToShift, sharedContext, postSharedContext,
                    getContexts, postGetContexts, preKeyIndices, postKeyIndices, modifiedOperators);
                processUpstreamShifts(upstream, useIndex, preKeyIndices, postKeyIndices,
                    applyChunkedShift);
            }
        }
    }

    private static void doProcessShiftNoKey(AggregationContext ac,
        IterativeChunkedAggregationOperator.SingletonContext[] opContexts,
        boolean[] operatorsToShift, SharedContext sharedContext, SharedContext postSharedContext,
        ColumnSource.GetContext[] getContexts, ColumnSource.GetContext[] postGetContexts,
        WritableLongChunk<OrderedKeyIndices> preKeyIndices,
        WritableLongChunk<OrderedKeyIndices> postKeyIndices,
        boolean[] modifiedOperators) {
        // noinspection unchecked
        final Chunk<? extends Values>[] workingPreChunks = new Chunk[ac.size()];
        // noinspection unchecked
        final Chunk<? extends Values>[] workingPostChunks = new Chunk[ac.size()];

        try (
            final OrderedKeys preChunkOk =
                OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(preKeyIndices);
            final OrderedKeys postChunkOk =
                OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(postKeyIndices)) {
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
                            workingPreChunks[inputSlot] =
                                ac.inputColumns[ii].getPrevChunk(getContexts[ii], preChunkOk);
                            workingPostChunks[inputSlot] =
                                ac.inputColumns[ii].getChunk(postGetContexts[ii], postChunkOk);
                        }
                        previousValues = workingPreChunks[inputSlot];
                        newValues = workingPostChunks[inputSlot];
                    }

                    modifiedOperators[ii] |= ac.operators[ii].shiftChunk(opContexts[ii],
                        previousValues, newValues, preKeyIndices, postKeyIndices, 0);
                }
            }
        }
    }

    private static boolean processColumnNoKey(boolean remove, OrderedKeys chunkOk,
        Chunk<? extends Values> values, IterativeChunkedAggregationOperator operator,
        IterativeChunkedAggregationOperator.SingletonContext opContext,
        LongChunk<? extends KeyIndices> keyIndices) {
        if (remove) {
            return operator.removeChunk(opContext, chunkOk.intSize(), values, keyIndices, 0);
        } else {
            return operator.addChunk(opContext, chunkOk.intSize(), values, keyIndices, 0);
        }
    }

    @Nullable
    private static Chunk<? extends Values> fetchValues(boolean usePrev, OrderedKeys chunkOk,
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
