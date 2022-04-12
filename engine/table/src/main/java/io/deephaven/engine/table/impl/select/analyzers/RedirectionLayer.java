package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.util.*;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.*;

/**
 * A layer that maintains the row redirection for future SelectColumnLayers.
 *
 * {@implNote This class is part of the Deephaven engine, and not intended for direct use.}
 */
final public class RedirectionLayer extends SelectAndViewAnalyzer {
    private final SelectAndViewAnalyzer inner;
    private final TrackingRowSet resultRowSet;
    private final WritableRowRedirection rowRedirection;
    private final WritableRowSet freeValues = RowSetFactory.empty();
    private long maxInnerIndex;

    RedirectionLayer(SelectAndViewAnalyzer inner, TrackingRowSet resultRowSet, WritableRowRedirection rowRedirection) {
        super(REDIRECTION_LAYER_INDEX);
        Assert.eq(inner.getLayerIndex(), "inner.getLayerIndex()", BASE_LAYER_INDEX);
        this.inner = inner;
        this.resultRowSet = resultRowSet;
        this.rowRedirection = rowRedirection;
        this.maxInnerIndex = -1;
    }

    @Override
    int getLayerIndexFor(String column) {
        return inner.getLayerIndexFor(column);
    }

    @Override
    void setBaseBits(BitSet bitset) {
        inner.setBaseBits(bitset);
        bitset.set(REDIRECTION_LAYER_INDEX);
    }

    @Override
    public void populateModifiedColumnSetRecurse(ModifiedColumnSet mcsBuilder, Set<String> remainingDepsToSatisfy) {
        inner.populateModifiedColumnSetRecurse(mcsBuilder, remainingDepsToSatisfy);
    }

    @Override
    public Map<String, ColumnSource<?>> getColumnSourcesRecurse(GetMode mode) {
        return inner.getColumnSourcesRecurse(mode);
    }

    @Override
    public void applyUpdate(TableUpdate upstream, RowSet toClear, UpdateHelper helper, JobScheduler jobScheduler,
            SelectLayerCompletionHandler onCompletion) {
        final BitSet baseLayerBitSet = new BitSet();
        inner.setBaseBits(baseLayerBitSet);
        inner.applyUpdate(upstream, toClear, helper, jobScheduler,
                new SelectLayerCompletionHandler(baseLayerBitSet, onCompletion) {
                    @Override
                    public void onAllRequiredColumnsCompleted() {
                        // we only have a base layer underneath us, so we do not care about the bitSet; it is always
                        // empty
                        doApplyUpdate(upstream, toClear, helper, onCompletion);
                    }
                });
    }

    private void doApplyUpdate(TableUpdate upstream, RowSet toClear, UpdateHelper helper,
            SelectLayerCompletionHandler onCompletion) {
        // we need to remove the removed values from our row redirection, and add them to our free RowSet; so that
        // updating tables will not consume more space over the course of a day for abandoned rows
        final RowSetBuilderRandom innerToFreeBuilder = RowSetFactory.builderRandom();
        upstream.removed().forAllRowKeys(key -> innerToFreeBuilder.addKey(rowRedirection.remove(key)));
        freeValues.insert(innerToFreeBuilder.build());

        // we have to shift things that have not been removed, this handles the unmodified rows; but also the
        // modified rows need to have their redirections updated for subsequent modified columns
        if (upstream.shifted().nonempty()) {
            try (final RowSet prevRowSet = resultRowSet.copyPrev();
                    final RowSet prevNoRemovals = prevRowSet.minus(upstream.removed())) {
                final MutableObject<RowSet.SearchIterator> forwardIt = new MutableObject<>();

                upstream.shifted().intersect(prevNoRemovals).apply((begin, end, delta) -> {
                    if (delta < 0) {
                        if (forwardIt.getValue() == null) {
                            forwardIt.setValue(prevNoRemovals.searchIterator());
                        }
                        final RowSet.SearchIterator localForwardIt = forwardIt.getValue();
                        if (localForwardIt.advance(begin)) {
                            for (long key = localForwardIt.currentValue(); localForwardIt.currentValue() <= end; key =
                                    localForwardIt.nextLong()) {
                                final long inner = rowRedirection.remove(key);
                                if (inner != RowSequence.NULL_ROW_KEY) {
                                    rowRedirection.put(key + delta, inner);
                                }
                                if (!localForwardIt.hasNext()) {
                                    break;
                                }
                            }
                        }
                    } else {
                        try (final RowSet.SearchIterator reverseIt = prevNoRemovals.reverseIterator()) {
                            if (reverseIt.advance(end)) {
                                for (long key = reverseIt.currentValue(); reverseIt.currentValue() >= begin; key =
                                        reverseIt.nextLong()) {
                                    final long inner = rowRedirection.remove(key);
                                    if (inner != RowSequence.NULL_ROW_KEY) {
                                        rowRedirection.put(key + delta, inner);
                                    }
                                    if (!reverseIt.hasNext()) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                });

                if (forwardIt.getValue() != null) {
                    forwardIt.getValue().close();
                }
            }
        }

        if (upstream.added().isNonempty()) {
            // added is non-empty, so can always remove at least one value from the RowSet (which must be >= 0);
            // if there is no freeValue, this is safe because we'll just remove something from an empty RowSet
            // if there is a freeValue, we'll remove up to that
            // if there are not enough free values, we'll remove all the free values then beyond
            final MutableLong lastAllocated = new MutableLong(0);
            final RowSet.Iterator freeIt = freeValues.iterator();
            upstream.added().forAllRowKeys(outerKey -> {
                final long innerKey = freeIt.hasNext() ? freeIt.nextLong() : ++maxInnerIndex;
                lastAllocated.setValue(innerKey);
                rowRedirection.put(outerKey, innerKey);
            });
            freeValues.removeRange(0, lastAllocated.longValue());
        }

        onCompletion.onLayerCompleted(REDIRECTION_LAYER_INDEX);
    }

    @Override
    public Map<String, Set<String>> calcDependsOnRecurse() {
        return inner.calcDependsOnRecurse();
    }

    @Override
    public SelectAndViewAnalyzer getInner() {
        return inner;
    }

    @Override
    public void updateColumnDefinitionsFromTopLayer(Map<String, ColumnDefinition<?>> columnDefinitions) {
        inner.updateColumnDefinitionsFromTopLayer(columnDefinitions);
    }

    @Override
    public void startTrackingPrev() {
        rowRedirection.startTrackingPrevValues();
        inner.startTrackingPrev();
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{RedirectionLayer").append(", layerIndex=").append(getLayerIndex()).append("}");
    }

    @Override
    public boolean allowCrossColumnParallelization() {
        return inner.allowCrossColumnParallelization();
    }
}
