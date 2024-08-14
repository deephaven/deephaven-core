//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.util.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Consumer;

/**
 * A layer that maintains the row redirection for future SelectColumnLayers.
 * <p>
 * {@implNote This class is part of the Deephaven engine, and not intended for direct use.}
 */
public final class RedirectionLayer extends SelectAndViewAnalyzer.Layer {
    private final TrackingRowSet resultRowSet;
    private final WritableRowRedirection rowRedirection;
    private final WritableRowSet freeValues = RowSetFactory.empty();
    private final BitSet layerDependencySet = new BitSet();
    private long maxInnerIndex;

    RedirectionLayer(
            final SelectAndViewAnalyzer.AnalyzerContext context,
            final TrackingRowSet resultRowSet,
            final WritableRowRedirection rowRedirection) {
        super(context.getNextLayerIndex());
        this.resultRowSet = resultRowSet;
        this.rowRedirection = rowRedirection;
        this.maxInnerIndex = -1;
    }

    @Override
    Set<String> getLayerColumnNames() {
        return Set.of();
    }

    @Override
    void populateColumnSources(final Map<String, ColumnSource<?>> result) {
        // we don't generate any column sources, so we don't need to do anything here
    }

    @Override
    ModifiedColumnSet getModifiedColumnSet() {
        return ModifiedColumnSet.EMPTY;
    }

    @Override
    BitSet getLayerDependencySet() {
        return layerDependencySet;
    }

    @Override
    boolean allowCrossColumnParallelization() {
        return true;
    }

    @Override
    public Runnable createUpdateHandler(
            final TableUpdate upstream,
            final RowSet toClear,
            final SelectAndViewAnalyzer.UpdateHelper helper,
            final JobScheduler jobScheduler,
            @Nullable final LivenessNode liveResultOwner,
            final Runnable onSuccess,
            final Consumer<Exception> onError) {
        // note that we process this layer directly because all subsequent layers depend on it
        return () -> doApplyUpdate(upstream, onSuccess);
    }

    private void doApplyUpdate(
            final TableUpdate upstream,
            final Runnable onSuccess) {
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
                lastAllocated.set(innerKey);
                rowRedirection.put(outerKey, innerKey);
            });
            freeValues.removeRange(0, lastAllocated.get());
        }

        onSuccess.run();
    }

    @Override
    public void startTrackingPrev() {
        rowRedirection.startTrackingPrevValues();
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{RedirectionLayer").append(", layerIndex=").append(getLayerIndex()).append("}");
    }
}
