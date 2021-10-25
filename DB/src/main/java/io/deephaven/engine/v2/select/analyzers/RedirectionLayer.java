package io.deephaven.engine.v2.select.analyzers;

import io.deephaven.engine.tables.ColumnDefinition;
import io.deephaven.engine.v2.ModifiedColumnSet;
import io.deephaven.engine.v2.ShiftAwareListener;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.*;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.*;

/**
 * A layer that maintains the redirection rowSet for future SelectColumnLayers.
 *
 * {@implNote This class is part of the Deephaven engine, and not intended for direct use.}
 */
final public class RedirectionLayer extends SelectAndViewAnalyzer {
    private final SelectAndViewAnalyzer inner;
    private final TrackingMutableRowSet resultRowSet;
    private final RedirectionIndex redirectionIndex;
    private final TrackingMutableRowSet freeValues = RowSetFactoryImpl.INSTANCE.getEmptyRowSet();
    private long maxInnerIndex;

    RedirectionLayer(SelectAndViewAnalyzer inner, TrackingMutableRowSet resultRowSet, RedirectionIndex redirectionIndex) {
        this.inner = inner;
        this.resultRowSet = resultRowSet;
        this.redirectionIndex = redirectionIndex;
        this.maxInnerIndex = -1;
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
    public void applyUpdate(ShiftAwareListener.Update upstream, RowSet toClear, UpdateHelper helper) {
        inner.applyUpdate(upstream, toClear, helper);

        // we need to remove the removed values from our redirection rowSet, and add them to our free rowSet; so that
        // updating tables will not consume more space over the course of a day for abandoned rows
        final RowSetBuilderRandom innerToFreeBuilder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
        upstream.removed.forAllLongs(key -> innerToFreeBuilder.addKey(redirectionIndex.remove(key)));
        freeValues.insert(innerToFreeBuilder.build());

        // we have to shift things that have not been removed, this handles the unmodified rows; but also the
        // modified rows need to have their redirections updated for subsequent modified columns
        if (upstream.shifted.nonempty()) {
            try (final TrackingMutableRowSet prevRowSet = resultRowSet.getPrevRowSet();
                 final TrackingMutableRowSet prevNoRemovals = prevRowSet.minus(upstream.removed)) {
                final MutableObject<TrackingMutableRowSet.SearchIterator> forwardIt = new MutableObject<>();

                upstream.shifted.intersect(prevNoRemovals).apply((begin, end, delta) -> {
                    if (delta < 0) {
                        if (forwardIt.getValue() == null) {
                            forwardIt.setValue(prevNoRemovals.searchIterator());
                        }
                        final TrackingMutableRowSet.SearchIterator localForwardIt = forwardIt.getValue();
                        if (localForwardIt.advance(begin)) {
                            for (long key = localForwardIt.currentValue(); localForwardIt.currentValue() <= end; key =
                                    localForwardIt.nextLong()) {
                                final long inner = redirectionIndex.remove(key);
                                if (inner != TrackingMutableRowSet.NULL_ROW_KEY) {
                                    redirectionIndex.put(key + delta, inner);
                                }
                                if (!localForwardIt.hasNext()) {
                                    break;
                                }
                            }
                        }
                    } else {
                        try (final TrackingMutableRowSet.SearchIterator reverseIt = prevNoRemovals.reverseIterator()) {
                            if (reverseIt.advance(end)) {
                                for (long key = reverseIt.currentValue(); reverseIt.currentValue() >= begin; key =
                                        reverseIt.nextLong()) {
                                    final long inner = redirectionIndex.remove(key);
                                    if (inner != TrackingMutableRowSet.NULL_ROW_KEY) {
                                        redirectionIndex.put(key + delta, inner);
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

        if (upstream.added.nonempty()) {
            // added is non-empty, so can always remove at least one value from the rowSet (which must be >= 0);
            // if there is no freeValue, this is safe because we'll just remove something from an empty rowSet
            // if there is a freeValue, we'll remove up to that
            // if there are not enough free values, we'll remove all the free values then beyond
            final MutableLong lastAllocated = new MutableLong(0);
            final TrackingMutableRowSet.Iterator freeIt = freeValues.iterator();
            upstream.added.forAllLongs(outerKey -> {
                final long innerKey = freeIt.hasNext() ? freeIt.nextLong() : ++maxInnerIndex;
                lastAllocated.setValue(innerKey);
                redirectionIndex.put(outerKey, innerKey);
            });
            freeValues.removeRange(0, lastAllocated.longValue());
        }
    }

    @Override
    public Map<String, Set<String>> calcDependsOnRecurse() {
        return inner.calcDependsOnRecurse();
    }

    @Override
    public SelectAndViewAnalyzer getInner() {
        return inner.getInner();
    }

    @Override
    public void updateColumnDefinitionsFromTopLayer(Map<String, ColumnDefinition<?>> columnDefinitions) {
        inner.updateColumnDefinitionsFromTopLayer(columnDefinitions);
    }

    @Override
    public void startTrackingPrev() {
        redirectionIndex.startTrackingPrevValues();
        inner.startTrackingPrev();
    }
}
