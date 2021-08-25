package io.deephaven.db.v2.select.analyzers;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.db.v2.utils.RedirectionIndex;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.*;

/**
 * A layer that maintains the redirection index for future SelectColumnLayers.
 *
 * {@implNote This class is part of the Deephaven engine, and not intended for direct use.}
 */
final public class RedirectionLayer extends SelectAndViewAnalyzer {
    private final SelectAndViewAnalyzer inner;
    private final Index resultIndex;
    private final RedirectionIndex redirectionIndex;
    private final Index freeValues = Index.CURRENT_FACTORY.getEmptyIndex();
    private long maxInnerIndex;

    RedirectionLayer(SelectAndViewAnalyzer inner, Index resultIndex,
        RedirectionIndex redirectionIndex) {
        this.inner = inner;
        this.resultIndex = resultIndex;
        this.redirectionIndex = redirectionIndex;
        this.maxInnerIndex = -1;
    }

    @Override
    public void populateModifiedColumnSetRecurse(ModifiedColumnSet mcsBuilder,
        Set<String> remainingDepsToSatisfy) {
        inner.populateModifiedColumnSetRecurse(mcsBuilder, remainingDepsToSatisfy);
    }

    @Override
    public Map<String, ColumnSource> getColumnSourcesRecurse(GetMode mode) {
        return inner.getColumnSourcesRecurse(mode);
    }

    @Override
    public void applyUpdate(ShiftAwareListener.Update upstream, ReadOnlyIndex toClear,
        UpdateHelper helper) {
        inner.applyUpdate(upstream, toClear, helper);

        // we need to remove the removed values from our redirection index, and add them to our free
        // index; so that
        // updating tables will not consume more space over the course of a day for abandoned rows
        final Index.RandomBuilder innerToFreeBuilder = Index.CURRENT_FACTORY.getRandomBuilder();
        upstream.removed
            .forAllLongs(key -> innerToFreeBuilder.addKey(redirectionIndex.remove(key)));
        freeValues.insert(innerToFreeBuilder.getIndex());

        // we have to shift things that have not been removed, this handles the unmodified rows; but
        // also the
        // modified rows need to have their redirections updated for subsequent modified columns
        if (upstream.shifted.nonempty()) {
            try (final Index prevIndex = resultIndex.getPrevIndex();
                final Index prevNoRemovals = prevIndex.minus(upstream.removed)) {
                final MutableObject<Index.SearchIterator> forwardIt = new MutableObject<>();

                upstream.shifted.intersect(prevNoRemovals).apply((begin, end, delta) -> {
                    if (delta < 0) {
                        if (forwardIt.getValue() == null) {
                            forwardIt.setValue(prevNoRemovals.searchIterator());
                        }
                        final Index.SearchIterator localForwardIt = forwardIt.getValue();
                        if (localForwardIt.advance(begin)) {
                            for (long key = localForwardIt.currentValue(); localForwardIt
                                .currentValue() <= end; key = localForwardIt.nextLong()) {
                                final long inner = redirectionIndex.remove(key);
                                if (inner != Index.NULL_KEY) {
                                    redirectionIndex.put(key + delta, inner);
                                }
                                if (!localForwardIt.hasNext()) {
                                    break;
                                }
                            }
                        }
                    } else {
                        try (final Index.SearchIterator reverseIt =
                            prevNoRemovals.reverseIterator()) {
                            if (reverseIt.advance(end)) {
                                for (long key = reverseIt.currentValue(); reverseIt
                                    .currentValue() >= begin; key = reverseIt.nextLong()) {
                                    final long inner = redirectionIndex.remove(key);
                                    if (inner != Index.NULL_KEY) {
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
            // added is non-empty, so can always remove at least one value from the index (which
            // must be >= 0);
            // if there is no freeValue, this is safe because we'll just remove something from an
            // empty index
            // if there is a freeValue, we'll remove up to that
            // if there are not enough free values, we'll remove all the free values then beyond
            final MutableLong lastAllocated = new MutableLong(0);
            final Index.Iterator freeIt = freeValues.iterator();
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
    public void updateColumnDefinitionsFromTopLayer(
        Map<String, ColumnDefinition> columnDefinitions) {
        inner.updateColumnDefinitionsFromTopLayer(columnDefinitions);
    }

    @Override
    public void startTrackingPrev() {
        redirectionIndex.startTrackingPrevValues();
        inner.startTrackingPrev();
    }
}
