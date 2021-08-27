package io.deephaven.db.v2.select.analyzers;

import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.ReadOnlyIndex;

import java.util.Map;

/**
 * A layer that copies a column from our input to our output.
 *
 * {@implNote This class is part of the Deephaven engine, and not intended for direct use.}
 */
final public class PreserveColumnLayer extends DependencyLayerBase {
    PreserveColumnLayer(SelectAndViewAnalyzer inner, String name, SelectColumn sc, ColumnSource<?> cs, String[] deps,
            ModifiedColumnSet mcsBuilder) {
        super(inner, name, sc, cs, deps, mcsBuilder);
    }

    @Override
    public void applyUpdate(ShiftAwareListener.Update upstream, ReadOnlyIndex toClear, UpdateHelper helper) {
        // Nothing to do at this level, but need to recurse because my inner layers might need to be called (e.g.
        // because they are SelectColumnLayers)
        inner.applyUpdate(upstream, toClear, helper);
    }

    @Override
    Map<String, ColumnSource<?>> getColumnSourcesRecurse(GetMode mode) {
        // our column is not a new column, so we need to make sure that we do not double enable previous tracking
        final Map<String, ColumnSource<?>> result = inner.getColumnSourcesRecurse(mode);
        switch (mode) {
            case New:
                // we have no new sources
                break;
            case Published:
            case All:
                result.put(name, columnSource);
                break;
        }
        return result;
    }

    @Override
    public void startTrackingPrev() {
        // nothing to do, here but the inner needs to be called
        inner.startTrackingPrev();
    }
}
