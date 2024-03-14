//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.util.JobScheduler;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;

/**
 * A layer that copies a column from our input to our output.
 *
 * {@implNote This class is part of the Deephaven engine, and not intended for direct use.}
 */
final public class PreserveColumnLayer extends DependencyLayerBase {
    private final BitSet dependencyBitSet;

    PreserveColumnLayer(SelectAndViewAnalyzer inner, String name, SelectColumn sc, ColumnSource<?> cs, String[] deps,
            ModifiedColumnSet mcsBuilder) {
        super(inner, name, sc, cs, deps, mcsBuilder);
        this.dependencyBitSet = new BitSet();
        Arrays.stream(deps).mapToInt(inner::getLayerIndexFor).forEach(dependencyBitSet::set);
    }

    @Override
    public void applyUpdate(TableUpdate upstream, RowSet toClear, UpdateHelper helper, JobScheduler jobScheduler,
            @Nullable LivenessNode liveResultOwner, SelectLayerCompletionHandler onCompletion) {
        // Nothing to do at this level, but need to recurse because my inner layers might need to be called (e.g.
        // because they are SelectColumnLayers)
        inner.applyUpdate(upstream, toClear, helper, jobScheduler, liveResultOwner,
                new SelectLayerCompletionHandler(dependencyBitSet, onCompletion) {
                    @Override
                    public void onAllRequiredColumnsCompleted() {
                        // we don't need to do anything specific here
                        onCompletion.onLayerCompleted(getLayerIndex());
                    }
                });
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

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{PreserveColumnLayer: ").append(name).append(", layerIndex=").append(getLayerIndex())
                .append("}");
    }

    @Override
    public boolean flattenedResult() {
        // preserve layer is only flattened if the inner is flattened
        // the "flattenedResult" means that we are flattening the table as part of select. For a pre-existing column, we
        // could not preserve a layer while flattening, but if we are preserving a newly generated column; it is valid
        // for the result to have been flattened as part of select.
        return inner.flattenedResult();
    }

    @Override
    public boolean alreadyFlattenedSources() {
        // a preserve layer is only already flattened if the inner is already flattened
        return inner.alreadyFlattenedSources();
    }

    @Override
    public boolean allowCrossColumnParallelization() {
        return inner.allowCrossColumnParallelization();
    }
}
