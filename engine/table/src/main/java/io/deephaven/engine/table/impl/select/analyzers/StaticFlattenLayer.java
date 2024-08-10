//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.WrappedRowSetRowRedirection;
import io.deephaven.engine.table.impl.util.JobScheduler;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final public class StaticFlattenLayer extends SelectAndViewAnalyzer.Layer {
    private final TrackingRowSet parentRowSet;
    private final Map<String, ColumnSource<?>> overriddenColumns;

    StaticFlattenLayer(
            final SelectAndViewAnalyzer analyzer,
            final TrackingRowSet parentRowSet,
            final Map<String, ColumnSource<?>> allColumnSources) {
        super(analyzer.getNextLayerIndex());
        this.parentRowSet = parentRowSet;
        final HashSet<String> alreadyFlattenedColumns = new HashSet<>();
        analyzer.getNewColumnSources().forEach((name, cs) -> {
            alreadyFlattenedColumns.add(name);
        });

        final RowRedirection rowRedirection = new WrappedRowSetRowRedirection(parentRowSet);
        overriddenColumns = new HashMap<>();
        allColumnSources.forEach((name, cs) -> {
            if (alreadyFlattenedColumns.contains(name)) {
                return;
            }

            overriddenColumns.put(name, RedirectedColumnSource.maybeRedirect(rowRedirection, cs));
        });
    }

    @Override
    Set<String> getLayerColumnNames() {
        return Set.of();
    }

    @Override
    void populateModifiedColumnSetInReverse(ModifiedColumnSet mcsBuilder, Set<String> remainingDepsToSatisfy) {
        // we don't have any dependencies, so we don't need to do anything here
    }

    @Override
    boolean allowCrossColumnParallelization() {
        return true;
    }

    @Override
    void populateColumnSources(
            final Map<String, ColumnSource<?>> result,
            final GetMode mode) {
        // for each overridden column replace it in the result map
        for (Map.Entry<String, ColumnSource<?>> entry : overriddenColumns.entrySet()) {
            final String columnName = entry.getKey();
            if (result.containsKey(columnName)) {
                result.put(columnName, entry.getValue());
            }
        }
    }

    @Override
    void calcDependsOn(
            final Map<String, Set<String>> result,
            final boolean forcePublishAllSources) {
        // we don't have any dependencies, so we don't need to do anything here
    }

    @Override
    public CompletionHandler createUpdateHandler(
            final TableUpdate upstream,
            final RowSet toClear,
            final SelectAndViewAnalyzer.UpdateHelper helper,
            final JobScheduler jobScheduler,
            @Nullable final LivenessNode liveResultOwner,
            final CompletionHandler onCompletion) {
        // this must be the fake update used to initialize the result table
        Assert.eqTrue(upstream.added().isFlat(), "upstream.added.isFlat()");
        Assert.eq(upstream.added().size(), "upstream.added.size()", parentRowSet.size(), "parentRowSet.size()");
        Assert.eqTrue(upstream.removed().isEmpty(), "upstream.removed.isEmpty()");
        Assert.eqTrue(upstream.modified().isEmpty(), "upstream.modified.isEmpty()");

        final BitSet baseLayerBitSet = new BitSet();
        baseLayerBitSet.set(BASE_LAYER_INDEX);
        return new CompletionHandler(baseLayerBitSet, onCompletion) {
            @Override
            public void onAllRequiredColumnsCompleted() {
                onCompletion.onLayerCompleted(getLayerIndex());
            }
        };
    }

    public RowSet getParentRowSetCopy() {
        return parentRowSet.copy();
    }

    @Override
    public void startTrackingPrev() {
        throw new UnsupportedOperationException("StaticFlattenLayer supports only non-refreshing scenarios");
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{StaticFlattenLayer").append(", layerIndex=").append(getLayerIndex()).append("}");
    }
}
