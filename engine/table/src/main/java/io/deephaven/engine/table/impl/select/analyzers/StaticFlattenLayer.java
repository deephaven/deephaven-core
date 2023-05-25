/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.WrappedRowSetRowRedirection;
import io.deephaven.engine.table.impl.util.JobScheduler;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

final public class StaticFlattenLayer extends SelectAndViewAnalyzer {
    private final SelectAndViewAnalyzer inner;
    private final TrackingRowSet parentRowSet;
    private final Map<String, ColumnSource<?>> overriddenColumns;

    StaticFlattenLayer(SelectAndViewAnalyzer inner, TrackingRowSet parentRowSet) {
        super(inner.getLayerIndex() + 1);
        this.inner = inner;
        this.parentRowSet = parentRowSet;
        final HashSet<String> alreadyFlattenedColumns = new HashSet<>();
        inner.getNewColumnSources().forEach((name, cs) -> {
            alreadyFlattenedColumns.add(name);
        });

        final RowRedirection rowRedirection = new WrappedRowSetRowRedirection(parentRowSet);
        overriddenColumns = new HashMap<>();
        inner.getAllColumnSources().forEach((name, cs) -> {
            if (alreadyFlattenedColumns.contains(name)) {
                return;
            }

            overriddenColumns.put(name, RedirectedColumnSource.maybeRedirect(rowRedirection, cs));
        });
    }

    @Override
    void setBaseBits(BitSet bitset) {
        inner.setBaseBits(bitset);
    }

    @Override
    void populateModifiedColumnSetRecurse(ModifiedColumnSet mcsBuilder, Set<String> remainingDepsToSatisfy) {
        inner.populateModifiedColumnSetRecurse(mcsBuilder, remainingDepsToSatisfy);
    }

    @Override
    Map<String, ColumnSource<?>> getColumnSourcesRecurse(GetMode mode) {
        final Map<String, ColumnSource<?>> innerColumns = inner.getColumnSourcesRecurse(mode);

        if (overriddenColumns.keySet().stream().noneMatch(innerColumns::containsKey)) {
            return innerColumns;
        }

        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        innerColumns.forEach((name, cs) -> columns.put(name, overriddenColumns.getOrDefault(name, cs)));
        return columns;
    }

    @Override
    public void applyUpdate(TableUpdate upstream, RowSet toClear, UpdateHelper helper, JobScheduler jobScheduler,
            @Nullable LivenessNode liveResultOwner, SelectLayerCompletionHandler onCompletion) {
        // this must be the fake update used to initialize the result table
        Assert.eqTrue(upstream.added().isFlat(), "upstream.added.isFlat()");
        Assert.eq(upstream.added().size(), "upstream.added.size()", parentRowSet.size(), "parentRowSet.size()");
        Assert.eqTrue(upstream.removed().isEmpty(), "upstream.removed.isEmpty()");
        Assert.eqTrue(upstream.modified().isEmpty(), "upstream.modified.isEmpty()");

        final BitSet baseLayerBitSet = new BitSet();
        inner.setBaseBits(baseLayerBitSet);
        final TableUpdate innerUpdate = new TableUpdateImpl(
                parentRowSet.copy(), RowSetFactory.empty(), RowSetFactory.empty(),
                RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);
        inner.applyUpdate(innerUpdate, toClear, helper, jobScheduler, liveResultOwner,
                new SelectLayerCompletionHandler(baseLayerBitSet, onCompletion) {
                    @Override
                    public void onAllRequiredColumnsCompleted() {
                        onCompletion.onLayerCompleted(getLayerIndex());
                    }
                });
    }

    @Override
    Map<String, Set<String>> calcDependsOnRecurse(boolean forcePublishAllResources) {
        return inner.calcDependsOnRecurse(forcePublishAllResources);
    }

    @Override
    public SelectAndViewAnalyzer getInner() {
        return inner;
    }

    @Override
    int getLayerIndexFor(String column) {
        if (overriddenColumns.containsKey(column)) {
            return getLayerIndex();
        }
        return inner.getLayerIndexFor(column);
    }

    @Override
    public void updateColumnDefinitionsFromTopLayer(Map<String, ColumnDefinition<?>> columnDefinitions) {
        inner.updateColumnDefinitionsFromTopLayer(columnDefinitions);
    }

    @Override
    public void startTrackingPrev() {
        throw new UnsupportedOperationException("StaticFlattenLayer is used in only non-refreshing scenarios");
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{StaticFlattenLayer").append(", layerIndex=").append(getLayerIndex()).append("}");
    }

    @Override
    public boolean allowCrossColumnParallelization() {
        return inner.allowCrossColumnParallelization();
    }
}
