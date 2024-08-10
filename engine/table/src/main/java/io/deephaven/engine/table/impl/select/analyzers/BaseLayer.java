//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.util.JobScheduler;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class BaseLayer extends SelectAndViewAnalyzer.Layer {
    private final Map<String, ColumnSource<?>> sources;
    private final boolean publishTheseSources;

    BaseLayer(Map<String, ColumnSource<?>> sources, boolean publishTheseSources) {
        super(BASE_LAYER_INDEX);
        this.sources = sources;
        this.publishTheseSources = publishTheseSources;
    }

    @Override
    Set<String> getLayerColumnNames() {
        return sources.keySet();
    }

    @Override
    void populateModifiedColumnSetInReverse(
            final ModifiedColumnSet mcsBuilder,
            final Set<String> remainingDepsToSatisfy) {
        mcsBuilder.setAll(remainingDepsToSatisfy.toArray(String[]::new));
    }

    @Override
    void populateColumnSources(
            final Map<String, ColumnSource<?>> result,
            final GetMode mode) {
        // We specifically return a LinkedHashMap so the columns get populated in order
        if (mode == GetMode.All || (mode == GetMode.Published && publishTheseSources)) {
            result.putAll(sources);
        }
    }

    @Override
    public CompletionHandler createUpdateHandler(
            final TableUpdate upstream,
            final RowSet toClear,
            final SelectAndViewAnalyzer.UpdateHelper helper,
            final JobScheduler jobScheduler,
            @Nullable final LivenessNode liveResultOwner,
            final CompletionHandler onCompletion) {
        return new CompletionHandler(new BitSet(), onCompletion) {
            @Override
            protected void onAllRequiredColumnsCompleted() {
                // nothing to do at the base layer
                onCompletion.onLayerCompleted(getLayerIndex());
            }
        };
    }

    @Override
    final void calcDependsOn(
            final Map<String, Set<String>> result,
            boolean forcePublishAllSources) {
        if (publishTheseSources || forcePublishAllSources) {
            for (final String col : sources.keySet()) {
                result.computeIfAbsent(col, dummy -> new HashSet<>()).add(col);
            }
        }
    }

    @Override
    boolean allowCrossColumnParallelization() {
        return true;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{BaseLayer").append(", layerIndex=").append(getLayerIndex()).append("}");
    }
}
