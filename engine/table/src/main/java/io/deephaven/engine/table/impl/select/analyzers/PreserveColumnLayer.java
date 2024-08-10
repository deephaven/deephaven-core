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
 * <p>
 * {@implNote This class is part of the Deephaven engine, and not intended for direct use.}
 */
final public class PreserveColumnLayer extends DependencyLayerBase {
    private final BitSet dependencyBitSet;

    PreserveColumnLayer(
            final SelectAndViewAnalyzer analyzer,
            final String name,
            final SelectColumn sc,
            final ColumnSource<?> cs,
            final String[] deps,
            final ModifiedColumnSet mcsBuilder) {
        super(analyzer, name, sc, cs, deps, mcsBuilder);
        this.dependencyBitSet = new BitSet();
        Arrays.stream(deps).mapToInt(analyzer::getLayerIndexFor).forEach(dependencyBitSet::set);
    }

    @Override
    public CompletionHandler createUpdateHandler(
            final TableUpdate upstream,
            final RowSet toClear,
            final SelectAndViewAnalyzer.UpdateHelper helper,
            final JobScheduler jobScheduler,
            @Nullable final LivenessNode liveResultOwner,
            final CompletionHandler onCompletion) {
        return new CompletionHandler(dependencyBitSet, onCompletion) {
            @Override
            public void onAllRequiredColumnsCompleted() {
                // we don't need to do anything specific here
                onCompletion.onLayerCompleted(getLayerIndex());
            }
        };
    }

    @Override
    void populateColumnSources(
            final Map<String, ColumnSource<?>> result,
            final GetMode mode) {
        switch (mode) {
            case New:
                // we have no new sources
                break;
            case Published:
            case All:
                result.put(name, columnSource);
                break;
        }
    }

    @Override
    boolean allowCrossColumnParallelization() {
        return true;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{PreserveColumnLayer: ").append(name).append(", layerIndex=").append(getLayerIndex())
                .append("}");
    }
}
