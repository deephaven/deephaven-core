//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.VectorChunkAdapter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.BitSet;

public class ConstantColumnLayer extends SelectOrViewColumnLayer {
    private final BitSet dependencyBitSet;

    ConstantColumnLayer(
            SelectAndViewAnalyzer analyzer,
            String name,
            SelectColumn sc,
            WritableColumnSource<?> ws,
            String[] deps,
            ModifiedColumnSet mcsBuilder) {
        super(analyzer, name, sc, ws, null, deps, mcsBuilder);
        this.dependencyBitSet = new BitSet();
        Arrays.stream(deps).mapToInt(analyzer::getLayerIndexFor).forEach(dependencyBitSet::set);
        initialize(ws);
    }

    /**
     * Initialize the column source with the constant value. Note that parent TableUpdates are ignored.
     */
    private void initialize(final WritableColumnSource<?> writableSource) {
        ChunkSource.WithPrev<Values> chunkSource = selectColumn.getDataView();
        if (selectColumnHoldsVector) {
            chunkSource = new VectorChunkAdapter<>(chunkSource);
        }

        try (final WritableColumnSource.FillFromContext destContext = writableSource.makeFillFromContext(1);
                final ChunkSource.GetContext chunkSourceContext = chunkSource.makeGetContext(1);
                final RowSequence keys = RowSetFactory.fromKeys(0)) {
            writableSource.fillFromChunk(destContext, chunkSource.getChunk(chunkSourceContext, keys), keys);
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
        // Nothing to do at this level, but need to recurse because my inner layers might need to be called (e.g.
        // because they are SelectColumnLayers)
        return new CompletionHandler(dependencyBitSet, onCompletion) {
            @Override
            public void onAllRequiredColumnsCompleted() {
                // we don't need to do anything specific here; our result value is constant
                onCompletion.onLayerCompleted(getLayerIndex());
            }
        };
    }

    @Override
    boolean allowCrossColumnParallelization() {
        return true;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{ConstantColumnLayer: ").append(selectColumn.toString()).append("}");
    }
}
