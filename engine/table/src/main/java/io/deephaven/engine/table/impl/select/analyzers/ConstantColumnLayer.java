//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.VectorChunkAdapter;

public class ConstantColumnLayer extends SelectOrViewColumnLayer {

    ConstantColumnLayer(
            final SelectAndViewAnalyzer.AnalyzerContext context,
            final SelectColumn sc,
            final WritableColumnSource<?> ws,
            final String[] deps,
            final ModifiedColumnSet mcsBuilder) {
        super(context, sc, ws, null, deps, mcsBuilder);
        if (sc.recomputeOnModifiedRow()) {
            throw new IllegalArgumentException(
                    "SelectColumn may not have alwaysEvaluate set for a constant column: " + sc);
        }
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
    public boolean hasRefreshingLogic() {
        return false;
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
