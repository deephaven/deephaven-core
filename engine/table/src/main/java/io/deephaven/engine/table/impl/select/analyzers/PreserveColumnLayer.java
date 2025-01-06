//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.ColumnSource;

import java.util.Map;

/**
 * A layer that copies a column from our input to our output.
 * <p>
 * {@implNote This class is part of the Deephaven engine, and not intended for direct use.}
 */
final public class PreserveColumnLayer extends DependencyLayerBase {

    PreserveColumnLayer(
            final SelectAndViewAnalyzer.AnalyzerContext context,
            final SelectColumn sc,
            final ColumnSource<?> cs,
            final String[] deps,
            final ModifiedColumnSet mcsBuilder) {
        super(context, sc, cs, deps, mcsBuilder);
    }

    @Override
    public boolean hasRefreshingLogic() {
        return false;
    }

    @Override
    void populateColumnSources(final Map<String, ColumnSource<?>> result) {
        result.put(name, columnSource);
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
