//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.select.SelectColumn;
import org.jetbrains.annotations.NotNull;

final public class ViewColumnLayer extends SelectOrViewColumnLayer {

    private static final boolean ALLOW_LIVENESS_REFERENT_RESULTS = Configuration.getInstance()
            .getBooleanForClassWithDefault(ViewColumnLayer.class, "allowLivenessReferentResults", false);

    ViewColumnLayer(
            final SelectAndViewAnalyzer.AnalyzerContext context,
            final SelectColumn sc,
            final ColumnSource<?> cs,
            final String[] deps,
            final ModifiedColumnSet mcsBuilder) {
        super(context, sc, checkResultType(cs), null, deps, mcsBuilder);
        if (sc.recomputeOnModifiedRow()) {
            throw new IllegalArgumentException(
                    "SelectColumn may not have recomputeOnModifiedRow set for a view column: " + sc);
        }
    }

    @Override
    public boolean hasRefreshingLogic() {
        return false;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{ViewColumnLayer: ").append(selectColumn.toString()).append(", layerIndex=")
                .append(getLayerIndex()).append("}");
    }

    @Override
    public boolean allowCrossColumnParallelization() {
        // this should not actually matter; but false seems like the safe answer for any formula
        return false;
    }

    private static ColumnSource<?> checkResultType(@NotNull final ColumnSource<?> cs) {
        final Class<?> resultType = cs.getType();
        if (!ALLOW_LIVENESS_REFERENT_RESULTS && LivenessReferent.class.isAssignableFrom(resultType)) {
            throw new UnsupportedOperationException(String.format(
                    "Cannot use view or updateView to produce results of type %s; use select or update instead",
                    resultType));
        }
        return cs;
    }
}
