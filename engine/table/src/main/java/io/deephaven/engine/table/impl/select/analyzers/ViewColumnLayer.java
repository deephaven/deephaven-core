package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;

final public class ViewColumnLayer extends SelectOrViewColumnLayer {
    ViewColumnLayer(SelectAndViewAnalyzer inner, String name, SelectColumn sc, ColumnSource cs, String[] deps,
            ModifiedColumnSet mcsBuilder) {
        super(inner, name, sc, cs, null, deps, mcsBuilder);
    }

    @Override
    public void applyUpdate(TableUpdate upstream, RowSet toClear, UpdateHelper helper, JobScheduler jobScheduler,
            SelectLayerCompletionHandler completionHandler) {
        // To be parallel with SelectColumnLayer, we would recurse here, but since this is ViewColumnLayer
        // (and all my inner layers are ViewColumnLayer), there's nothing to do.
        Assert.eqNull(completionHandler, "completionHandler");
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
}
