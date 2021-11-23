package io.deephaven.engine.table.impl.select.analyzers;

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
    public void applyUpdate(TableUpdate upstream, RowSet toClear, UpdateHelper helper) {
        // To be parallel with SelectColumnLayer, we would recurse here, but since this is ViewColumnLayer
        // (and all my inner layers are ViewColumnLayer), there's nothing to do.
    }
}
