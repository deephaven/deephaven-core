package io.deephaven.engine.v2.select.analyzers;

import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.v2.select.SelectColumn;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.chunk.Attributes;

import java.util.Map;

public abstract class SelectOrViewColumnLayer extends DependencyLayerBase {
    private final ColumnSource<Attributes.Values> optionalUnderlying;

    SelectOrViewColumnLayer(SelectAndViewAnalyzer inner, String name, SelectColumn sc,
            ColumnSource<Attributes.Values> ws, ColumnSource<Attributes.Values> optionalUnderlying,
            String[] deps, ModifiedColumnSet mcsBuilder) {
        super(inner, name, sc, ws, deps, mcsBuilder);
        this.optionalUnderlying = optionalUnderlying;
    }

    @Override
    final Map<String, ColumnSource<?>> getColumnSourcesRecurse(GetMode mode) {
        final Map<String, ColumnSource<?>> result = inner.getColumnSourcesRecurse(mode);
        result.put(name, columnSource);
        return result;
    }

    @Override
    public void startTrackingPrev() {
        columnSource.startTrackingPrevValues();
        if (optionalUnderlying != null) {
            optionalUnderlying.startTrackingPrevValues();
        }
        inner.startTrackingPrev();
    }
}
