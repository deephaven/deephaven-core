/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.ColumnSource;

import java.util.Map;

public abstract class SelectOrViewColumnLayer extends DependencyLayerBase {
    private final ColumnSource<?> optionalUnderlying;

    SelectOrViewColumnLayer(SelectAndViewAnalyzer inner, String name, SelectColumn sc,
            ColumnSource<?> ws, ColumnSource<?> optionalUnderlying,
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
