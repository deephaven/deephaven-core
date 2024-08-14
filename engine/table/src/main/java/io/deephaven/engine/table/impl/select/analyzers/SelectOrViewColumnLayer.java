//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.ColumnSource;

import java.util.Map;

public abstract class SelectOrViewColumnLayer extends DependencyLayerBase {
    private final ColumnSource<?> optionalUnderlying;

    SelectOrViewColumnLayer(
            final SelectAndViewAnalyzer.AnalyzerContext context,
            final SelectColumn sc,
            final ColumnSource<?> ws,
            final ColumnSource<?> optionalUnderlying,
            final String[] deps,
            final ModifiedColumnSet mcsBuilder) {
        super(context, sc, ws, deps, mcsBuilder);
        this.optionalUnderlying = optionalUnderlying;
    }

    @Override
    void populateColumnSources(final Map<String, ColumnSource<?>> result) {
        result.put(name, columnSource);
    }

    @Override
    public void startTrackingPrev() {
        columnSource.startTrackingPrevValues();
        if (optionalUnderlying != null) {
            optionalUnderlying.startTrackingPrevValues();
        }
    }
}
