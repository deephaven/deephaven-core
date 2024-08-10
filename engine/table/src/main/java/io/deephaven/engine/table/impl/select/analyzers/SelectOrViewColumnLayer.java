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
            final SelectAndViewAnalyzer analyzer,
            final String name,
            final SelectColumn sc,
            final ColumnSource<?> ws,
            final ColumnSource<?> optionalUnderlying,
            final String[] deps,
            final ModifiedColumnSet mcsBuilder) {
        super(analyzer, name, sc, ws, deps, mcsBuilder);
        this.optionalUnderlying = optionalUnderlying;
    }

    @Override
    void populateColumnSources(
            final Map<String, ColumnSource<?>> result,
            final GetMode mode) {
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
