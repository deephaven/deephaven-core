/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.util.functions;

import io.deephaven.db.plot.FigureImpl;
import io.deephaven.db.plot.datasets.DataSeriesInternal;
import io.deephaven.db.plot.datasets.multiseries.MultiSeriesInternal;

import java.util.function.Function;

/**
 * Applies a series transform to a given {@link FigureImpl}.
 */
public class FigureImplFunction implements Function<FigureImpl, FigureImpl> {

    private final Function<FigureImpl, FigureImpl> function;
    private final int chartRowIndex;
    private final int chartColumnIndex;
    private final int axesIndex;
    private final int seriesIndex;

    public FigureImplFunction(final Function<FigureImpl, FigureImpl> function,
            final MultiSeriesInternal multiSeriesInternal) {
        this(function, multiSeriesInternal.chart().row(), multiSeriesInternal.chart().column(),
                multiSeriesInternal.axes().id(), multiSeriesInternal.id());
    }

    public FigureImplFunction(final Function<FigureImpl, FigureImpl> function,
            final DataSeriesInternal dataSeriesInternal) {
        this(function, dataSeriesInternal.chart().row(), dataSeriesInternal.chart().column(),
                dataSeriesInternal.axes().id(), dataSeriesInternal.id());
    }

    public FigureImplFunction(final Function<FigureImpl, FigureImpl> function, final int chartRowIndex,
            final int chartColumnIndex, final int axesIndex, final int seriesIndex) {
        this.function = function;
        this.chartRowIndex = chartRowIndex;
        this.chartColumnIndex = chartColumnIndex;
        this.axesIndex = axesIndex;
        this.seriesIndex = seriesIndex;
    }

    @Override
    public FigureImpl apply(FigureImpl figure) {
        return function.apply(figure.chart(chartRowIndex, chartColumnIndex).axes(axesIndex).series(seriesIndex));
    }
}
