//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import io.deephaven.web.client.api.AbstractAsyncGwtTestCase;

public class JsFigureTestGwt extends AbstractAsyncGwtTestCase {
    private static final TableSourceBuilder tables = new TableSourceBuilder()
            .script("""
                    from deephaven import empty_table
                    from deephaven.calendar import calendar
                    from deephaven.plot.figure import Figure

                    nyse_cal = calendar("USNYSE_EXAMPLE")""")
            .script("source = empty_table(100).update([\"Timestamp = '2024-01-01T00:00:00 ET' + i * HOUR\", \"Value = i\"])")
            .script("""
                    bizday_plot = (
                        Figure()
                        .axis(dim=0, business_time=True, calendar=nyse_cal)
                        .plot_xy(series_name="Business day data", t=source, x="Timestamp", y="Value")
                        .show()
                    )""");

    public void testBusinessTime() {
        connect(tables).then(figure("bizday_plot"))
                .then(figure -> {
                    for (int i = 0; i < figure.getCharts().length; i++) {
                        JsChart chart = figure.getCharts()[i];
                        for (int j = 0; j < chart.getAxes().length; j++) {
                            JsAxis axis = chart.getAxes()[j];
                            axis.range(200.0, null, null);
                        }
                    }
                    figure.subscribe();
                    return waitForEvent(figure, JsFigure.EVENT_UPDATED, 4321).onInvoke(figure);
                })
                .then(this::finish, this::report);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
