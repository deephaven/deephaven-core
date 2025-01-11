//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.AbstractAsyncGwtTestCase;
import io.deephaven.web.client.api.subscription.AbstractTableSubscription;
import io.deephaven.web.client.api.subscription.TableSubscription;

public class ChartDataTestGwt extends AbstractAsyncGwtTestCase {
    private final AbstractAsyncGwtTestCase.TableSourceBuilder tables = new AbstractAsyncGwtTestCase.TableSourceBuilder()
            .script("from deephaven import time_table")
            .script("appending_table", "time_table('PT0.1s').update([\"A = i % 2\", \"B = `` + A\"])")
            .script("updating_table", "appending_table.last_by('A')");

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }

    public void testAppendingChartData() {
        connect(tables)
                .then(table("appending_table"))
                .then(table -> {
                    ChartData chartData = new ChartData(table);
                    TableSubscription sub = table.subscribe(table.getColumns());

                    // Handle at least one event with data
                    int[] count = {0};

                    return new Promise<AbstractTableSubscription.SubscriptionEventData>((resolve, reject) -> {
                        delayTestFinish(12301);
                        sub.addEventListener(TableSubscription.EVENT_UPDATED, event -> {
                            AbstractTableSubscription.SubscriptionEventData data =
                                    (AbstractTableSubscription.SubscriptionEventData) event.getDetail();
                            chartData.update(data);

                            if (count[0] > 0) {
                                // Don't test on the first update, could still be empty
                                assertTrue(chartData.getColumn("A", arr -> arr, data).length > 0);

                                resolve.onInvoke(data);
                            }
                            count[0]++;
                        });
                    }).then(data -> {
                        sub.close();
                        table.close();
                        return Promise.resolve(data);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testModifiedChartData() {
        connect(tables)
                .then(table("updating_table"))
                .then(table -> {
                    ChartData chartData = new ChartData(table);
                    TableSubscription sub = table.subscribe(table.getColumns());

                    int[] count = {0};
                    return new Promise<AbstractTableSubscription.SubscriptionEventData>((resolve, reject) -> {
                        delayTestFinish(12302);
                        sub.addEventListener(TableSubscription.EVENT_UPDATED, event -> {

                            AbstractTableSubscription.SubscriptionEventData data =
                                    (AbstractTableSubscription.SubscriptionEventData) event.getDetail();

                            chartData.update(data);
                            if (count[0] > 0) {
                                // Don't test on the first update, could still be empty
                                assertTrue(chartData.getColumn("A", arr -> arr, data).length > 0);
                            }

                            if (count[0] > 2) {
                                // We've seen at least three updates, and must have modified rows at least once
                                resolve.onInvoke((AbstractTableSubscription.SubscriptionEventData) event.getDetail());
                            }
                            count[0]++;
                        });
                    })
                            .then(data -> {
                                sub.close();
                                table.close();
                                return Promise.resolve(data);
                            });
                })
                .then(this::finish).catch_(this::report);
    }
}
