//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.AbstractAsyncGwtTestCase;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.subscription.AbstractTableSubscription;
import io.deephaven.web.client.api.subscription.SubscriptionTableData;
import io.deephaven.web.client.api.subscription.TableSubscription;
import io.deephaven.web.shared.fu.JsFunction;
import jsinterop.base.Any;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;
import java.util.function.BiConsumer;

public class ChartDataTestGwt extends AbstractAsyncGwtTestCase {
    private final AbstractAsyncGwtTestCase.TableSourceBuilder tables = new AbstractAsyncGwtTestCase.TableSourceBuilder()
            .script("from deephaven import time_table")
            .script("appending_table", "time_table('PT0.1s').update([\"A = i % 2\", \"B = `` + A\"])")
            .script("updating_table", "appending_table.last_by('A')");

    private final TableSourceBuilder tickingData = new TableSourceBuilder()
            .script("from deephaven import input_table",
                    "from deephaven import dtypes as dht",
                    "import jpy",
                    "from deephaven.execution_context import ExecutionContext, get_exec_ctx",
                    "my_col_defs = {\"ID\": dht.int32, \"Value\": dht.string, \"Deleted\": dht.bool_}",
                    "ug = jpy.get_type('io.deephaven.engine.updategraph.impl.EventDrivenUpdateGraph').newBuilder('test').existingOrBuild()",
                    "exec_ctx = ExecutionContext(get_exec_ctx().j_object.withUpdateGraph(ug))",
                    "with exec_ctx:\n" +
                            "    input = input_table(col_defs=my_col_defs, key_cols=\"ID\")\n" +
                            "    result = input.without_attributes('InputTable').where(\"!Deleted\").sort(\"ID\")");

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

    public void testGeneratedChartData() {
        connect(tickingData)
                .then(ideSession -> {
                    delayTestFinish(9870);
                    Promise<JsTable> inputPromise = ideSession.getTable("input", false);
                    Promise<JsTable> resultPromise = ideSession.getTable("result", false);
                    return inputPromise.then(JsTable::inputTable).then(delayFinish(9871)).then(input -> {
                        delayTestFinish(9872);
                        return resultPromise.then(result -> {
                            delayTestFinish(9873);
                            ChartData chartData = new ChartData(result);

                            // Keep the same function instance, as it is used for caching data
                            JsFunction<Any, Any> dataFunction = arr -> Js.asAny("::" + arr);

                            BiConsumer<SubscriptionTableData, SubscriptionTableData> consistencyCheck =
                                    (subscriptionUpdate, snapshot) -> {
                                        chartData.update(subscriptionUpdate);
                                        // Verify the "Value" column, the only mutable column, has the same contents
                                        // regardless of source
                                        String[] expectedValues = new String[snapshot.getRows().length];
                                        snapshot.getRows().forEach((row, index) -> {
                                            expectedValues[index] = row.get(result.findColumn("Value")).asString();
                                            return null;
                                        });
                                        JsArray<Any> values = chartData.getColumn("Value", null, subscriptionUpdate);
                                        JsArray<Any> valuesWithPrefix =
                                                chartData.getColumn("Value", dataFunction, subscriptionUpdate);
                                        assertEquals(values.length, valuesWithPrefix.length);
                                        assertEquals(expectedValues.length, values.length);
                                        for (int i = 0; i < expectedValues.length; i++) {
                                            assertEquals(expectedValues[i], values.getAt(i).asString());
                                            assertEquals("::" + expectedValues[i],
                                                    valuesWithPrefix.getAt(i).asString());
                                        }
                                    };
                            return subscriptionValidator(result, result.getColumns())
                                    // Check initial state (empty)
                                    .check(consistencyCheck)
                                    // Add three rows
                                    .when(() -> {
                                        input.addRows(new JsPropertyMap[] {
                                                row(1, "A"),
                                                row(2, "B"),
                                                row(5, "E")
                                        }, null);
                                    })
                                    .check(consistencyCheck)
                                    // Add two more rows, causing a shift
                                    .when(() -> {
                                        input.addRows(new JsPropertyMap[] {
                                                row(3, "C"),
                                                row(4, "D")
                                        }, null);
                                    })
                                    .check(consistencyCheck)
                                    // Update an existing row
                                    .when(() -> {
                                        input.addRows(new JsPropertyMap[] {
                                                row(3, "F")
                                        }, null);
                                    })
                                    .check(consistencyCheck)
                                    // Delete two rows
                                    .when(() -> {
                                        input.addRows(new JsPropertyMap[] {
                                                delete(1),
                                                delete(3)
                                        }, null);
                                    })
                                    .check(consistencyCheck)
                                    .cleanup();
                        });
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * Helper to validate subscription results by comparing an updated subscription against a fresh snapshot. Intended
     * to be generalized for use in viewport tests, but haven't yet done this.
     */
    private SubscriptionValidator subscriptionValidator(JsTable table, JsArray<Column> subscriptionColumns) {
        TableSubscription subscription = table.subscribe(subscriptionColumns);

        return new SubscriptionValidator() {
            Promise<?> nextStep = Promise.resolve(subscription);

            @Override
            public SubscriptionValidator when(Runnable setup) {
                // Run the step when the previous step completes
                nextStep = nextStep.then(ignore -> {
                    setup.run();
                    return Promise.resolve(ignore);
                });
                return this;
            }

            @Override
            public SubscriptionValidator check(BiConsumer<SubscriptionTableData, SubscriptionTableData> check) {
                // Run this as a step once the previous step completes, by waiting for the original subscription to
                // update, then creating a new subscription and waiting for that as well. Pass both updates to the
                // provided check function.
                nextStep = nextStep.then(ignore -> new Promise<>((resolve, reject) -> {
                    try {
                        subscription.addEventListenerOneShot(TableSubscription.EVENT_UPDATED, e1 -> {
                            try {
                                SubscriptionTableData data = (SubscriptionTableData) e1.getDetail();
                                // Now that this update has happened, we make a new subscription to get a single
                                // snapshot of the table
                                TableSubscription checkSub = table.subscribe(subscriptionColumns);
                                checkSub.addEventListenerOneShot(TableSubscription.EVENT_UPDATED, e2 -> {
                                    try {
                                        SubscriptionTableData checkData = (SubscriptionTableData) e2.getDetail();
                                        check.accept(data, checkData);
                                        resolve.onInvoke(checkData);
                                    } catch (Throwable e) {
                                        reject.onInvoke(e);
                                    } finally {
                                        checkSub.close();
                                    }
                                });
                            } catch (Throwable e) {
                                reject.onInvoke(e);
                            }
                        });
                    } catch (Throwable e) {
                        reject.onInvoke(e);
                    }
                }));
                return this;
            }

            @Override
            public Promise<?> cleanup() {
                // Finished, clean up after ourselves, emit success/failure
                return nextStep.finally_(subscription::close);
            }
        };
    }

    private interface SubscriptionValidator {
        /**
         * Performs some step that will lead to the table subscription being updated.
         */
        SubscriptionValidator when(Runnable setup);

        /**
         * Checks that the actual and expected data match. Must complete synchronously.
         */
        SubscriptionValidator check(BiConsumer<SubscriptionTableData, SubscriptionTableData> check);

        /**
         * Cleans up after validation work, returning a promise that the test can await.
         */
        Promise<?> cleanup();
    }

    private static JsPropertyMap<? extends Serializable> row(int id, String value) {
        return JsPropertyMap.of("ID", (double) id, "Value", value, "Deleted", false);
    }

    private static JsPropertyMap<? extends Serializable> delete(int id) {
        return JsPropertyMap.of("ID", (double) id, "Value", "deleted", "Deleted", true);
    }
}
