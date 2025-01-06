//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.JsString;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.event.Event;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.filter.FilterValue;
import io.deephaven.web.client.api.subscription.ViewportData;
import jsinterop.base.Js;

import java.util.function.Consumer;

public class TotalsTableTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("strings", "empty_table(1).update([\"str=`abcdefg`\", \"nostr=(String)null\"])")
            .script("hasTotals",
                    "empty_table(5).update_view([(\"I = (double)i\", \"J = (double) i * i\", \"K = (double) i % 2\")])"
                            +
                            ".with_attributes({'TotalsTable':'false,false,Count;J=Min:Avg,K=Skip,;'})");

    public void testQueryDefinedConfigs() {
        connect(tables)
                .then(session -> {
                    session.getTable("strings", true)
                            .then(table1 -> {
                                // make sure the config is null, since it wasn't defined in the config
                                assertNull(table1.getTotalsTableConfig());

                                // check that we get a totals table back, even though it won't have any columns
                                // noinspection unchecked
                                return (Promise<Object[]>) Promise.all(new Promise[] {
                                        table1.getTotalsTable(null).then(totals1 -> {
                                            assertEquals(0, totals1.getColumns().length);
                                            assertEquals(1, totals1.getSize(), DELTA);
                                            return Promise.resolve(totals1);
                                        }),
                                        table1.getGrandTotalsTable(null).then(totals11 -> {
                                            assertEquals(0, totals11.getColumns().length);
                                            assertEquals(1, totals11.getSize(), DELTA);
                                            return Promise.resolve(totals11);
                                        }),
                                });
                            });
                    return Promise.resolve(session);
                })
                .then(table("hasTotals"))
                .then(table -> {
                    // make sure the config is null, since it wasn't defined in the config
                    assertNotNull(table.getTotalsTableConfig());
                    assertEquals("Count", table.getTotalsTableConfig().defaultOperation);
                    assertTrue(table.getTotalsTableConfig().operationMap.has("K"));
                    assertEquals(1, table.getTotalsTableConfig().operationMap.get("K").length);
                    assertEquals(Js.cast("Skip"), table.getTotalsTableConfig().operationMap.get("K").getAt(0));

                    assertTrue(table.getTotalsTableConfig().operationMap.has("J"));
                    assertEquals(2, table.getTotalsTableConfig().operationMap.get("J").length);
                    assertEquals(Js.cast("Min"), table.getTotalsTableConfig().operationMap.get("J").getAt(0));
                    assertEquals(Js.cast("Avg"), table.getTotalsTableConfig().operationMap.get("J").getAt(1));

                    assertFalse(table.getTotalsTableConfig().operationMap.has("I"));

                    // check that we get a totals table back, even though it won't have any columns
                    // noinspection unchecked
                    return Promise.all((Promise<Object>[]) new Promise[] {
                            table.getTotalsTable(null)
                                    .then(totals -> {
                                        assertEquals(3, totals.getColumns().length);
                                        assertEquals(1, totals.getSize(), DELTA);
                                        totals.setViewport(0, 100, null, null, null);

                                        return waitForEvent(totals, JsTable.EVENT_UPDATED,
                                                checkTotals(totals, 5, 6., 0, "a1"), 2508);
                                    }),
                            table.getGrandTotalsTable(null)
                                    .then(totals -> {
                                        assertEquals(3, totals.getColumns().length);
                                        assertEquals(1, totals.getSize(), DELTA);
                                        totals.setViewport(0, 100, null, null, null);

                                        return waitForEvent(totals, JsTable.EVENT_UPDATED,
                                                checkTotals(totals, 5, 6.0, 0., "a2"), 2509);
                                    })
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    // TODO: https://deephaven.atlassian.net/browse/DH-11196
    public void ignore_testTotalsOnFilteredTable() {
        JsTotalsTable[] totalTables = {null, null};
        Promise[] totalPromises = {null, null};
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(8000);
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    table.setViewport(0, 100, null);// not strictly required, but part of the normal usage

                    return waitForEvent(table, JsTable.EVENT_FILTERCHANGED, 2001).onInvoke(table);
                })
                .then(table -> promiseAllThen(table,
                        table.getTotalsTable(null)
                                .then(totals -> {
                                    totalTables[0] = totals;
                                    assertEquals(3, totals.getColumns().length);
                                    assertEquals(1, totals.getSize(), DELTA);
                                    totals.setViewport(0, 100, null, null, null);

                                    // confirm the normal totals match the filtered data
                                    return waitForEvent(totals, JsTable.EVENT_UPDATED,
                                            checkTotals(totals, 3, 6.666666, 0.0, "a1"), 2501);
                                }),
                        table.getGrandTotalsTable(null)
                                .then(totals -> {
                                    totalTables[1] = totals;
                                    assertEquals(3, totals.getColumns().length);
                                    assertEquals(1, totals.getSize(), DELTA);
                                    totals.setViewport(0, 100, null, null, null);

                                    // confirm the grand totals are unchanged
                                    return waitForEvent(totals, JsTable.EVENT_UPDATED,
                                            checkTotals(totals, 5, 6., 0., "a2"), 2502);
                                })))
                .then(table -> {
                    // Now, change the filter on the original table, and expect the totals tables to automatically
                    // update.

                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(1.0))
                    });
                    table.setViewport(0, 100, null);// not strictly required, but part of the normal usage

                    return promiseAllThen(table,
                            waitForEvent(table, JsTable.EVENT_FILTERCHANGED, 2002).onInvoke(table),
                            totalPromises[0] = waitForEvent(totalTables[0], JsTable.EVENT_UPDATED,
                                    checkTotals(totalTables[0], 2, 5, 1, "b1"), 2503),
                            totalPromises[1] = waitForEvent(totalTables[1], JsTable.EVENT_UPDATED,
                                    checkTotals(totalTables[1], 5, 6, 0, "b2"), 2504));
                })
                .then(table -> {
                    // forcibly disconnect the worker and test that the total table come back up, and respond to
                    // re-filtering.
                    table.getConnection().forceReconnect();
                    return Promise.resolve(table);
                })
                .then(table -> waitForEvent(table, JsTable.EVENT_RECONNECT, 5001).onInvoke(table))
                .then(table -> promiseAllThen(table,
                        waitForEvent(totalTables[0], JsTable.EVENT_UPDATED,
                                checkTotals(totalTables[0], 2, 5, 1, "c1"), 7505),
                        waitForEvent(totalTables[1], JsTable.EVENT_UPDATED,
                                checkTotals(totalTables[1], 5, 6, 0, "c2"), 7506)))
                .then(table -> {
                    // Now... refilter the original table, and assert that the totals tables update.
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    table.setViewport(0, 100, null);// not strictly required, but part of the normal usage

                    return promiseAllThen(table,
                            waitForEvent(table, JsTable.EVENT_FILTERCHANGED, 2003).onInvoke(table),
                            waitForEvent(totalTables[0], JsTable.EVENT_UPDATED,
                                    checkTotals(totalTables[0], 3, 6.666666, 0.0, "d1"), 2507),
                            waitForEvent(totalTables[1], JsTable.EVENT_UPDATED,
                                    checkTotals(totalTables[1], 5, 6., 0., "d2"), 2508));
                })
                .then(this::finish).catch_(this::report);
    }

    public void testClosingTotalsWhileClearingFilter() {
        JsTotalsTable[] totalTables = {null};
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(8000);
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    table.setViewport(0, 100, null);// not strictly required, but part of the normal usage

                    return waitForEvent(table, JsTable.EVENT_UPDATED, 2001).onInvoke(table);
                })
                .then(table -> table.getTotalsTable(null)
                        .then(totals -> {
                            totalTables[0] = totals;
                            return Promise.resolve(table);
                        }))
                .then(table -> {
                    // Now, clear the filter on the original table, and close the totals table at the same time
                    table.applyFilter(new FilterCondition[] {});
                    // Close the table in the same step as applying the filter. Should not throw an error.
                    totalTables[0].close();
                    table.setViewport(0, 100, null);// not strictly required, but part of the normal usage

                    return waitForEvent(table, JsTable.EVENT_UPDATED, 2002).onInvoke(table);
                })
                .then(this::finish).catch_(this::report);
    }

    // TODO: https://deephaven.atlassian.net/browse/DH-11196
    public void ignore_testFilteringTotalsTable() {
        JsTotalsTable[] totalTables = {null, null};
        Promise[] totalPromises = {null, null};
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(8000);
                    /*
                     * Here is the base table: I J K 0 0 0 1 1 1 2 4 0 // we are going to remove this row, to test
                     * source table filtering. 3 9 1 4 16 0
                     */
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("J").filter().notEq(FilterValue.ofNumber(4.0))
                    });
                    table.setViewport(0, 100, null);// not strictly required, but part of the normal usage

                    return waitForEvent(table, JsTable.EVENT_FILTERCHANGED, 2001).onInvoke(table);
                })
                .then(table -> {
                    JsTotalsTableConfig config = new JsTotalsTableConfig();
                    // group by K so we can do some filtering on the output of the tables
                    config.groupBy.push("K");
                    // copy over the rest of the operations set on the server, so we can reuse some assertion logic
                    config.operationMap.set("J",
                            Js.uncheckedCast(new JsString[] {toJsString("Avg"), toJsString("Min")}));
                    // config.operationMap.set("K", Js.uncheckedCast(new JsString[]{ toJsString("Skip")}));
                    config.defaultOperation = "Count";
                    return promiseAllThen(table,
                            table.getTotalsTable(config)
                                    .then(totals -> {
                                        totalTables[0] = totals;
                                        assertEquals(4, totals.getColumns().length);
                                        assertEquals(2, totals.getSize(), DELTA);
                                        totals.setViewport(0, 100, null, null, null);

                                        // confirm the normal totals match the filtered data
                                        return waitForEvent(totals, JsTable.EVENT_UPDATED, checkTotals(totals, "a1",
                                                TotalsResults.of(2, 2, 8, 0.0),
                                                TotalsResults.of(2, 2, 5, 1.0)), 2501);
                                    }),
                            table.getGrandTotalsTable(config)
                                    .then(totals -> {
                                        totalTables[1] = totals;
                                        assertEquals(4, totals.getColumns().length);
                                        assertEquals(2, totals.getSize(), DELTA);
                                        totals.setViewport(0, 100, null, null, null);

                                        // confirm the grand totals include the missing row...
                                        return waitForEvent(totals, JsTable.EVENT_UPDATED, checkTotals(totals, "a2",
                                                TotalsResults.of(3, 3, 6.66666, 0.0),
                                                TotalsResults.of(2, 2, 5, 1.0)), 2502);
                                    }));
                })
                .then(table -> {
                    // Now, apply a filter to each totals tables...

                    totalTables[0].applyFilter(new FilterCondition[] {
                            // we'll use notEq here, so that changing a filter in the source table causes this filter
                            // to remove nothing (instead of remove everything).
                            totalTables[0].findColumn("J__Avg").filter().notEq(FilterValue.ofNumber(8.0))
                    });
                    totalTables[1].applyFilter(new FilterCondition[] {
                            totalTables[1].findColumn("J__Avg").filter().eq(FilterValue.ofNumber(5.0))
                    });
                    totalTables[0].setViewport(0, 100, null, null, null);
                    totalTables[1].setViewport(0, 100, null, null, null);

                    return promiseAllThen(table,
                            totalPromises[0] = waitForEvent(totalTables[0], JsTable.EVENT_UPDATED,
                                    checkTotals(totalTables[0], "b1", TotalsResults.of(2, 2, 5, 1)), 2503),
                            totalPromises[1] = waitForEvent(totalTables[1], JsTable.EVENT_UPDATED,
                                    checkTotals(totalTables[1], "b2", TotalsResults.of(2, 2, 5, 1)), 2504));
                })
                .then(table -> {
                    // forcibly disconnect the worker and test that the total table come back up, and respond to
                    // re-filtering.
                    table.getConnection().forceReconnect();
                    return Promise.resolve(table);
                })
                .then(table -> waitForEvent(table, JsTable.EVENT_RECONNECT, 5001).onInvoke(table))
                .then(table -> promiseAllThen(table,
                        totalPromises[0] = waitForEvent(totalTables[0], JsTable.EVENT_UPDATED,
                                checkTotals(totalTables[0], "c1", TotalsResults.of(2, 2, 5, 1)), 2505),
                        totalPromises[1] = waitForEvent(totalTables[1], JsTable.EVENT_UPDATED,
                                checkTotals(totalTables[1], "c2", TotalsResults.of(2, 2, 5, 1)), 2506)))
                .then(table -> {
                    // Now... refilter the original table, and assert that the totals tables update.
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("J").filter().notEq(FilterValue.ofNumber(9.0))
                            // the != filters on the regular totals will no longer remove anything w/ updated source
                            // filter....
                            // but grand totals will ignore this, and still be filtered
                    });
                    table.setViewport(0, 100, null);// not strictly required, but part of the normal usage

                    return promiseAllThen(table,
                            waitForEvent(table, JsTable.EVENT_FILTERCHANGED, 5503).onInvoke(table),
                            waitForEvent(totalTables[0], JsTable.EVENT_UPDATED, checkTotals(totalTables[1], "d1",
                                    TotalsResults.of(3, 3, 6.666666, 0.0),
                                    TotalsResults.of(1, 1, 1, 1.0)), 2507),
                            totalPromises[1] = waitForEvent(totalTables[1], JsTable.EVENT_UPDATED,
                                    checkTotals(totalTables[1], "d2", TotalsResults.of(2, 2, 5, 1)), 2508));
                })
                .then(this::finish).catch_(this::report);
    }

    public void testGroupedTotals() {
        connect(tables)
                .then(session -> {
                    delayFinish(2000);
                    return session.getTable("hasTotals", true);
                })
                .then(table -> {
                    // take the existing config and group by K
                    JsTotalsTableConfig config = table.getTotalsTableConfig();
                    config.groupBy = new JsArray<>("K");

                    // use the same check for both totals and grand totals tables
                    IThenable.ThenOnFulfilledCallbackFn<JsTotalsTable, JsTotalsTable> checkForBothTotalsTables =
                            (JsTotalsTable totals) -> {
                                assertEquals(4, totals.getColumns().length);
                                assertEquals(2, totals.getSize(), DELTA);
                                totals.setViewport(0, 100, null, null, null);

                                // confirm the grand totals are unchanged
                                return waitForEvent(totals, JsTable.EVENT_UPDATED, update -> {
                                    ViewportData viewportData = (ViewportData) update.getDetail();

                                    // 2 rows (one for k=0, one for k=1)
                                    assertEquals(2, viewportData.getRows().length);
                                    // 4 columns (3 agg'd, and the grouped column)
                                    assertEquals(4, viewportData.getColumns().length);

                                    // k=0 row
                                    assertEquals(0,
                                            viewportData.getRows().getAt(0).get(totals.findColumn("K")).asInt());
                                    assertEquals(3, viewportData.getRows().getAt(0).get(totals.findColumn("I"))
                                            .<LongWrapper>cast().getWrapped());
                                    assertEquals(6.666666,
                                            viewportData.getRows().getAt(0).get(totals.findColumn("J__Avg")).asDouble(),
                                            DELTA);
                                    assertEquals(0.0, viewportData.getRows().getAt(0).get(totals.findColumn("J__Min"))
                                            .asDouble());

                                    // k=1 row
                                    assertEquals(1,
                                            viewportData.getRows().getAt(1).get(totals.findColumn("K")).asInt());
                                    assertEquals(2, viewportData.getRows().getAt(1).get(totals.findColumn("I"))
                                            .<LongWrapper>cast().getWrapped());
                                    assertEquals(5.0,
                                            viewportData.getRows().getAt(1).get(totals.findColumn("J__Avg")).asDouble(),
                                            DELTA);
                                    assertEquals(1.0, viewportData.getRows().getAt(1).get(totals.findColumn("J__Min"))
                                            .asDouble());
                                }, 1500);
                            };

                    // noinspection unchecked
                    return Promise.all((Promise<Object>[]) new Promise[] {
                            table.getTotalsTable(config).then(checkForBothTotalsTables),
                            table.getGrandTotalsTable(config).then(checkForBothTotalsTables)
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    private static class TotalsResults {
        int k;
        long i;
        double avg;
        double min;

        public TotalsResults(int k, long i, double avg, double min) {
            this.k = k;
            this.i = i;
            this.avg = avg;
            this.min = min;
        }

        static TotalsResults of(int k, long i, double avg, double min) {
            return new TotalsResults(k, i, avg, min);
        }
    }

    private Consumer<Event<ViewportData>> checkTotals(
            JsTotalsTable totals,
            long i,
            double avg,
            double min,
            String messages) {
        String ext = messages;
        return update -> {
            ViewportData viewportData = update.getDetail();

            assertEquals(1, viewportData.getRows().length);
            assertEquals(3, viewportData.getColumns().length);

            assertEquals("I" + ext, i,
                    viewportData.getRows().getAt(0).get(totals.findColumn("I")).<LongWrapper>cast().getWrapped());
            assertEquals("J_Avg" + ext, avg,
                    viewportData.getRows().getAt(0).get(totals.findColumn("J__Avg")).asDouble(), 0.0001);
            assertEquals("J_Min" + ext, min,
                    viewportData.getRows().getAt(0).get(totals.findColumn("J__Min")).asDouble(), 0.0001);

        };
    }

    private Consumer<Event<ViewportData>> checkTotals(
            JsTotalsTable totals,
            String messages,
            TotalsResults... expected) {
        String ext = messages;
        return update -> {
            ViewportData viewportData = update.getDetail();

            assertEquals("Viewport data rows", expected.length, viewportData.getRows().length);
            assertEquals("Viewport columns", 4, viewportData.getColumns().length);

            for (int ind = 0; ind < expected.length; ind++) {
                final TotalsResults result = expected[ind];
                assertEquals("K" + ext, result.k,
                        viewportData.getRows().getAt(ind).get(totals.findColumn("K")).<LongWrapper>cast().getWrapped());
                assertEquals("I" + ext, result.i,
                        viewportData.getRows().getAt(ind).get(totals.findColumn("I")).<LongWrapper>cast().getWrapped());
                assertEquals("J_Avg" + ext, result.avg,
                        viewportData.getRows().getAt(ind).get(totals.findColumn("J__Avg")).asDouble(), 0.0001);
                assertEquals("J_Min" + ext, result.min,
                        viewportData.getRows().getAt(ind).get(totals.findColumn("J__Min")).asDouble(), 0.0001);
            }

        };
    }

    /**
     * Specialized waitForEvent since JsTotalsTable isn't a HasEventHandling subtype, and doesnt make sense to shoehorn
     * it in just for tests.
     */
    private <T> Promise<JsTotalsTable> waitForEvent(JsTotalsTable table, String eventName, Consumer<Event<T>> check,
            int timeout) {
        return waitForEvent(table.getWrappedTable(), eventName, check, timeout)
                .then(t -> Promise.resolve(table));
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
