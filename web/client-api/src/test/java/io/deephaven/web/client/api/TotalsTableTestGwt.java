//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
import io.deephaven.web.client.api.tree.enums.JsAggregationOperation;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.base.Js;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Map.entry;

public class TotalsTableTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("strings", "empty_table(1).update([\"str=`abcdefg`\", \"nostr=(String)null\", \"n=1\"])")
            .script("hasTotals",
                    "empty_table(5).update_view([(\"I = (double)i\", \"J = (double) i * i\", \"K = (double) i % 2\")])"
                            +
                            ".with_attributes({'TotalsTable':'false,false,Count;J=Min:Avg,K=Skip,;'})")
            .script("hasCountColumn",
                    "empty_table(5).update_view([\"Count = (double)i\", \"Value = (double) i * 2\"])")
            // Like hasTotals, but with a lazily computed column that is slow to read, so that aggregating the
            // unfiltered table takes measurably longer than aggregating a filtered subset of it
            .script("import time")
            .script("def slow(x) -> float:\n    time.sleep(0.4)\n    return float(x)")
            .script("slowTotals",
                    "empty_table(5).update_view([\"I = (double)i\", \"J = (double) i * i\", \"K = (double) i % 2\", \"S = slow(i)\"])"
                            + ".with_attributes({'TotalsTable':'false,false,Sum;J=Min:Avg,K=Skip,;'})")
            // G is slow to fail when read. Client formulas may not call Python functions, so a test that wants a
            // totals rebase to fail slowly points a summed column at G instead
            .script("def slow_fail(x) -> float:\n    time.sleep(1.0)\n    raise Exception('slow failure')")
            .script("rebaseRace",
                    "empty_table(5).update_view([\"I = (double)i\", \"S = slow(i)\", \"F = (double)i\", \"G = slow_fail(i)\"])");

    // This will need to be updated to reflect values from JsTotalsTableConfig.knownAggTypes
    // These are the values calculated for each Agg for column I of the "hasTotals" table
    private final Map<String, Double> AggValueMap = Map.ofEntries(
            entry(JsAggregationOperation.MIN, 0.0),
            entry(JsAggregationOperation.MAX, 4.0),
            entry(JsAggregationOperation.SUM, 10.0),
            entry(JsAggregationOperation.ABS_SUM, 10.0),
            entry(JsAggregationOperation.VAR, 2.5),
            entry(JsAggregationOperation.AVG, 2.0),
            entry(JsAggregationOperation.MEDIAN, 2.0),
            entry(JsAggregationOperation.STD, 1.5811388300841898),
            entry(JsAggregationOperation.FIRST, 0.0),
            entry(JsAggregationOperation.LAST, 4.0));

    public void testQueryDefinedConfigs() {
        connect(tables)
                .then(session -> {
                    return session.getTable("strings", true)
                            .then(table1 -> {
                                // make sure the config is null, since it wasn't defined in the config
                                assertNull(table1.getTotalsTableConfig());

                                // check that we get a totals table back from the default config, with only the
                                // column that the default operation can aggregate
                                // noinspection unchecked
                                return (Promise<Object[]>) Promise.all(new Promise[] {
                                        table1.getTotalsTable(null).then(totals1 -> {
                                            assertEquals(1, totals1.getColumns().length);
                                            assertEquals(1, totals1.getSize(), DELTA);
                                            return Promise.resolve(totals1);
                                        }),
                                        table1.getGrandTotalsTable(null).then(totals11 -> {
                                            assertEquals(1, totals11.getColumns().length);
                                            assertEquals(1, totals11.getSize(), DELTA);
                                            return Promise.resolve(totals11);
                                        }),
                                });
                            })
                            .then(ignored -> Promise.resolve(session));
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

    /**
     * Requests the totals in the same turn as a source filter, before the filtered state is running. The totals must
     * end up reflecting the filter, even though the filter's state change fired before the totals could listen for it.
     */
    public void testTotalsRequestedWhileFilterPending() {
        long[] lastCount = {-1};
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(8000);
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    return table.getTotalsTable(null).then(totals -> {
                        totals.<TableData>addEventListener(JsTable.EVENT_UPDATED, e -> {
                            TableData data = e.getDetail();
                            if (data.getRows().length == 1) {
                                lastCount[0] = data.getRows().getAt(0).get(totals.findColumn("I"))
                                        .<LongWrapper>cast().getWrapped();
                            }
                        });
                        totals.setViewport(0, 100, null, null, null);
                        // K == 0 keeps rows 0, 2 and 4; an interim update with all five rows is acceptable
                        return this.<ViewportData>waitForEventWhere(totals, JsTable.EVENT_UPDATED,
                                singleRowWith(totals, "I", 3), 3001)
                                .then(ok -> Promise.resolve(table), err -> Promise.reject(
                                        "totals never reflected the pending filter; last count seen " + lastCount[0]));
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testTotalsOnFilteredTable() {
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
                    // Now, change the filter on the original table, and expect the totals table to automatically
                    // update.

                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(1.0))
                    });
                    table.setViewport(0, 100, null);// not strictly required, but part of the normal usage

                    return promiseAllThen(table,
                            waitForEvent(table, JsTable.EVENT_FILTERCHANGED, 2002).onInvoke(table),
                            totalPromises[0] = waitForEvent(totalTables[0], JsTable.EVENT_UPDATED,
                                    checkTotals(totalTables[0], 2, 5, 1, "b1"), 2503));
                })
                .then(table -> {
                    // The grand totals ignore the filter, so they neither update nor change.
                    return totalTables[1].getViewportData().then(viewportData -> {
                        assertTotals(viewportData, totalTables[1], 5, 6, 0, "b2");
                        return Promise.resolve(table);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * Requests the totals table while the source is unfiltered, then applies a filter before that request resolves, as
     * a UI restoring saved filters does. The totals table must end up reflecting the filter.
     */
    public void testFilterAppliedWhileTotalsFetchPending() {
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(6000);
                    Promise<JsTotalsTable> totalsPromise = table.getTotalsTable(null);
                    // the totals request is in flight against the unfiltered table; now change the source
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    table.setViewport(0, 100, null);
                    return totalsPromise.then(totals -> {
                        totals.setViewport(0, 100, null, null, null);
                        // an update with the unfiltered totals may arrive first; wait for one that matches the filter
                        return this.<ViewportData>waitForEventWhere(totals, JsTable.EVENT_UPDATED,
                                e -> e.getDetail().getRows().length == 1 && e.getDetail().getRows().getAt(0)
                                        .get(totals.findColumn("I")).<LongWrapper>cast().getWrapped() == 3,
                                2501);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * Changes the source while the initial totals request is in flight, in a way that makes the rebase fail before that
     * request resolves. The caller then holds a totals table that is behind its source; the failure must be delivered
     * to a listener attached after the promise resolves, not lost to the console.
     */
    public void testRebaseFailsBeforeInitialTotalsFetchResolves() {
        // on code that logs the failure instead of delivering it, the log must not end the test early
        errorLogsDontFail();
        JsTotalsTable[] holder = {null};
        connect(tables)
                .then(table("slowTotals"))
                .then(table -> {
                    delayTestFinish(10000);
                    // sum the slow column so the first fetch takes a couple of seconds
                    JsTotalsTableConfig config = new JsTotalsTableConfig();
                    config.defaultOperation = JsAggregationOperation.SKIP;
                    config.operationMap.set("I", Js.uncheckedCast(new JsString[] {toJsString("Sum")}));
                    config.operationMap.set("S", Js.uncheckedCast(new JsString[] {toJsString("Sum")}));
                    Promise<JsTotalsTable> totalsPromise = table.getTotalsTable(config);
                    // retype both summed columns while that fetch is in flight; the rebased aggregate has nothing
                    // left to sum, and the server refuses it before the first fetch resolves
                    table.applyCustomColumns(JsArray.of(
                            JsTable.CustomColumnArgUnionType.of("I = `x`"),
                            JsTable.CustomColumnArgUnionType.of("S = `y`")));
                    // the rebase failed before anyone could hold this table, so the totals would be behind the
                    // source; the request must fail with the server's reason rather than hand those totals out
                    return totalsPromise.then(
                            totals -> Promise.reject("expected the totals request to fail, got " + totals),
                            error -> {
                                assertTrue("unexpected reason: " + error,
                                        String.valueOf(error).contains("at least one aggregations"));
                                assertTrue("failure was logged: " + describeErrorLogs(), errorLogsSeen.isEmpty());
                                return Promise.resolve(table);
                            });
                })
                .then(table -> {
                    // the source is unaffected; put its columns back and the same request succeeds. Going back to an
                    // existing state needs no server round trip, so the event fires synchronously: listen first
                    IThenable<JsTable> restored =
                            waitForEvent(table, JsTable.EVENT_CUSTOMCOLUMNSCHANGED, 2002).onInvoke(table);
                    table.applyCustomColumns(new JsArray<>());
                    return restored;
                })
                .then(table -> {
                    JsTotalsTableConfig config = new JsTotalsTableConfig();
                    config.defaultOperation = JsAggregationOperation.SKIP;
                    config.operationMap.set("I", Js.uncheckedCast(new JsString[] {toJsString("Sum")}));
                    config.operationMap.set("S", Js.uncheckedCast(new JsString[] {toJsString("Sum")}));
                    return table.getTotalsTable(config).then(totals -> {
                        holder[0] = totals;
                        totals.setViewport(0, 100, null, null, null);
                        return this.<ViewportData>waitForEventWhere(totals, JsTable.EVENT_UPDATED,
                                e -> e.getDetail().getRows().length == 1 && e.getDetail().getRows().getAt(0)
                                        .get(totals.findColumn("I")).asDouble() == 10.0,
                                3001).then(ignored -> Promise.resolve(table));
                    });
                })
                .then(table -> {
                    assertTrue("no failure may be logged: " + describeErrorLogs(), errorLogsSeen.isEmpty());
                    holder[0].close();
                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * A rebase that is still in flight when the source changes again is superseded; its failure is not the totals
     * table's failure. Here the superseded rebase fails after the replacement has succeeded, with the totals table
     * already in the caller's hands: no event and no log.
     */
    public void testSupersededRebaseFailureIsIgnored() {
        errorLogsDontFail();
        JsTotalsTable[] holder = {null};
        List<Object> failures = new ArrayList<>();
        connect(tables)
                .then(table("rebaseRace"))
                .then(table -> {
                    delayTestFinish(10000);
                    return table.getTotalsTable(sumOf("I", "F")).then(totals -> {
                        holder[0] = totals;
                        totals.addEventListener(JsTable.EVENT_REQUEST_FAILED, e -> failures.add(e.getDetail()));
                        totals.setViewport(0, 100, null, null, null);
                        return this.<ViewportData>waitForEventWhere(totals, JsTable.EVENT_UPDATED,
                                sumsOf(totals, 10, 10), 2001).then(ignored -> Promise.resolve(table));
                    });
                })
                .then(table -> {
                    // make F slow to fail; the rebase that follows is in flight for about a second
                    table.applyCustomColumns(JsArray.of(JsTable.CustomColumnArgUnionType.of("F = G")));
                    return sourceCustomColumnsChanged(table, 2002);
                })
                .then(table -> {
                    // supersede it with a change whose rebase succeeds at once
                    table.applyCustomColumns(JsArray.of(JsTable.CustomColumnArgUnionType.of("F = (double) i * 2")));
                    return this.<ViewportData>waitForEventWhere(holder[0], JsTable.EVENT_UPDATED,
                            sumsOf(holder[0], 10, 20), 2003).then(ignored -> Promise.resolve(table));
                })
                // the superseded rebase holds its state until it fails; wait for that rather than guess at the time
                .then(table -> waitForNoRetainedState(holder[0], 4000).then(ignored -> Promise.resolve(table)))
                .then(table -> {
                    assertTrue("superseded failure reported: " + failures, failures.isEmpty());
                    assertTrue("superseded failure logged: " + describeErrorLogs(), errorLogsSeen.isEmpty());
                    assertTrue(holder[0].state().isRunning());
                    holder[0].close();
                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * As {@link #testSupersededRebaseFailureIsIgnored()}, but everything happens while the initial totals request is
     * still in flight. The request must resolve, since the rebase that counts succeeded, and nothing may be logged.
     */
    public void testSupersededRebaseFailureBeforeInitialTotalsFetchResolves() {
        errorLogsDontFail();
        JsTotalsTable[] holder = {null};
        List<Object> failures = new ArrayList<>();
        connect(tables)
                .then(table("rebaseRace"))
                .then(table -> {
                    delayTestFinish(10000);
                    // summing S keeps the first fetch in flight for about two seconds; F comes first so that the
                    // failing rebase reads it, and fails, before it gets to S
                    Promise<JsTotalsTable> totalsPromise = table.getTotalsTable(sumOf("F", "S", "I"));
                    table.applyCustomColumns(JsArray.of(JsTable.CustomColumnArgUnionType.of("F = G")));
                    return sourceCustomColumnsChanged(table, 2001)
                            .then(ignored -> {
                                // the failing rebase is in flight; supersede it
                                table.applyCustomColumns(
                                        JsArray.of(JsTable.CustomColumnArgUnionType.of("F = (double) i * 2")));
                                return totalsPromise;
                            })
                            .then(totals -> {
                                holder[0] = totals;
                                totals.addEventListener(JsTable.EVENT_REQUEST_FAILED,
                                        e -> failures.add(e.getDetail()));
                                totals.setViewport(0, 100, null, null, null);
                                return this.<ViewportData>waitForEventWhere(totals, JsTable.EVENT_UPDATED,
                                        sumsOf(totals, 10, 20), 4001).then(ignored -> Promise.resolve(table));
                            });
                })
                .then(table -> waitForNoRetainedState(holder[0], 4000).then(ignored -> Promise.resolve(table)))
                .then(table -> {
                    assertTrue("superseded failure reported: " + failures, failures.isEmpty());
                    assertTrue("superseded failure logged: " + describeErrorLogs(), errorLogsSeen.isEmpty());
                    holder[0].close();
                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * As {@link #testFilterAppliedWhileTotalsFetchPending()}, but the initial totals request is slow enough that the
     * filter and the rebase it triggers both complete first. The totals promise must still resolve, and the totals must
     * reflect the filter.
     */
    public void testRebaseCompletesBeforeInitialTotalsFetch() {
        connect(tables)
                .then(table("slowTotals"))
                .then(table -> {
                    delayTestFinish(10000);
                    // aggregating all five rows reads the slow column five times
                    Promise<JsTotalsTable> totalsPromise = table.getTotalsTable(null);
                    // the filtered aggregate reads it only three times, so the rebase finishes first
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    return totalsPromise.then(totals -> {
                        totals.setViewport(0, 100, null, null, null);
                        return this.<ViewportData>waitForEventWhere(totals, JsTable.EVENT_UPDATED,
                                e -> e.getDetail().getRows().length == 1 && e.getDetail().getRows().getAt(0)
                                        .get(totals.findColumn("I")).asDouble() == 6.0,
                                5000);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * Two filter changes while the slow initial totals request is pending. The second filtered aggregate reads fewer
     * rows than the first, so the replacement states can run out of order. The totals must settle on the last filter.
     */
    public void testRepeatedFilterChangesDuringInitialTotalsFetch() {
        connect(tables)
                .then(table("slowTotals"))
                .then(table -> {
                    delayTestFinish(10000);
                    Promise<JsTotalsTable> totalsPromise = table.getTotalsTable(null);
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(1.0))
                    });
                    return totalsPromise.then(totals -> {
                        totals.setViewport(0, 100, null, null, null);
                        // K == 1 keeps rows 1 and 3, so I sums to 4
                        return this.<ViewportData>waitForEventWhere(totals, JsTable.EVENT_UPDATED,
                                e -> e.getDetail().getRows().length == 1 && e.getDetail().getRows().getAt(0)
                                        .get(totals.findColumn("I")).asDouble() == 4.0,
                                5000);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * Closes the source table while a rebase of the totals is in flight. The rebase must be dropped: its state is
     * released rather than left retained, and the totals table stays usable on the totals it already had.
     */
    public void testCloseSourceWhileRebaseFetchPending() {
        JsTotalsTable[] holder = {null};
        connect(tables)
                .then(table("slowTotals"))
                .then(table -> {
                    delayTestFinish(10000);
                    return table.getTotalsTable(null).then(totals -> {
                        holder[0] = totals;
                        totals.setViewport(0, 100, null, null, null);
                        return this.<ViewportData>waitForEventWhere(totals, JsTable.EVENT_UPDATED,
                                e -> e.getDetail().getRows().length == 1 && e.getDetail().getRows().getAt(0)
                                        .get(totals.findColumn("I")).asDouble() == 10.0,
                                5000).then(ignored -> Promise.resolve(table));
                    });
                })
                .then(table -> {
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    return waitForEvent(table, JsTable.EVENT_FILTERCHANGED, 2001).onInvoke(table);
                })
                .then(table -> {
                    // the filtered state is running, so the slow rebase aggregate is now in flight; close the source
                    table.close();
                    return Promise.resolve(table);
                })
                // the rebased state was only ever owned by the totals table's retainer; a rebase that was dropped
                // cleanly has let go of it
                .then(table -> waitForNoRetainedState(holder[0], 4000).then(ignored -> Promise.resolve(table)))
                .then(table -> {
                    assertFalse(holder[0].isClosed());
                    assertEquals(1, holder[0].getSize(), DELTA);
                    holder[0].close();
                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * Closes the source table while the initial totals request is still pending. The totals table must still resolve,
     * on the source state it retained.
     */
    public void testCloseSourceWhileTotalsFetchPending() {
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(5000);
                    Promise<JsTotalsTable> totalsPromise = table.getTotalsTable(null);
                    table.close();
                    return totalsPromise.then(totals -> {
                        totals.setViewport(0, 100, null, null, null);
                        return waitForEvent(totals, JsTable.EVENT_UPDATED,
                                checkTotals(totals, 5, 6., 0., "a1"), 2501);
                    }).then(totals -> {
                        totals.close();
                        return Promise.resolve(totals);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * A custom column added to the source shows up in the totals and in the grand totals, since it changes the
     * unfiltered table as well.
     */
    public void testCustomColumnChangeRebasesTotals() {
        JsTotalsTable[] totalTables = {null, null};
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(6000);
                    return promiseAllThen(table,
                            table.getTotalsTable(null)
                                    .then(totals -> {
                                        totalTables[0] = totals;
                                        totals.setViewport(0, 100, null, null, null);
                                        return waitForEvent(totals, JsTable.EVENT_UPDATED,
                                                checkTotals(totals, 5, 6., 0., "a1"), 2501);
                                    }),
                            table.getGrandTotalsTable(null)
                                    .then(totals -> {
                                        totalTables[1] = totals;
                                        totals.setViewport(0, 100, null, null, null);
                                        return waitForEvent(totals, JsTable.EVENT_UPDATED,
                                                checkTotals(totals, 5, 6., 0., "a2"), 2502);
                                    }));
                })
                .then(table -> {
                    table.applyCustomColumns(JsArray.of(JsTable.CustomColumnArgUnionType.of("X = I * 2")));
                    // the default operation is Count, so both totals tables gain an X column counting 5 rows
                    return promiseAllThen(table,
                            waitForEvent(table, JsTable.EVENT_CUSTOMCOLUMNSCHANGED, 2503).onInvoke(table),
                            waitForEventWhere(totalTables[0], JsTable.EVENT_UPDATED,
                                    singleRowWith(totalTables[0], "X", 5), 2504),
                            waitForEventWhere(totalTables[1], JsTable.EVENT_UPDATED,
                                    singleRowWith(totalTables[1], "X", 5), 2505));
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * A source change that makes the totals table's own filter invalid. The totals table must report the failure and
     * end up on the rebased, unfiltered totals of the current source, never on totals of the previous source.
     */
    public void testTotalsFilterInvalidatedBySourceChange() {
        JsTotalsTable[] holder = {null};
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(8000);
                    return table.getTotalsTable(null).then(totals -> {
                        holder[0] = totals;
                        totals.setViewport(0, 100, null, null, null);
                        return waitForEvent(totals, JsTable.EVENT_UPDATED, checkTotals(totals, 5, 6., 0., "a1"),
                                2001).then(ignored -> Promise.resolve(table));
                    });
                })
                .then(table -> {
                    // add a column to the source; it is counted into the totals as X
                    table.applyCustomColumns(JsArray.of(JsTable.CustomColumnArgUnionType.of("X = I * 2")));
                    return waitForEventWhere(holder[0], JsTable.EVENT_UPDATED, singleRowWith(holder[0], "X", 5), 2002)
                            .then(ignored -> Promise.resolve(table));
                })
                .then(table -> {
                    // filter the totals table on the column that only exists because of the custom column
                    holder[0].applyFilter(new FilterCondition[] {
                            holder[0].findColumn("X").filter().eq(FilterValue.ofNumber(5))
                    });
                    return holder[0].nextEvent(JsTable.EVENT_FILTERCHANGED, 2003.)
                            .then(ignored -> Promise.resolve(table));
                })
                .then(table -> {
                    // remove the custom column from the source; the rebased totals have no X, so re-applying the
                    // totals filter on them fails
                    Promise<Event<Object>> failed = holder[0].nextEvent(JsTable.EVENT_REQUEST_FAILED, 3000.);
                    table.applyCustomColumns(new JsArray<>());
                    return failed.then(ignored -> Promise.resolve(table));
                })
                .then(table -> {
                    assertTrue("totals should be on a running state", holder[0].state().isRunning());
                    assertEquals("the invalid filter must not stick", 0, holder[0].state().getFilters().size());
                    for (int i = 0; i < holder[0].getColumns().length; i++) {
                        assertFalse("totals still have the removed column",
                                "X".equals(holder[0].getColumns().getAt(i).getName()));
                    }
                    // and the viewport comes back on its own, showing totals of the current source
                    return waitForEvent(holder[0], JsTable.EVENT_UPDATED, checkTotals(holder[0], 5, 6., 0., "after"),
                            2004).then(ignored -> Promise.resolve(table));
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * A totals table with its own filter is rebased when the source changes. The states it was on before the rebase,
     * the filtered totals and the totals root under them, are no longer reachable from it and must be released, and
     * closing the totals table afterward must leave nothing bound to it.
     */
    public void testRebaseReleasesPreviousTotalsStates() {
        JsTotalsTable[] holder = {null};
        List<ClientTableState> previous = new ArrayList<>();
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(8000);
                    return table.getTotalsTable(null).then(totals -> {
                        holder[0] = totals;
                        totals.setViewport(0, 100, null, null, null);
                        return waitForEvent(totals, JsTable.EVENT_UPDATED, checkTotals(totals, 5, 6., 0., "a1"),
                                2001).then(ignored -> Promise.resolve(table));
                    });
                })
                .then(table -> {
                    // give the totals table a filter of its own, one that still matches after the source changes
                    holder[0].applyFilter(new FilterCondition[] {
                            holder[0].findColumn("J__Min").filter().eq(FilterValue.ofNumber(0))
                    });
                    return holder[0].nextEvent(JsTable.EVENT_FILTERCHANGED, 2002.)
                            .then(ignored -> Promise.resolve(table));
                })
                .then(table -> {
                    for (ClientTableState state : holder[0].state().reversed()) {
                        previous.add(state);
                    }
                    assertEquals("filtered totals over the totals root", 2, previous.size());
                    // change the source; the totals are rebuilt from the filtered source and the filter re-applied
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    return waitForEventWhere(holder[0], JsTable.EVENT_UPDATED, singleRowWith(holder[0], "I", 3), 2003)
                            .then(ignored -> Promise.resolve(table));
                })
                .then(waitFor(500))
                .then(table -> {
                    for (ClientTableState state : previous) {
                        assertEquals("previous totals state not released: " + state,
                                ClientTableState.ResolutionState.RELEASED, state.getResolution());
                    }
                    holder[0].close();
                    return Promise.resolve(table);
                })
                .then(waitFor(500))
                .then(table -> {
                    JsTable wrapped = holder[0].getWrappedTable();
                    for (ClientTableState state : holder[0].getConnection().getCache().getAllStates()) {
                        for (JsTable bound : state.getBoundTables()) {
                            assertNotSame("closed totals table still bound to " + state, wrapped, bound);
                        }
                    }
                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * Requests the totals table right after a filter the server will reject. The source rolls back to its last good
     * state, and the totals must reflect that state rather than fail.
     */
    public void testTotalsRequestedDuringFailingFilter() {
        // the rejected filter is logged as an error by the table
        errorLogsDontFail();
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(6000);
                    table.applyFilter(new FilterCondition[] {
                            FilterCondition.invoke("noSuchFunction", FilterValue.ofNumber(1))
                    });
                    Promise<JsTotalsTable> totalsPromise = table.getTotalsTable(null);
                    return promiseAllThen(table,
                            table.nextEvent(JsTable.EVENT_REQUEST_FAILED, 2001.),
                            totalsPromise.then(totals -> {
                                totals.setViewport(0, 100, null, null, null);
                                return waitForEvent(totals, JsTable.EVENT_UPDATED,
                                        checkTotals(totals, 5, 6., 0., "a1"), 2502);
                            }));
                })
                .then(table -> {
                    assertEquals(0, table.getFilter().length);
                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * A totals config the server rejects fails the totals promise and leaves the source table usable.
     */
    public void testInvalidTotalsConfigRejects() {
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(5000);
                    JsTotalsTableConfig config = new JsTotalsTableConfig();
                    config.groupBy.push("NoSuchColumn");
                    return table.getTotalsTable(config).then(
                            totals -> Promise.reject("Expected the totals request to fail"),
                            error -> Promise.resolve(table));
                })
                .then(table -> table.getTotalsTable(null).then(totals -> {
                    totals.setViewport(0, 100, null, null, null);
                    return waitForEvent(totals, JsTable.EVENT_UPDATED, checkTotals(totals, 5, 6., 0., "a1"), 2501);
                }))
                .then(this::finish).catch_(this::report);
    }

    /**
     * Requests totals of a table that is already closed. The request must be rejected without creating anything: no
     * client state, no export.
     */
    public void testTotalsOfClosedTableRejects() {
        int[] statesBefore = {0};
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(6000);
                    table.close();
                    return Promise.resolve(table);
                })
                // let the closed table's own states be released before counting
                .then(waitFor(100))
                .then(table -> {
                    statesBefore[0] = countStates(table);
                    return table.getTotalsTable(sumOf("I")).then(
                            totals -> Promise.reject("expected the totals request to fail, got " + totals),
                            error -> {
                                assertTrue("unexpected reason: " + error, String.valueOf(error).contains("closed"));
                                return Promise.resolve(table);
                            });
                })
                .then(table -> table.getTotalsTable(null).then(
                        totals -> Promise.reject("expected the totals request to fail, got " + totals),
                        error -> {
                            assertTrue("unexpected reason: " + error, String.valueOf(error).contains("closed"));
                            return Promise.resolve(table);
                        }))
                .then(waitFor(100))
                .then(table -> {
                    assertEquals("a rejected request must leave no state behind", statesBefore[0], countStates(table));
                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * Changes the source table and closes it before the totals table's deferred rebase can run. The totals table must
     * stay usable on its last good state, and nothing may throw.
     */
    public void testCloseSourceWhileTotalsRebasePending() {
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(5000);
                    return table.getTotalsTable(null)
                            .then(totals -> {
                                totals.setViewport(0, 100, null, null, null);
                                return waitForEvent(totals, JsTable.EVENT_UPDATED,
                                        checkTotals(totals, 5, 6., 0., "a1"), 2501);
                            })
                            .then(totals -> {
                                table.applyFilter(new FilterCondition[] {
                                        table.findColumn("K").filter().eq(FilterValue.ofNumber(0.0))
                                });
                                table.close();
                                return Promise.resolve(totals);
                            })
                            // let the deferred rebase callbacks run; an exception there fails the test
                            .then(waitFor(500))
                            .then(totals -> {
                                assertFalse(totals.isClosed());
                                assertEquals(1, totals.getSize(), DELTA);
                                totals.close();
                                return Promise.resolve(totals);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * A filter change on the source table rebases the totals table exactly once, and does not rebase the grand totals
     * table at all, since its source is the same unfiltered ancestor before and after.
     */
    public void testFilterChangeRebasesTotalsOnce() {
        JsTotalsTable[] totalTables = {null, null};
        int[] totalsRebases = {0};
        int[] grandRebases = {0};
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    delayTestFinish(6000);
                    return promiseAllThen(table,
                            table.getTotalsTable(null)
                                    .then(totals -> {
                                        totalTables[0] = totals;
                                        totals.addEventListener(JsTable.INTERNAL_EVENT_STATECHANGED,
                                                e -> totalsRebases[0]++);
                                        totals.setViewport(0, 100, null, null, null);
                                        return waitForEvent(totals, JsTable.EVENT_UPDATED,
                                                checkTotals(totals, 5, 6., 0., "a1"), 2501);
                                    }),
                            table.getGrandTotalsTable(null)
                                    .then(totals -> {
                                        totalTables[1] = totals;
                                        totals.addEventListener(JsTable.INTERNAL_EVENT_STATECHANGED,
                                                e -> grandRebases[0]++);
                                        totals.setViewport(0, 100, null, null, null);
                                        return waitForEvent(totals, JsTable.EVENT_UPDATED,
                                                checkTotals(totals, 5, 6., 0., "a2"), 2502);
                                    }));
                })
                .then(table -> {
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("K").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    return promiseAllThen(table,
                            waitForEvent(table, JsTable.EVENT_FILTERCHANGED, 2003).onInvoke(table),
                            waitForEvent(totalTables[0], JsTable.EVENT_UPDATED,
                                    checkTotals(totalTables[0], 3, 6.666666, 0.0, "b1"), 2504));
                })
                // give any superfluous rebase time to show up before counting
                .then(waitFor(500))
                .then(table -> {
                    assertEquals("totals rebases", 1, totalsRebases[0]);
                    assertEquals("grand totals rebases", 0, grandRebases[0]);
                    return Promise.resolve(table);
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

    /**
     * Filters each totals table on its own, then changes the source filter. The rebased totals must have the totals
     * table's own filter re-applied, and the grand totals, whose source is unchanged, must keep theirs.
     */
    public void testFilteringTotalsTable() {
        JsTotalsTable[] totalTables = {null, null};
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
                                                TotalsResults.of(0, 2, 8, 0.0),
                                                TotalsResults.of(1, 2, 5, 1.0)), 2501);
                                    }),
                            table.getGrandTotalsTable(config)
                                    .then(totals -> {
                                        totalTables[1] = totals;
                                        assertEquals(4, totals.getColumns().length);
                                        assertEquals(2, totals.getSize(), DELTA);
                                        totals.setViewport(0, 100, null, null, null);

                                        // confirm the grand totals include the missing row...
                                        return waitForEvent(totals, JsTable.EVENT_UPDATED, checkTotals(totals, "a2",
                                                TotalsResults.of(0, 3, 6.66666, 0.0),
                                                TotalsResults.of(1, 2, 5, 1.0)), 2502);
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
                            waitForEvent(totalTables[0], JsTable.EVENT_UPDATED,
                                    checkTotals(totalTables[0], "b1", TotalsResults.of(1, 2, 5, 1)), 2503),
                            waitForEvent(totalTables[1], JsTable.EVENT_UPDATED,
                                    checkTotals(totalTables[1], "b2", TotalsResults.of(1, 2, 5, 1)), 2504));
                })
                .then(table -> {
                    // Now refilter the original table. The totals table is rebased with its own filter re-applied,
                    // which now removes nothing; the grand totals ignore the source filter and keep theirs.
                    totalTables[0].addEventListener(JsTable.EVENT_REQUEST_FAILED, e -> report(
                            "Totals table request failed: " + e.getDetail()));
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("J").filter().notEq(FilterValue.ofNumber(9.0))
                    });
                    table.setViewport(0, 100, null);// not strictly required, but part of the normal usage

                    return promiseAllThen(table,
                            waitForEvent(table, JsTable.EVENT_FILTERCHANGED, 2505).onInvoke(table),
                            waitForEvent(totalTables[0], JsTable.EVENT_UPDATED, checkTotals(totalTables[0], "d1",
                                    TotalsResults.of(0, 3, 6.666666, 0.0),
                                    TotalsResults.of(1, 1, 1, 1.0)), 2506));
                })
                .then(table -> totalTables[1].getViewportData().then(viewportData -> {
                    assertGroupedTotals(viewportData, totalTables[1], "d2", TotalsResults.of(1, 2, 5, 1));
                    return Promise.resolve(table);
                }))
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

    /**
     * Count aggregation triggers the batch path in fetchTotals: aggregate creates a synthetic "Count" column,
     * updateView renames it to each target column (e.g. "I = Count"), then drop removes "Count". With incorrect batch
     * ordering (drop before updateView), the updateView fails because "Count" was already removed.
     */
    public void testCountAggBatchOrdering() {
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    JsTotalsTableConfig config = new JsTotalsTableConfig();
                    config.defaultOperation = JsAggregationOperation.COUNT;
                    // No per-column overrides: all columns (I, J, K) use Count,
                    // which populates both customColumns and dropColumns, triggering the batch path.

                    return table.getTotalsTable(config)
                            .then(totals -> {
                                assertEquals(3, totals.getColumns().length);
                                assertEquals(1, totals.getSize(), DELTA);
                                totals.setViewport(0, 100, null, null, null);

                                return waitForEvent(totals, JsTable.EVENT_UPDATED, update -> {
                                    ViewportData viewportData = (ViewportData) update.getDetail();
                                    assertEquals(1, viewportData.getRows().length);
                                    // All three columns should show count = 5
                                    assertEquals(5,
                                            viewportData.getRows().getAt(0).get(totals.findColumn("I"))
                                                    .<LongWrapper>cast().getWrapped());
                                    assertEquals(5,
                                            viewportData.getRows().getAt(0).get(totals.findColumn("J"))
                                                    .<LongWrapper>cast().getWrapped());
                                    assertEquals(5,
                                            viewportData.getRows().getAt(0).get(totals.findColumn("K"))
                                                    .<LongWrapper>cast().getWrapped());
                                }, 2500);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * Tests that a user column literally named "Count" survives Count aggregation, despite being a hardcoded column
     * name.
     */
    public void testColumnNamedCountNotDropped() {
        connect(tables)
                .then(table("hasCountColumn"))
                .then(table -> {
                    JsTotalsTableConfig config = new JsTotalsTableConfig();
                    config.defaultOperation = JsAggregationOperation.COUNT;

                    return table.getTotalsTable(config)
                            .then(totals -> {
                                // Both "Count" and "Value" columns should be present
                                assertEquals(2, totals.getColumns().length);
                                assertNotNull("Count column should exist", totals.findColumn("Count"));
                                assertNotNull("Value column should exist", totals.findColumn("Value"));
                                assertEquals(1, totals.getSize(), DELTA);
                                totals.setViewport(0, 100, null, null, null);

                                return waitForEvent(totals, JsTable.EVENT_UPDATED, update -> {
                                    ViewportData viewportData = (ViewportData) update.getDetail();
                                    assertEquals(1, viewportData.getRows().length);
                                    assertEquals(5,
                                            viewportData.getRows().getAt(0).get(totals.findColumn("Count"))
                                                    .<LongWrapper>cast().getWrapped());
                                    assertEquals(5,
                                            viewportData.getRows().getAt(0).get(totals.findColumn("Value"))
                                                    .<LongWrapper>cast().getWrapped());
                                }, 2500);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCreateTotalsTableAggTypes() {
        connect(tables)
                .then(table("hasTotals"))
                .then(table -> {
                    List<Supplier<Promise<JsTotalsTable>>> tests = new ArrayList<>();
                    List<String> aggs = JsTotalsTableConfig.knownAggTypes;
                    for (int i = 0; i < aggs.size(); i++) {
                        String operation = aggs.get(i);
                        JsTotalsTableConfig config = new JsTotalsTableConfig();
                        config.operationMap.set("I",
                                Js.uncheckedCast(new JsString[] {toJsString(operation)}));
                        tests.add(() -> table.getTotalsTable(config).then(totals -> {
                            totals.setViewport(0, 100, null, null, null);

                            return waitForEvent(totals, JsTable.EVENT_UPDATED, update -> {
                                ViewportData viewportData = (ViewportData) update.getDetail();
                                assertEquals(1, viewportData.getRows().length);
                                if (AggValueMap.containsKey(operation)) {
                                    assertEquals(AggValueMap.get(operation),
                                            viewportData.getRows().getAt(0).get(totals.findColumn("I")).asDouble());
                                }
                            }, 1500);
                        }));
                    }

                    return tests.stream().reduce((p1, p2) -> () -> p1.get().then(result -> p2.get())).get().get();
                })
                .then(this::finish).catch_(this::report);
    }

    private static class TotalsResults {
        double k;
        long i;
        double avg;
        double min;

        public TotalsResults(double k, long i, double avg, double min) {
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
        return update -> assertTotals(update.getDetail(), totals, i, avg, min, messages);
    }

    private void assertTotals(TableData viewportData, JsTotalsTable totals, long i, double avg, double min,
            String ext) {
        assertEquals(1, viewportData.getRows().length);
        assertEquals(3, viewportData.getColumns().length);

        assertEquals("I" + ext, i,
                viewportData.getRows().getAt(0).get(totals.findColumn("I")).<LongWrapper>cast().getWrapped());
        assertEquals("J_Avg" + ext, avg,
                viewportData.getRows().getAt(0).get(totals.findColumn("J__Avg")).asDouble(), 0.0001);
        assertEquals("J_Min" + ext, min,
                viewportData.getRows().getAt(0).get(totals.findColumn("J__Min")).asDouble(), 0.0001);
    }

    private Consumer<Event<ViewportData>> checkTotals(
            JsTotalsTable totals,
            String messages,
            TotalsResults... expected) {
        return update -> assertGroupedTotals(update.getDetail(), totals, messages, expected);
    }

    private void assertGroupedTotals(TableData viewportData, JsTotalsTable totals, String ext,
            TotalsResults... expected) {
        assertEquals("Viewport data rows" + ext, expected.length, viewportData.getRows().length);
        assertEquals("Viewport columns" + ext, 4, viewportData.getColumns().length);

        for (int ind = 0; ind < expected.length; ind++) {
            final TotalsResults result = expected[ind];
            assertEquals("K" + ext, result.k,
                    viewportData.getRows().getAt(ind).get(totals.findColumn("K")).asDouble(), 0.0001);
            assertEquals("I" + ext, result.i,
                    viewportData.getRows().getAt(ind).get(totals.findColumn("I")).<LongWrapper>cast().getWrapped());
            assertEquals("J_Avg" + ext, result.avg,
                    viewportData.getRows().getAt(ind).get(totals.findColumn("J__Avg")).asDouble(), 0.0001);
            assertEquals("J_Min" + ext, result.min,
                    viewportData.getRows().getAt(ind).get(totals.findColumn("J__Min")).asDouble(), 0.0001);
        }
    }

    /**
     * Matches an update whose rows include a single row with the given long value in the named column, once that column
     * exists on the totals table.
     */
    private Predicate<Event<ViewportData>> singleRowWith(JsTotalsTable totals, String column, long value) {
        return e -> {
            ViewportData viewportData = e.getDetail();
            if (viewportData.getRows().length != 1
                    || viewportData.getColumns().asList().stream().noneMatch(c -> c.getName().equals(column))) {
                return false;
            }
            return viewportData.getRows().getAt(0).get(totals.findColumn(column)).<LongWrapper>cast()
                    .getWrapped() == value;
        };
    }

    private int countStates(JsTable table) {
        int count = 0;
        for (ClientTableState ignored : table.getConnection().getCache().getAllStates()) {
            count++;
        }
        return count;
    }

    /** The cached states the totals table retains: a rebase in flight holds its state this way until it settles. */
    private List<ClientTableState> statesRetainedBy(JsTotalsTable totals) {
        List<ClientTableState> retained = new ArrayList<>();
        for (ClientTableState state : totals.getConnection().getCache().getAllStates()) {
            if (state.hasRetainer(totals)) {
                retained.add(state);
            }
        }
        return retained;
    }

    /** Resolves once no rebase is in flight for the totals table; rejects naming the states still held. */
    private IThenable<JsTotalsTable> waitForNoRetainedState(JsTotalsTable totals, int timeout) {
        return this.<JsTotalsTable>waitFor(() -> statesRetainedBy(totals).isEmpty(), 50, timeout, totals)
                .then(ok -> Promise.resolve(ok), err -> Promise.reject(
                        "rebased state still retained by the totals table: " + statesRetainedBy(totals)));
    }

    /** Resolves once the source's custom columns have changed; rejects with the reason if the change is refused. */
    private Promise<JsTable> sourceCustomColumnsChanged(JsTable table, int timeout) {
        return Promise.<Object>race(
                waitForEvent(table, JsTable.EVENT_CUSTOMCOLUMNSCHANGED, timeout).onInvoke(table),
                table.nextEvent(JsTable.EVENT_REQUEST_FAILED, (double) timeout)
                        .then(e -> Promise.reject("source change refused: " + e.getDetail())))
                .then(ignored -> Promise.resolve(table));
    }

    /** A config that sums the named columns and skips the rest. */
    private JsTotalsTableConfig sumOf(String... columns) {
        JsTotalsTableConfig config = new JsTotalsTableConfig();
        config.defaultOperation = JsAggregationOperation.SKIP;
        for (String column : columns) {
            config.operationMap.set(column, Js.uncheckedCast(new JsString[] {toJsString("Sum")}));
        }
        return config;
    }

    /** Matches an update with one row whose I and F sums are the given values. */
    private Predicate<Event<ViewportData>> sumsOf(JsTotalsTable totals, double i, double f) {
        return e -> {
            ViewportData data = e.getDetail();
            if (data.getRows().length != 1) {
                return false;
            }
            TableData.Row row = data.getRows().getAt(0);
            return row.get(totals.findColumn("I")).asDouble() == i && row.get(totals.findColumn("F")).asDouble() == f;
        };
    }

    private String describeErrorLogs() {
        StringBuilder sb = new StringBuilder();
        for (Object[] args : errorLogsSeen) {
            sb.append('[');
            for (Object arg : args) {
                sb.append(arg).append(' ');
            }
            sb.append(']');
        }
        return sb.toString();
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

    private <T> Promise<JsTotalsTable> waitForEventWhere(JsTotalsTable table, String eventName,
            Predicate<Event<T>> check, int timeout) {
        return this.<JsTable, T>waitForEventWhere(table.getWrappedTable(), eventName, check, timeout)
                .then(t -> Promise.resolve(table));
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
