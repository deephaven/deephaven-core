//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import elemental2.core.JsArray;
import elemental2.dom.CustomEvent;
import elemental2.dom.DomGlobal;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.AbstractAsyncGwtTestCase;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.HasEventHandling;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.filter.FilterValue;
import io.deephaven.web.shared.fu.RemoverFn;
import jsinterop.base.Js;

import java.util.Objects;

import static elemental2.dom.DomGlobal.console;

/**
 * Assumes two tables, ticking every 2 seconds:
 *
 * growingForward = db.timeTable("00:00:01").update("I=i", "J=i*i", "K=0") growingBackward =
 * growingForward.sortDescending("Timestamp") blinkOne = db.timeTable("00:00:01").update("I=i",
 * "J=1").lastBy("J").where("I%2 != 0")
 *
 * And another static one:
 *
 * staticTable = emptyTable(100).update("I=i")
 */
public class ViewportTestGwt extends AbstractAsyncGwtTestCase {

    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table, time_table")
            .script("staticTable", "empty_table(100).update(\"I=i\")")
            .script("from datetime import datetime, timedelta")
            .script("growingForward",
                    "time_table(period=\"PT00:00:01\", start_time=datetime.now() - timedelta(minutes=1)).update([\"I=i\", \"J=i*i\", \"K=0\"])")
            .script("growingBackward", "growingForward.sort_descending(\"Timestamp\")")
            .script("blinkOne",
                    "time_table(\"PT00:00:01\").update([\"I=i\", \"J=1\"]).last_by(by=\"J\").where(\"I%2 != 0\")");

    public void testViewportOnStaticTable() {
        connect(tables)
                .then(table("staticTable"))
                .then(table -> {
                    delayTestFinish(5000);

                    int size = (int) table.getSize();
                    int lastRow = size - 1;
                    table.setViewport(0, lastRow, null);
                    return assertUpdateReceived(table, size, 2500);
                })
                .then(table -> {
                    // table has 100 rows, go through each page of 25, make sure the offset and length is sane
                    table.setViewport(0, 24, null);
                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(0, (long) viewport.getOffset());
                        assertEquals(25, viewport.getRows().length);
                    }, 2100);
                })
                .then(table -> {
                    table.setViewport(25, 49, null);
                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(25, (long) viewport.getOffset());
                        assertEquals(25, viewport.getRows().length);
                    }, 2101);
                })
                .then(table -> {
                    table.setViewport(50, 74, null);
                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(50, (long) viewport.getOffset());
                        assertEquals(25, viewport.getRows().length);
                    }, 2102);
                })
                .then(table -> {
                    table.setViewport(75, 99, null);
                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(75, (long) viewport.getOffset());
                        assertEquals(25, viewport.getRows().length);
                    }, 2103);
                })
                .then(this::finish).catch_(this::report);
    }

    // TODO: https://deephaven.atlassian.net/browse/DH-11196
    public void ignore_testViewportOnGrowingTable() {
        connect(tables)
                .then(table("growingForward"))
                .then(waitForTick(2200))
                .then(delayFinish(25_000))
                .then(table -> {
                    // set viewport to actual table size, check that all items are present
                    int size = (int) table.getSize();
                    int lastRow = size - 1;
                    table.setViewport(0, lastRow, null);
                    return assertUpdateReceived(table, size, 1500);
                })
                .then(waitForTick(2201))
                .then(table -> {
                    // set viewport size to be larger than range of items, check only size items are present
                    int size = (int) table.getSize();
                    table.setViewport(0, size, null);
                    return assertUpdateReceived(table, size, 1501);
                })
                .then(waitForTick(2202))
                .then(table -> {
                    table.setViewport(1, 2, null);
                    // start with the last visible item, showing more than one item, should only see one item at first,
                    // but we'll tick forward to see more
                    int size = (int) table.getSize();
                    int lastRow = size - 1;
                    table.setViewport(lastRow, lastRow + 99, null);
                    return assertUpdateReceived(table, 1, 1502);
                })
                .then(waitForTick(2203))
                .then(table -> {
                    // wait for the size to tick once, verify that the current viewport size reflects that
                    double size = table.getSize();
                    double lastRow = size - 1;
                    table.setViewport(size, lastRow + 9, null);
                    return waitFor(() -> table.getSize() == size + 1, 100, 3000, table)
                            .then(waitForEvent(table, JsTable.EVENT_SIZECHANGED, 2510))
                            .then(JsTable::getViewportData)
                            .then(viewportData -> {
                                assertEquals(2, viewportData.getRows().length);
                                // switch back to table for next promise
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testViewportOnUpdatingTable() {
        connect(tables)
                .then(table("growingBackward"))
                .then(table -> {
                    delayTestFinish(4000);
                    // set up a viewport, and watch it show up, and tick once
                    table.setViewport(0, 9, null);
                    return assertUpdateReceived(table, viewportData -> {
                    }, 1004);
                })
                .then(table -> {
                    return assertUpdateReceived(table, viewportData -> {
                    }, 2000);
                })
                .then(this::finish).catch_(this::report);
    }

    private static <T> int indexOf(JsArray<T> array, T object) {
        return indexOf(array.asList().toArray(), object);
    }

    private static <T> int indexOf(Object[] array, T object) {
        for (int i = 0; i < array.length; i++) {
            Object t = array[i];
            if (Objects.equals(t, object)) {
                return i;
            }
        }

        return -1;
    }

    public void testViewportSubsetOfColumns() {
        connect(tables)
                .then(table("growingBackward"))
                .then(table -> {
                    delayTestFinish(8000);
                    table.setViewport(0, 0, Js.uncheckedCast(new Column[] {table.findColumn("I")}));

                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(1, viewport.getColumns().length);
                        assertEquals(0, indexOf(viewport.getColumns(), table.findColumn("I")));

                        assertEquals(1, viewport.getRows().length);
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("I")));
                        assertThrowsException(() -> viewport.getRows().getAt(0).get(table.findColumn("J")));
                        assertThrowsException(() -> viewport.getRows().getAt(0).get(table.findColumn("K")));

                    }, 2501);
                })
                .then(table -> {
                    // don't change viewport, test the same thing again, make sure deltas behave too
                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(1, viewport.getColumns().length);
                        assertEquals(0, indexOf(viewport.getColumns(), table.findColumn("I")));

                        assertEquals(1, viewport.getRows().length);
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("I")));
                        assertThrowsException(() -> viewport.getRows().getAt(0).get(table.findColumn("J")));
                        assertThrowsException(() -> viewport.getRows().getAt(0).get(table.findColumn("K")));

                    }, 2000);
                })
                .then(table -> {
                    table.setViewport(0, 0, Js.uncheckedCast(new Column[] {table.findColumn("J")}));

                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(1, viewport.getColumns().length);
                        assertEquals(0, indexOf(viewport.getColumns(), table.findColumn("J")));

                        assertEquals(1, viewport.getRows().length);
                        assertThrowsException(() -> viewport.getRows().getAt(0).get(table.findColumn("I")));
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("J")));
                        assertThrowsException(() -> viewport.getRows().getAt(0).get(table.findColumn("K")));

                    }, 2502);
                })
                .then(table -> {
                    table.setViewport(0, 0, Js.uncheckedCast(new Column[] {table.findColumn("K")}));

                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(1, viewport.getColumns().length);
                        assertEquals(0, indexOf(viewport.getColumns(), table.findColumn("K")));

                        assertEquals(1, viewport.getRows().length);
                        assertThrowsException(() -> viewport.getRows().getAt(0).get(table.findColumn("I")));
                        assertThrowsException(() -> viewport.getRows().getAt(0).get(table.findColumn("J")));
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("K")));

                    }, 2503);
                })
                .then(table -> {
                    table.setViewport(0, 0, Js.uncheckedCast(new Column[] {
                            table.findColumn("J"),
                            table.findColumn("K")
                    }));

                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(2, viewport.getColumns().length);
                        assertEquals(0, indexOf(viewport.getColumns(), table.findColumn("J")));
                        assertEquals(1, indexOf(viewport.getColumns(), table.findColumn("K")));

                        assertEquals(1, viewport.getRows().length);
                        assertThrowsException(() -> viewport.getRows().getAt(0).get(table.findColumn("I")));
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("J")));
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("K")));

                    }, 2504);
                })
                .then(table -> {
                    table.setViewport(0, 0, Js.uncheckedCast(new Column[] {
                            table.findColumn("J"),
                            table.findColumn("Timestamp"),
                            table.findColumn("I"),
                            table.findColumn("K")
                    }));

                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(4, viewport.getColumns().length);
                        assertEquals(0, indexOf(viewport.getColumns(), table.findColumn("Timestamp")));
                        assertEquals(1, indexOf(viewport.getColumns(), table.findColumn("I")));
                        assertEquals(2, indexOf(viewport.getColumns(), table.findColumn("J")));
                        assertEquals(3, indexOf(viewport.getColumns(), table.findColumn("K")));

                        assertEquals(1, viewport.getRows().length);
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("Timestamp")));
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("I")));
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("J")));
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("K")));

                    }, 2505);
                })
                .then(table -> {
                    table.setViewport(0, 0, null);

                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(4, viewport.getColumns().length);
                        assertEquals(0, indexOf(viewport.getColumns(), table.findColumn("Timestamp")));
                        assertEquals(1, indexOf(viewport.getColumns(), table.findColumn("I")));
                        assertEquals(2, indexOf(viewport.getColumns(), table.findColumn("J")));
                        assertEquals(3, indexOf(viewport.getColumns(), table.findColumn("K")));

                        assertEquals(1, viewport.getRows().length);
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("Timestamp")));
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("I")));
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("J")));
                        assertNotNull(viewport.getRows().getAt(0).get(table.findColumn("K")));

                    }, 2506);
                })
                .then(this::finish).catch_(this::report);
    }

    // TODO: https://deephaven.atlassian.net/browse/DH-11196
    public void ignore_testEmptyTableWithViewport() {
        // confirm that when the viewport is set on an empty table that we get exactly one update event
        connect(tables)
                .then(table("staticTable"))
                .then(table -> {
                    delayTestFinish(10000);
                    console.log("size", table.getSize());
                    // change the filter, set a viewport, assert that sizechanged and update both happen once
                    table.applyFilter(new FilterCondition[] {
                            FilterValue.ofBoolean(false).isTrue()
                    });
                    table.setViewport(0, 100, null);
                    return Promise.all(new IThenable<?>[] {
                            // when IDS-2113 is fixed, restore this stronger assertion
                            // assertEventFiresOnce(table, JsTable.EVENT_UPDATED, 1000)
                            waitForEvent(table, JsTable.EVENT_UPDATED, ignore -> {
                            }, 2011),
                            assertEventFiresOnce(table, JsTable.EVENT_SIZECHANGED, 1005)
                    }).then(ignore -> Promise.resolve(table));
                })
                .then(table -> {
                    // reset the filter, wait for back to normal
                    table.applyFilter(new FilterCondition[0]);
                    table.setViewport(0, 100, null);
                    return assertUpdateReceived(table, ignore -> {
                    }, 1006);
                })
                .then(table -> {
                    // change the filter, don't set a viewport, assert only size changes
                    table.applyFilter(new FilterCondition[] {
                            FilterValue.ofBoolean(false).isTrue()
                    });
                    return assertEventFiresOnce(table, JsTable.EVENT_SIZECHANGED, 1007);
                })
                .then(table -> {
                    // set a viewport, assert that update fires and no size change
                    table.setViewport(0, 100, null);
                    // when IDS-2113 is fixed, restore this stronger assertion
                    // return assertEventFiresOnce(table, JsTable.EVENT_UPDATED, 1000);
                    return waitForEvent(table, JsTable.EVENT_UPDATED, ignore -> {
                    }, 2012);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testViewportOutOfRangeOfTable() {
        // confirm that when the viewport is set beyond the range of the table that we get exactly one update event
        connect(tables)
                .then(table("staticTable"))
                .then(table -> {
                    table.setViewport(100, 104, null);

                    return Promise.all(new IThenable<?>[] {
                            // when IDS-2113 is fixed, restore this stronger assertion
                            // assertEventFiresOnce(table, JsTable.EVENT_UPDATED, 1000)
                            waitForEvent(table, JsTable.EVENT_UPDATED, ignore -> {
                            }, 2013)
                    }).then(ignore -> Promise.resolve(table));
                })
                .then(this::finish).catch_(this::report);

    }

    public void testRapidChangingViewport() {
        connect(tables)
                .then(table("staticTable"))
                .then(table -> {
                    delayTestFinish(5000);
                    // test running both synchronously
                    table.setViewport(0, 10, null);
                    table.setViewport(5, 14, null);
                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(5, (int) viewport.getOffset());
                        assertEquals(10, (int) viewport.getRows().length);
                    }, 1008);
                })
                .then(table -> {
                    // test changing the viewport over a microtask (anyone in the web api getting clever with batching?)
                    table.setViewport(0, 10, null);
                    return Promise.resolve((Object) null).then(ignore -> Promise.resolve(table));
                })
                .then(table -> {
                    table.setViewport(6, 14, null);
                    return assertUpdateReceived(table, viewport -> {
                        assertEquals(6, (int) viewport.getOffset());
                        assertEquals(9, (int) viewport.getRows().length);
                    }, 1009);
                })
                .then(table -> {
                    table.setViewport(0, 10, null);
                    return Promise.resolve(table);
                })
                // test again over a 4ms delay, minimum task delay
                .then(waitFor(4))
                .then(table -> {
                    table.setViewport(7, 17, null);
                    return assertUpdateReceived(table, ignored -> {
                    }, 1010)
                            .then(waitFor(JsTable.DEBOUNCE_TIME * 2))
                            .then(t -> {
                                // force the debounce to be processed
                                t.processSnapshot();
                                t.getViewportData().then(vp -> {
                                    // assertEquals(7, (int) vp.getOffset());
                                    assertEquals(11, (int) vp.getRows().length);
                                    return Promise.resolve(vp);
                                });
                                return Promise.resolve(t);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testViewportWithNoInitialItems() {
        // The bug exposed by this case is that a snapshot might start initially empty, but then get
        // a delta to make it non-empty. This test goes further, and waits until it is empty again,
        // and then cycles back to non-empty once more to make sure all the transitions are tested
        connect(tables)
                .then(table("blinkOne"))
                .then(table -> {
                    delayTestFinish(20_000);

                    // first run, assume all columns
                    return helperForViewportWithNoInitialItems(table, null, table.getColumns());
                }).then(table -> {
                    // second, specify only one column to ensure that it is respected
                    Column i = table.findColumn("I");
                    return helperForViewportWithNoInitialItems(table, new Column[] {i}, new JsArray<>(i));
                })
                .then(this::finish).catch_(this::report);
    }

    private IThenable<JsTable> helperForViewportWithNoInitialItems(JsTable t, Column[] requestColumns,
            JsArray<Column> expectedColumns) {
        // wait until zero rows are present, so we can set the viewport and get a zero-row "snapshot"
        return waitFor(() -> t.getSize() == 0, 100, 2000, t)
                .then(table -> {
                    // set up the viewport to only watch for the first row, then wait until zero rows
                    table.setViewport(0, 0, Js.uncheckedCast(requestColumns));

                    // viewport should come back quickly showing no data, and all columns
                    return assertUpdateReceived(table, emptyViewport -> {
                        assertEquals(0, emptyViewport.getRows().length);
                        assertEquals(expectedColumns.length, emptyViewport.getColumns().length);
                    }, 1501);
                })
                .then(table -> {
                    // wait for the next tick, where we get the "first" row added, confirm that the viewport
                    // data is sane
                    return waitForEventWhere(table, "updated", (CustomEvent<ViewportData> e) -> {
                        ViewportData viewport = e.detail;
                        if (viewport.getRows().length != 1) {
                            return false; // wrong data, wait for another event
                        }
                        assertEquals(expectedColumns.length, viewport.getColumns().length);
                        for (int i = 0; i < viewport.getColumns().length; i++) {
                            final Column c = viewport.getColumns().getAt(i);
                            assertNotNull(viewport.getRows().getAt(0).get(c));
                        }
                        return true;
                    }, 2508);
                })
                .then(table -> {
                    // again wait for the table to go back to zero items, make sure it makes sense
                    return waitForEventWhere(table, "updated", (CustomEvent<ViewportData> e) -> {
                        ViewportData emptyViewport = (ViewportData) e.detail;
                        if (emptyViewport.getRows().length != 0) {
                            return false; // wrong data, wait for another event
                        }
                        assertEquals(expectedColumns.length, emptyViewport.getColumns().length);
                        return true;
                    }, 2503);
                })
                .then(table -> {
                    // one more tick later, we'll see the item back again
                    return waitForEventWhere(table, "updated", (CustomEvent<ViewportData> e) -> {
                        ViewportData viewport = (ViewportData) e.detail;
                        if (viewport.getRows().length != 1) {
                            return false; // wrong data, wait for another event
                        }
                        assertEquals(expectedColumns.length, viewport.getColumns().length);
                        for (int i = 0; i < viewport.getColumns().length; i++) {
                            final Column c = viewport.getColumns().getAt(i);
                            assertNotNull(viewport.getRows().getAt(0).get(c));
                        }
                        return true;
                    }, 2511);
                });
    }

    private <T extends HasEventHandling> Promise<T> assertEventFiresOnce(T eventSource, String eventName,
            int intervalInMilliseconds) {
        return new Promise<>((resolve, reject) -> {
            int[] runCount = {0};
            console.log("adding " + eventName + " listener " + eventSource);
            // apparent compiler bug, review in gwt 2.9
            RemoverFn unsub = Js.<HasEventHandling>uncheckedCast(eventSource)
                    .addEventListener(eventName, e -> {
                        runCount[0]++;
                        console.log(eventName + " event observed " + eventSource + ", #" + runCount[0]);
                        if (runCount[0] > 1) {
                            reject.onInvoke("Event " + eventName + " fired " + runCount[0] + " times");
                        }
                    });
            DomGlobal.setTimeout(p0 -> {
                unsub.remove();
                if (runCount[0] == 1) {
                    resolve.onInvoke(eventSource);
                } else {
                    reject.onInvoke("Event " + eventName + " fired " + runCount[0] + " times");
                }
            }, intervalInMilliseconds);
        });
    }

    private void assertThrowsException(Runnable r) {
        try {
            r.run();
            fail("Expected exception");
        } catch (Exception ignore) {
            // expected
        }
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}

