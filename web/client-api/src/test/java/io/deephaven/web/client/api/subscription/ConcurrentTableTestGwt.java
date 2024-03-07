//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.google.gwt.junit.DoNotRunWith;
import com.google.gwt.junit.Platform;
import elemental2.core.JsArray;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.AbstractAsyncGwtTestCase;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.filter.FilterCondition;
import jsinterop.base.Js;

import static elemental2.dom.DomGlobal.console;

@DoNotRunWith(Platform.HtmlUnitBug)
public class ConcurrentTableTestGwt extends AbstractAsyncGwtTestCase {

    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import time_table")
            .script("from datetime import datetime, timedelta")
            .script("updates",
                    "time_table(period=\"PT00:00:01\", start_time=datetime.now() - timedelta(minutes=1)).update_view(\"condition= (i%2 == 0)\").sort_descending(\"Timestamp\")");

    /**
     * Take a table, get a viewport on it, copy it, filter the copy, get a viewport on the copy, ensure updates arrive
     * on both copies.
     *
     * Assumes a table exists called `updates` with more than 20 items, where each page gets ticked at least once per
     * second, and there is a column `condition` which is `true` for half and `false` for half.
     */
    public void testOldCopyKeepsGettingUpdates() {
        connect(tables)
                .then(table("updates"))
                .then(table -> {
                    // assign a viewport
                    table.setViewport(0, 9, null);
                    console.log("viewport set");

                    // within the usual update interval, expect to see an update (added 50% to ensure we get it)
                    delayTestFinish(2002);
                    return assertUpdateReceived(table, 10, 1500);
                }).then(table -> {
                    // copy the table, apply a filter to it
                    console.log("applying filter");
                    Promise<JsTable> filteredCopy = table.copy(true).then(toFilter -> {
                        Column conditionColumn =
                                toFilter.getColumns().find((c, p1, p2) -> c.getName().equals("condition"));
                        toFilter.applyFilter(new FilterCondition[] {conditionColumn.filter().isTrue()});
                        toFilter.setViewport(0, 9, null);
                        return new Promise<>((resolve, reject) -> {
                            toFilter.addEventListener(JsTable.EVENT_FILTERCHANGED, e -> {
                                resolve.onInvoke(toFilter);
                            });
                        });
                    });

                    console.log("testing results");
                    // confirm all are getting updates - higher timeout for filtered table, since half the items are
                    // skipped, 2s tick now
                    delayTestFinish(3501);
                    // noinspection unchecked
                    return Promise.all(new IThenable[] {
                            assertUpdateReceived(filteredCopy, 10, 3001),
                            assertUpdateReceived(table, 10, 2501)
                    });
                }).then(this::finish).catch_(this::report);
    }

    public void testTwoIdenticalTablesWithDifferentViewports() {
        connect(tables)
                .then(delayFinish(2007))
                .then(table("updates"))
                .then(table -> {
                    return Promise.all(new Promise[] {Promise.resolve(table), table.copy(true)});
                })
                .then(result -> {
                    JsArray<JsTable> array = Js.uncheckedCast(result);
                    array.getAt(0).setViewport(0, 9, null);
                    array.getAt(1).setViewport(10, 24, null);
                    delayTestFinish(3007);
                    // noinspection unchecked
                    return Promise.all(new Promise[] {
                            assertUpdateReceived(array.getAt(0), 10, 2007),
                            assertUpdateReceived(array.getAt(1), 15, 2008)
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
