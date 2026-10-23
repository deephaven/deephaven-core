//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import io.deephaven.web.client.ide.IdeConnection;
import jsinterop.base.Js;

/**
 * Drops the connection out from under live subscriptions and checks that they come back. Uses a transport the test
 * controls, so the cut lands wherever the stream happens to be, including inside an update.
 */
public class TransportCutTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import time_table")
            .script("from datetime import datetime, timedelta")
            // One row from the start, replaced twice a second
            .script("ticking", "time_table(\"PT0.5S\", start_time=datetime.now() - timedelta(seconds=10))"
                    + ".update([\"X = i\", \"Y = 0\"]).last_by(by=\"Y\")");

    private static Promise<Object> delay(int millis) {
        return new Promise<>((resolve, reject) -> DomGlobal.setTimeout(p -> resolve.onInvoke((Object) null), millis));
    }

    public void testViewportResumesAfterTransportCut() {
        CuttingTransportFactory transport = new CuttingTransportFactory();
        ConnectOptions options = new ConnectOptions();
        options.transportFactory = transport;
        connect(tables, options)
                .then(session -> {
                    delayTestFinish(20_001);
                    return Promise.resolve(session);
                })
                .then(table("ticking"))
                .then(table -> {
                    table.setViewport(0, 0, null);
                    IdeConnection ide = lastIdeConnection;
                    // An unhandled disconnect event is logged as an error, which the harness treats as a failure
                    ide.addEventListener(CoreClient.EVENT_DISCONNECT, e -> {
                    });
                    return assertUpdateReceived(table, 1, 5002)
                            .then(ignore -> {
                                // Every open stream dies, the viewport's Barrage stream included; the connection
                                // sees a transport error and reconnects within the same session
                                transport.cutAll();
                                return waitForEvent(ide, CoreClient.EVENT_RECONNECT, 5003).onInvoke(ide);
                            })
                            .then(ignore -> Promise.race(
                                    assertUpdateReceived(table, 1, 8004),
                                    delay(8000).then(late -> Promise.reject(
                                            "viewport did not resume after the transport was cut"))))
                            .then(ignore -> {
                                table.close();
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testViewportSubscriptionResumesAfterTransportCut() {
        CuttingTransportFactory transport = new CuttingTransportFactory();
        ConnectOptions options = new ConnectOptions();
        options.transportFactory = transport;
        connect(tables, options)
                .then(session -> {
                    delayTestFinish(20_005);
                    return Promise.resolve(session);
                })
                .then(table("ticking"))
                .then(table -> {
                    // A standalone viewport subscription, as opposed to the table's own viewport
                    DataOptions.ViewportSubscriptionOptions subOptions = new DataOptions.ViewportSubscriptionOptions();
                    subOptions.columns = Js.cast(table.getColumns());
                    subOptions.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 0));
                    TableViewportSubscription sub = table.createViewportSubscription(subOptions);
                    IdeConnection ide = lastIdeConnection;
                    ide.addEventListener(CoreClient.EVENT_DISCONNECT, e -> {
                    });
                    return assertUpdateReceived(sub, viewport -> assertEquals(1, viewport.getRows().length), 5006)
                            .then(ignore -> {
                                transport.cutAll();
                                return waitForEvent(ide, CoreClient.EVENT_RECONNECT, 5007).onInvoke(ide);
                            })
                            .then(ignore -> Promise.race(
                                    assertUpdateReceived(sub,
                                            viewport -> assertEquals(1, viewport.getRows().length), 8008),
                                    delay(8000).then(late -> Promise.reject(
                                            "viewport subscription did not resume after the transport was cut"))))
                            .then(ignore -> {
                                sub.close();
                                table.close();
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
