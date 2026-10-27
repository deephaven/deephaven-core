//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.AbstractAsyncGwtTestCase;
import io.deephaven.web.client.api.ConnectOptions;
import io.deephaven.web.client.api.CoreClient;
import io.deephaven.web.client.api.CuttingTransportFactory;
import io.deephaven.web.client.api.event.Event;
import io.deephaven.web.client.ide.IdeConnection;
import jsinterop.base.Js;

/**
 * Verifies behavior of full-table subscriptions, as opposed to viewports.
 */
public class TableSubscriptionTestGwt extends AbstractAsyncGwtTestCase {

    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import time_table")
            .script("from datetime import datetime, timedelta")
            .script("growingForward",
                    "time_table(period=\"PT00:00:01\", start_time=datetime.now() - timedelta(minutes=1))" +
                            ".update([\"I=i\", \"J=i*i\", \"K=0\"])")
            // One row from the start, replaced twice a second, so updates carry data continuously
            .script("ticking", "time_table(\"PT0.5S\", start_time=datetime.now() - timedelta(seconds=10))"
                    + ".update([\"X = i\", \"Y = 0\"]).last_by(by=\"Y\")");

    private static Promise<Object> delay(int millis) {
        return new Promise<>((resolve, reject) -> DomGlobal.setTimeout(p -> resolve.onInvoke((Object) null), millis));
    }

    public void testSubscriptionResumesAfterTransportCut() {
        CuttingTransportFactory transport = new CuttingTransportFactory();
        ConnectOptions options = new ConnectOptions();
        options.transportFactory = transport;
        connect(tables, options)
                .then(session -> {
                    delayTestFinish(20_008);
                    return Promise.resolve(session);
                })
                .then(table("ticking"))
                .then(table -> {
                    // A full-table subscription, which no table ever owns
                    DataOptions.SubscriptionOptions subOptions = new DataOptions.SubscriptionOptions();
                    subOptions.columns = Js.cast(table.getColumns());
                    TableSubscription sub = table.createSubscription(subOptions);
                    IdeConnection ide = lastIdeConnection;
                    ide.addEventListener(CoreClient.EVENT_DISCONNECT, e -> {
                    });
                    return assertUpdateReceived(sub, data -> assertEquals(1, data.getRows().length), 5009)
                            .then(ignore -> {
                                transport.cutAll();
                                return waitForEvent(ide, CoreClient.EVENT_RECONNECT, 5010).onInvoke(ide);
                            })
                            .then(ignore -> Promise.race(
                                    assertUpdateReceived(sub, data -> assertEquals(1, data.getRows().length), 8011),
                                    delay(8000).then(late -> Promise.reject(
                                            "full subscription did not resume after the transport was cut"))))
                            .then(ignore -> {
                                sub.close();
                                table.close();
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    @SuppressWarnings("deprecation")
    public void testLegacySubscribeResumesAfterTransportCut() {
        CuttingTransportFactory transport = new CuttingTransportFactory();
        ConnectOptions options = new ConnectOptions();
        options.transportFactory = transport;
        connect(tables, options)
                .then(session -> {
                    delayTestFinish(20_012);
                    return Promise.resolve(session);
                })
                .then(table("ticking"))
                .then(table -> {
                    // The older spelling of a full-table subscription, still present on the release branches
                    TableSubscription sub = table.subscribe(table.getColumns());
                    IdeConnection ide = lastIdeConnection;
                    ide.addEventListener(CoreClient.EVENT_DISCONNECT, e -> {
                    });
                    return assertUpdateReceived(sub, data -> assertEquals(1, data.getRows().length), 5013)
                            .then(ignore -> {
                                transport.cutAll();
                                return waitForEvent(ide, CoreClient.EVENT_RECONNECT, 5014).onInvoke(ide);
                            })
                            .then(ignore -> Promise.race(
                                    assertUpdateReceived(sub, data -> assertEquals(1, data.getRows().length), 8015),
                                    delay(8000).then(late -> Promise.reject(
                                            "subscribe() subscription did not resume after the transport was cut"))))
                            .then(ignore -> {
                                sub.close();
                                table.close();
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * A full subscription's first update after a reconnect is a snapshot. For it to be a correct delta for a consumer
     * that accumulates rows, it has to remove every row known before the reconnect as it adds the snapshot's rows.
     */
    public void testReconnectRemovesTheRowsItKnew() {
        CuttingTransportFactory transport = new CuttingTransportFactory();
        ConnectOptions options = new ConnectOptions();
        options.transportFactory = transport;
        connect(tables, options)
                .then(session -> {
                    delayTestFinish(20_086);
                    return Promise.resolve(session);
                })
                .then(table("growingForward"))
                .then(table -> {
                    IdeConnection ide = lastIdeConnection;
                    ide.addEventListener(CoreClient.EVENT_DISCONNECT, e -> {
                    });
                    DataOptions.SubscriptionOptions subOptions = new DataOptions.SubscriptionOptions();
                    subOptions.columns = Js.cast(table.getColumns());
                    TableSubscription sub = table.createSubscription(subOptions);
                    double[] knownRows = {0};
                    sub.addEventListener(TableSubscription.EVENT_UPDATED,
                            (Event<AbstractTableSubscription.SubscriptionEventData> e) -> knownRows[0] =
                                    e.getDetail().getFullIndex().getSize());
                    return assertUpdateReceived(sub, data -> assertTrue(data.getRows().length > 0), 5087)
                            .then(ignore -> {
                                double knownAtCut = knownRows[0];
                                assertTrue("rows known before the cut", knownAtCut > 0);
                                transport.cutAll();
                                return waitForEvent(ide, CoreClient.EVENT_RECONNECT, 5088).onInvoke(ide)
                                        .then(reconnected -> Promise.race(
                                                this.<TableSubscription, AbstractTableSubscription.SubscriptionEventData>waitForEvent(
                                                        sub, TableSubscription.EVENT_UPDATED, e -> assertEquals(
                                                                "first update after the reconnect must remove the rows known before it",
                                                                knownAtCut, e.getDetail().getRemoved().getSize()),
                                                        8089),
                                                delay(8000).then(late -> Promise.reject(
                                                        "full subscription did not resume after the transport was cut"))));
                            })
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
