//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.subscription.ViewportData;
import io.deephaven.web.client.ide.IdeSession;
import io.deephaven.web.client.ide.SharedExportBytesUnion;
import jsinterop.base.Js;

public class SharedObjectTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("t1 = empty_table(1).update('A=1')")
            .script("t2 = empty_table(1).update('A=2')")
            .script("t3 = empty_table(1).update('A=3')");
    private final TableSourceBuilder nothing = new TableSourceBuilder();

    public void testSharedTicket() {
        connect(tables).then(client1 -> connect(nothing).then(client2 -> {
            delayTestFinish(220000);
            return Promise.all(
                    // use the ascii `1` as our shared id
                    assertCanFetchAndShare(client1, client2, "t1", "1", 1)
                            // try to re-fetch the shared ticket "1"
                            .then(ignore -> getShared(client2, "1", 1)),
                    // make sure we don't just test for prefixes, add a bad prefix
                    assertCanFetchAndShare(client1, client2, "t2", "h/1", 2),
                    // try an empty shared ticket, this should fail
                    assertCanFetchAndShare(client1, client2, "t3", "", 3))

                    // Try some tests that will fail, now that the above shares have worked
                    .then(ignore -> Promise.all(
                            // attempt to reuse "1" as a publish target
                            promiseFails(fetchAndShare(client1, "t1", "1")),
                            // attempt to fetch a non-existent ticket
                            promiseFails(
                                    client2.getSharedObject(SharedExportBytesUnion.of("987"), JsVariableType.TABLE))));
        }))
                .then(this::finish).catch_(this::report);
    }

    private Promise<?> promiseFails(Promise<?> promise) {
        return promise.then(success -> Promise.reject("expected reject"), Promise::resolve);
    }

    private static Promise<?> assertCanFetchAndShare(IdeSession client1, IdeSession client2,
            String tableToFetchAndShare, String sharedTicketBytes, int cellValue) {
        return fetchAndShare(client1, tableToFetchAndShare, sharedTicketBytes)
                .then(sharedTicketValue -> {
                    // verify the returned value is what we created
                    assertEquals(sharedTicketBytes, Js.uncheckedCast(sharedTicketValue));

                    // ask the other client to get that shared object, confirm we can read it
                    return getShared(client2, sharedTicketBytes, cellValue);
                });
    }

    /**
     * Helper that just fetches a scope ticket and shares it.
     */
    private static Promise<SharedExportBytesUnion> fetchAndShare(IdeSession client1, String tableToFetchAndShare,
            String sharedTicketBytes) {
        return client1.getTable(tableToFetchAndShare, true)
                .then(t1 -> {
                    // start with a sample table owned by the first client
                    return client1.shareObject(ServerObject.Union.of(t1), SharedExportBytesUnion.of(sharedTicketBytes));
                });
    }

    /**
     * Helper that just reads a shared ticket.
     */
    private static Promise<JsTable> getShared(IdeSession client2, String sharedTicketBytes, int cellValue) {
        // fetch the shared object from the second client
        return client2.getSharedObject(SharedExportBytesUnion.of(sharedTicketBytes), JsVariableType.TABLE)
                .then(value -> Promise.resolve((JsTable) value))
                .then(table -> {
                    // make sure we got the correct table instance
                    table.setViewport(0, 0);
                    return table.getViewportData().then(data -> {
                        ViewportData v = (ViewportData) data;
                        assertEquals(cellValue, v.getData(0, v.getColumns().getAt(0)).asInt());
                        return Promise.resolve(table);
                    });
                });
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
