package io.deephaven.web.client.api;

import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.subscription.ViewportData;
import io.deephaven.web.client.ide.IdeConnection;
import io.deephaven.web.client.ide.IdeSession;
import jsinterop.base.Js;

public class SharedObjectTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("t1 = empty_table(1).update('A=1')")
            .script("t2 = empty_table(1).update('A=2')")
            .script("t3 = empty_table(1).update('A=3')");
    private final TableSourceBuilder nothing = new TableSourceBuilder();
    public void testSharedTicket() {
        connect(tables).then(client1 -> {
            return connect(nothing).then(client2 -> {
                delayTestFinish(220000);
                return
                        // use the ascii `1` as our shared id
                        assertCanFetchAndShare(client1, client2, "t1", "1", 1)
                                .then(ignore1 ->
                        // make sure we don't just test for prefixes, add a bad prefix
                        assertCanFetchAndShare(client1, client2, "t2", "h/1", 2))
                                        .then(ignore ->
                        // try an empty shared ticket, this should fail
                        assertCanFetchAndShare(client1, client2, "t3", "", 3)
                ).then(ignore -> Promise.all(
                        // attempt to reuse 1
                        promiseFails(fetchAndShare(client1, "t1", "1"))
                ));
            });
        })
        .then(this::finish).catch_(this::report);
    }

    private IThenable<Object> promiseFails(Promise<IdeConnection.SharedExportBytesUnion> promise) {
        return promise.then(success -> Promise.reject("expected reject"), failure -> Promise.resolve(failure));
    }

    private static Promise<Object> assertCanFetchAndShare(IdeSession client1, IdeSession client2, String tableToFetchAndShare, String ticketBytes, int cellValue) {
        return fetchAndShare(client1, tableToFetchAndShare, ticketBytes)
                .then(sharedTicketValue -> {
                    // verify the returned value is what we created
                    assertEquals(ticketBytes, Js.uncheckedCast(sharedTicketValue));

                    // fetch the shared object from the second client
                    return client2.getSharedObject(IdeConnection.SharedExportBytesUnion.of(ticketBytes), JsVariableType.TABLE);
                })
                .then(value -> Promise.resolve((JsTable) value))
                .then(table -> {
                    // make sure we got the correct table instance
                    table.setViewport(0, 0);
                    return table.getViewportData();
                }).then(data -> {
                    ViewportData v = (ViewportData) data;
                    assertEquals(cellValue, v.getData(0, v.getColumns().getAt(0)).asInt());
                    return null;
                });
    }

    private static Promise<IdeConnection.SharedExportBytesUnion> fetchAndShare(IdeSession client1, String tableToFetchAndShare, String ticketBytes) {
        return client1.getTable(tableToFetchAndShare, true)
                .then(t1 -> {
                    // start with a sample table owned by the first client
                    return client1.shareObject(t1, IdeConnection.SharedExportBytesUnion.of(ticketBytes));
                });
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
