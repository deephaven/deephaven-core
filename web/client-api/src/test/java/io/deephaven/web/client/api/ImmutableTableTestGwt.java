//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.ReadonlyArray;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.base.Js;

import java.util.stream.StreamSupport;

/**
 * Exercises the immutable, chainable table API end to end: chain before awaiting, read data from the result, close it,
 * and confirm intermediate tables are released.
 */
public class ImmutableTableTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("source", "empty_table(5).update([\"X = i\", \"Y = i * 2\"])");

    private static ReadonlyArray<JsTable.CustomColumnArgUnionType> cols(String... exprs) {
        return Js.uncheckedCast(JsArray.of(exprs));
    }

    private static ReadonlyArray<Sort.SortUnion> sorts(String... columns) {
        return Js.uncheckedCast(JsArray.of(columns));
    }

    private static Promise<Object> delay(int millis) {
        return new Promise<>((resolve, reject) -> DomGlobal.setTimeout(p -> resolve.onInvoke((Object) null), millis));
    }

    private static long batchStates(WorkerConnection connection) {
        return StreamSupport.stream(connection.getCache().getAllStates().spliterator(), false)
                .filter(s -> "batch operation".equals(s.getFetchSummary()))
                .filter(s -> s.getResolution() != ClientTableState.ResolutionState.RELEASED)
                .count();
    }

    public void testChainBeforeAwait() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_000);
                    JsPendingTable<JsTable> source = session.getTable("source", null);
                    // Two operations chained before anything has been awaited, one await for the lot
                    return source.update(cols("Z = X + Y")).sort(sorts("Z")).resolve();
                })
                .then(resolved -> {
                    assertEquals(5., resolved.getSize(), 0);
                    assertEquals(3, resolved.getColumns().getLength());
                    Column z = resolved.findColumn("Z");
                    assertNotNull(z);

                    DataOptions.SnapshotOptions opts = new DataOptions.SnapshotOptions();
                    opts.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 4));
                    opts.columns = Js.cast(resolved.getColumns());
                    return resolved.createSnapshot(opts).then(data -> {
                        assertEquals(5, data.getRows().length);
                        // Z = X + Y = i + 2i
                        assertEquals(6, data.getData(2, z).asInt());
                        resolved.close();
                        return Promise.resolve(resolved);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testAwaitTwiceIsOneOwner() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_000);
                    JsPendingTable<JsResolvedTable> pending =
                            session.getTable("source", null).head(Js.uncheckedCast(2.0));
                    return Promise.all(pending.resolve(), pending.resolve()).then(both -> {
                        assertSame(both[0], both[1]);
                        JsResolvedTable resolved = (JsResolvedTable) both[0];
                        assertEquals(2., resolved.getSize(), 0);
                        resolved.close();
                        return Promise.resolve(session);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testIntermediatesAreReleased() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_000);
                    return session.getTable("source", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    // The update is never awaited; only the sort is
                    JsPendingTable<JsResolvedTable> last =
                            table.operations().update(cols("Z = X + Y")).sort(sorts("Z"));
                    return last.resolve().then(resolved -> delay(200).then(ignore -> {
                        // The intermediate has been released, the awaited result is still held
                        assertEquals(1, batchStates(connection));
                        resolved.close();
                        return delay(200);
                    })).then(ignore -> {
                        assertEquals(0, batchStates(connection));
                        table.close();
                        return Promise.resolve(table);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testViewportOnResolvedTable() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_000);
                    return session.getTable("source", null).sort(sorts("X")).resolve();
                })
                .then(resolved -> {
                    DataOptions.ViewportSubscriptionOptions opts = new DataOptions.ViewportSubscriptionOptions();
                    opts.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 4));
                    opts.columns = Js.cast(resolved.getColumns());
                    TableViewportSubscription sub = resolved.createViewportSubscription(opts);
                    return assertUpdateReceived(sub, viewport -> assertEquals(5, viewport.getRows().length), 5000)
                            .then(ignore -> {
                                sub.close();
                                resolved.close();
                                return Promise.resolve(resolved);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
