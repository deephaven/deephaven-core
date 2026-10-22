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
            .script("from deephaven import empty_table, time_table")
            .script("source", "empty_table(5).update([\"X = i\", \"Y = i * 2\"])")
            .script("from datetime import datetime, timedelta")
            // One row from the start (the backfilled rows collapse into it), replaced twice a second
            .script("ticking", "time_table(\"PT0.5S\", start_time=datetime.now() - timedelta(seconds=10))"
                    + ".update([\"X = i\", \"Y = 0\"]).last_by(by=\"Y\")")
            .script("import time")
            .script("def slow(x) -> int:\n    time.sleep(1.0)\n    return x")
            // S costs a second per row to evaluate, so an operation that reads it is slow on the server
            .script("slowsource", "empty_table(2).update_view([\"X = i\", \"S = slow(X)\"])");

    private static final String SLOW = "Z = S + 1";

    /** Resolves once {@code promise} has rejected with the cancellation message, fails otherwise. */
    private static Promise<Object> expectCanceled(Promise<?> promise) {
        return promise.then(ignore -> Promise.reject("Expected the promise to reject"), err -> {
            String message = String.valueOf(err);
            assertTrue(message, message.contains("Operation canceled"));
            return Promise.resolve((Object) null);
        });
    }

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
                    Promise<JsResolvedTable> first = pending.resolve();
                    Promise<JsResolvedTable> second = pending.resolve();
                    assertSame(first, second);
                    return first.then(a -> second.then(b -> {
                        assertSame(a, b);
                        assertEquals(2., a.getSize(), 0);
                        a.close();
                        return Promise.resolve(session);
                    }));
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

    public void testSubscriptionSurvivesReconnect() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(20_000);
                    return session.getTable("ticking", null).update(cols("Z = X * 2")).resolve();
                })
                .then(resolved -> {
                    DataOptions.ViewportSubscriptionOptions opts = new DataOptions.ViewportSubscriptionOptions();
                    opts.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 0));
                    opts.columns = Js.cast(resolved.getColumns());
                    TableViewportSubscription sub = resolved.createViewportSubscription(opts);
                    WorkerConnection connection = resolved.getConnection();
                    return assertUpdateReceived(sub, viewport -> assertEquals(1, viewport.getRows().length), 5000)
                            .then(ignore -> {
                                // Same session, new transport: the batch-created state must be revived like any other
                                connection.forceReconnect();
                                return connection.whenServerReady("reconnect in test");
                            })
                            .then(ignore -> assertUpdateReceived(sub,
                                    viewport -> assertEquals(1, viewport.getRows().length), 5000))
                            // The revived ticket is still usable as a source for further operations
                            .then(ignore -> resolved.head(Js.uncheckedCast(1.0)).resolve())
                            .then(head -> {
                                assertEquals(1., head.getSize(), 0);
                                head.close();
                                sub.close();
                                resolved.close();
                                return Promise.resolve(resolved);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCancelBeforeSend() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_000);
                    return session.getTable("source", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    JsPendingTable<JsResolvedTable> pending = table.operations().update(cols("Z = X + Y"));
                    pending.cancel();
                    try {
                        pending.sort(sorts("Z"));
                        fail("Expected chaining on a canceled operation to throw");
                    } catch (IllegalStateException expected) {
                        // nothing was sent for the canceled operation, so nothing may reference its ticket
                    }
                    return expectCanceled(pending.resolve())
                            .then(ignore -> delay(200))
                            .then(ignore -> {
                                assertEquals(0, batchStates(connection));
                                table.close();
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCancelAfterResolve() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_000);
                    return session.getTable("slowsource", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    JsPendingTable<JsResolvedTable> pending = table.operations().update(cols(SLOW));
                    Promise<JsResolvedTable> resolved = pending.resolve();
                    // Let the batch go out, then cancel while the server is busy with it
                    return delay(0)
                            .then(ignore -> {
                                pending.cancel();
                                return expectCanceled(resolved);
                            })
                            // The state failed and was released at once, well before the server would have answered
                            .then(ignore -> delay(200))
                            .then(ignore -> {
                                assertEquals(0, batchStates(connection));
                                table.close();
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCancelSentButNotAwaited() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_000);
                    return session.getTable("slowsource", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    JsPendingTable<JsResolvedTable> pending = table.operations().update(cols(SLOW));
                    return delay(0)
                            .then(ignore -> {
                                pending.cancel();
                                // Awaiting after the cancel rejects the same way
                                return expectCanceled(pending.resolve());
                            })
                            .then(ignore -> delay(200))
                            .then(ignore -> {
                                assertEquals(0, batchStates(connection));
                                table.close();
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCancelOneOperationOfABatch() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_000);
                    return session.getTable("slowsource", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    // Both go out in one batch, on one stream
                    JsPendingTable<JsResolvedTable> slow = table.operations().update(cols(SLOW));
                    JsPendingTable<JsResolvedTable> quick = table.operations().head(Js.uncheckedCast(1.0));
                    return delay(0)
                            .then(ignore -> {
                                // Canceling one must not cut off the other's response
                                slow.cancel();
                                return quick.resolve();
                            })
                            .then(head -> {
                                assertEquals(1., head.getSize(), 0);
                                // Canceling after delivery is a no-op; the table is closed instead
                                quick.cancel();
                                assertEquals(1., head.getSize(), 0);
                                head.close();
                                return delay(200);
                            })
                            .then(ignore -> {
                                assertEquals(0, batchStates(connection));
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
