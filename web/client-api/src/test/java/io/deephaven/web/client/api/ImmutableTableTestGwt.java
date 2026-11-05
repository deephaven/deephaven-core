//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.ReadonlyArray;
import elemental2.dom.AbortController;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import io.deephaven.web.client.ide.IdeConnection;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;


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
            // S costs a second to evaluate, so an operation that reads it is slow on the server
            .script("slowsource", "empty_table(1).update_view([\"X = i\", \"S = slow(X)\"])");

    /**
     * One row a second from SERIES_START, for the downsample test only. Any width rounds up to at least 500 bins of up
     * to four rows each, so the series needs well over 2000 rows to shrink.
     */
    private final TableSourceBuilder seriesTables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("series", "empty_table(10_000).update([\"Timestamp = '2024-01-01T00:00:00 UTC' + i * SECOND\","
                    + " \"Y = i % 100\"])");

    /** 2024-01-01T00:00:00Z in nanoseconds, the first timestamp of the series table. */
    private static final long SERIES_START = 1_704_067_200_000_000_000L;
    private static final long SECOND_NANOS = 1_000_000_000L;

    private static final String SLOW = "Z = S + 1";

    /**
     * A canceled slow operation keeps running on the server; wait it out so the next test's fixture script is not held
     * up behind it, then pass {@code value} on.
     */
    private static <T> Promise<T> abandonedWorkDone(T value) {
        return delay(1_300).then(ignore -> Promise.resolve(value));
    }

    /** Resolves once {@code promise} has rejected with the cancellation message, fails otherwise. */
    private static Promise<Object> expectCanceled(Promise<?> promise) {
        return expectRejection(promise, "Operation canceled");
    }

    /** Resolves once {@code promise} has rejected with a message containing {@code fragment}, fails otherwise. */
    private static Promise<Object> expectRejection(Promise<?> promise, String fragment) {
        return promise.then(ignore -> Promise.reject("Expected the promise to reject"), err -> {
            String message = String.valueOf(err);
            assertTrue(message, message.contains(fragment));
            return Promise.resolve((Object) null);
        });
    }

    private static Object withSignal(AbortController controller) {
        return JsPropertyMap.of("signal", controller.signal);
    }

    private static ReadonlyArray<JsTable.CustomColumnArgUnionType> cols(String... exprs) {
        return Js.uncheckedCast(JsArray.of(exprs));
    }

    private static ReadonlyArray<Sort.SortUnion> sorts(String... columns) {
        return Js.uncheckedCast(JsArray.of(columns));
    }

    public void testChainBeforeAwait() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_001);
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
                    delayTestFinish(10_002);
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
                    delayTestFinish(10_003);
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
                    delayTestFinish(10_004);
                    return session.getTable("source", null).sort(sorts("X")).resolve();
                })
                .then(resolved -> {
                    DataOptions.ViewportSubscriptionOptions opts = new DataOptions.ViewportSubscriptionOptions();
                    opts.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 4));
                    opts.columns = Js.cast(resolved.getColumns());
                    TableViewportSubscription sub = resolved.createViewportSubscription(opts);
                    return assertUpdateReceived(sub, viewport -> assertEquals(5, viewport.getRows().length), 5004)
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
                    delayTestFinish(20_005);
                    return session.getTable("ticking", null).update(cols("Z = X * 2")).resolve();
                })
                .then(resolved -> {
                    DataOptions.ViewportSubscriptionOptions opts = new DataOptions.ViewportSubscriptionOptions();
                    opts.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 0));
                    opts.columns = Js.cast(resolved.getColumns());
                    TableViewportSubscription sub = resolved.createViewportSubscription(opts);
                    WorkerConnection connection = resolved.getConnection();
                    return assertUpdateReceived(sub, viewport -> assertEquals(1, viewport.getRows().length), 5005)
                            .then(ignore -> {
                                // Same session, new transport: the batch-created state must be revived like any other
                                connection.forceReconnect();
                                return connection.whenServerReady("reconnect in test");
                            })
                            .then(ignore -> assertUpdateReceived(sub,
                                    viewport -> assertEquals(1, viewport.getRows().length), 5006))
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

    public void testSubscriptionResumesAfterTransportCut() {
        CuttingTransportFactory transport = new CuttingTransportFactory();
        ConnectOptions options = new ConnectOptions();
        options.transportFactory = transport;
        connect(tables, options)
                .then(session -> {
                    delayTestFinish(20_023);
                    return session.getTable("ticking", null).update(cols("Z = X * 2")).resolve();
                })
                .then(resolved -> {
                    DataOptions.ViewportSubscriptionOptions opts = new DataOptions.ViewportSubscriptionOptions();
                    opts.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 0));
                    opts.columns = Js.cast(resolved.getColumns());
                    TableViewportSubscription sub = resolved.createViewportSubscription(opts);
                    IdeConnection ide = lastIdeConnection;
                    // An unhandled disconnect event is logged as an error, which the harness treats as a failure
                    ide.addEventListener(CoreClient.EVENT_DISCONNECT, e -> {
                    });
                    return assertUpdateReceived(sub, viewport -> assertEquals(1, viewport.getRows().length), 5007)
                            .then(ignore -> {
                                // Every open stream dies, the subscription's Barrage stream included: the connection
                                // sees a transport error, reconnects within the same session, and the reviver must
                                // bring back a subscription that no JsTable owns
                                transport.cutAll();
                                return waitForEvent(ide, CoreClient.EVENT_RECONNECT, 5009).onInvoke(ide);
                            })
                            .then(ignore -> Promise.race(
                                    assertUpdateReceived(sub,
                                            viewport -> assertEquals(1, viewport.getRows().length), 8008),
                                    delay(8000).then(late -> Promise.reject(
                                            "subscription owned by a ResolvedTable did not resume after the transport was cut"))))
                            // Close on failure too, or a broken stream keeps logging errors into the next tests
                            .then(ignore -> {
                                sub.close();
                                resolved.close();
                                return Promise.resolve(resolved);
                            }, err -> {
                                sub.close();
                                resolved.close();
                                return Promise.reject(err);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCancelBeforeSend() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_007);
                    return session.getTable("source", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    JsPendingTable<JsResolvedTable> pending = table.operations().update(cols("Z = X + Y"));
                    pending.cancel();
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
                    delayTestFinish(15_008);
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
                                return abandonedWorkDone(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCancelSentButNotAwaited() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_009);
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
                                return abandonedWorkDone(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCancelOneOperationOfABatch() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_010);
                    return session.getTable("slowsource", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    // Both go out in one batch, on one stream
                    JsPendingTable<JsResolvedTable> slow = table.operations().update(cols(SLOW));
                    JsPendingTable<JsResolvedTable> quick = table.operations().head(Js.uncheckedCast(1.0));
                    Promise<JsResolvedTable> quickResult = quick.resolve();
                    return delay(0)
                            .then(ignore -> {
                                // Canceling one must not cut off the other's response
                                slow.cancel();
                                return quickResult;
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
                                return abandonedWorkDone(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testAbortBeforeSend() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_011);
                    return session.getTable("source", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    AbortController controller = new AbortController();
                    JsPendingTable<JsResolvedTable> pending = table.operations().update(cols("Z = X + Y"));
                    Promise<JsResolvedTable> resolved = pending.resolve(withSignal(controller));
                    controller.abort();
                    // As for cancel(), chaining on it fails through the result
                    JsPendingTable<JsResolvedTable> chained = pending.sort(sorts("Z"));
                    return expectRejection(resolved, "Abort")
                            .then(ignore -> expectRejection(chained.resolve(), "Abort"))
                            .then(ignore -> delay(200))
                            .then(ignore -> {
                                assertEquals(0, batchStates(connection));
                                table.close();
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testAbortInFlight() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_012);
                    return session.getTable("slowsource", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    AbortController controller = new AbortController();
                    Promise<JsResolvedTable> resolved =
                            table.operations().update(cols(SLOW)).resolve(withSignal(controller));
                    return delay(0)
                            .then(ignore -> {
                                controller.abort();
                                return expectRejection(resolved, "Abort");
                            })
                            .then(ignore -> delay(200))
                            .then(ignore -> {
                                assertEquals(0, batchStates(connection));
                                table.close();
                                return abandonedWorkDone(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testAlreadyAbortedSignal() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_013);
                    return session.getTable("source", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    AbortController controller = new AbortController();
                    controller.abort();
                    Promise<JsResolvedTable> resolved =
                            table.operations().update(cols("Z = X + Y")).resolve(withSignal(controller));
                    return expectRejection(resolved, "Abort")
                            .then(ignore -> delay(200))
                            .then(ignore -> {
                                assertEquals(0, batchStates(connection));
                                table.close();
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testAbortAfterDelivery() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_014);
                    return session.getTable("source", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    AbortController controller = new AbortController();
                    return table.operations().head(Js.uncheckedCast(2.0)).resolve(withSignal(controller))
                            .then(head -> {
                                // Too late to cancel: the caller owns the table and closes it
                                controller.abort();
                                assertEquals(2., head.getSize(), 0);
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

    public void testCancelAfterDeliveryClosesTable() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_015);
                    return session.getTable("source", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    JsPendingTable<JsResolvedTable> pending = table.operations().head(Js.uncheckedCast(2.0));
                    return pending.resolve()
                            .then(head -> {
                                assertEquals(2., head.getSize(), 0);
                                // Cancel after delivery releases the delivered table
                                pending.cancel();
                                try {
                                    head.createSnapshot(new DataOptions.SnapshotOptions());
                                    fail("Expected the delivered table to be closed");
                                } catch (IllegalStateException expected) {
                                    // closed by the cancel
                                }
                                // The caller's own close is expected and harmless
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

    public void testCancelAfterDeliveryClosesFetchedTable() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_016);
                    JsPendingTable<JsTable> pending = session.getTable("source", null);
                    return pending.resolve()
                            .then(table -> {
                                pending.cancel();
                                return delay(200);
                            })
                            .then(ignore -> {
                                TableTicket ticket = new TableTicket(pending.typedTicket().getTicket());
                                pending.getConnection().getCache().get(ticket).ifPresent(state -> assertEquals(
                                        ClientTableState.ResolutionState.RELEASED, state.getResolution()));
                                return Promise.resolve(session);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testAbortEntryPointFetch() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_017);
                    AbortController controller = new AbortController();
                    JsPendingTable<JsTable> pending = session.getTable("source", null);
                    Promise<JsTable> fetched = pending.resolve(withSignal(controller));
                    controller.abort();
                    // The fetch itself is not withdrawn, but the table it produces is closed on arrival
                    return expectRejection(fetched, "Abort")
                            .then(ignore -> delay(500))
                            .then(ignore -> {
                                TableTicket ticket = new TableTicket(pending.typedTicket().getTicket());
                                pending.getConnection().getCache().get(ticket).ifPresent(state -> assertEquals(
                                        ClientTableState.ResolutionState.RELEASED, state.getResolution()));
                                return Promise.resolve(session);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testDependentOfFailedFetchFails() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_018);
                    return session.getTable("source", null).resolve();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    // Neither source is awaited itself; only the operation chained on each is. The fetch fails, so
                    // the dependent must fail too rather than wait on the server for a source that never arrives.
                    // A definition lookup that fails (what getTable(name) does after waiting ten seconds for the
                    // variable, shortcut here) fails before anything is sent, so only the client can fail the
                    // dependent; a definition the server cannot resolve fails once sent, and the server fails the
                    // dependent as well, so either message is acceptable there.
                    JsPendingTable<JsTable> unsendable = connection.pendingTable(connection.fetchTableState(
                            Promise.reject("no such table"), null, connection.getTickets().newTableTicket()));
                    JsVariableDefinition unresolvable = new JsVariableDefinition(JsVariableType.TABLE, "missing",
                            DomGlobal.btoa("s/missing"), null);
                    JsPendingTable<JsResolvedTable> ofUnsendable = unsendable.head(Js.uncheckedCast(1.0));
                    JsPendingTable<JsResolvedTable> ofUnresolvable =
                            connection.getObject(unresolvable, null).asPendingTable().head(Js.uncheckedCast(1.0));
                    return expectRejection(ofUnsendable.resolve(), "Source table failed")
                            .then(ignore -> expectRejection(ofUnresolvable.resolve(), "missing"))
                            .then(ignore -> delay(200))
                            .then(ignore -> {
                                assertEquals(0, batchStates(connection));
                                table.close();
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    /** The state a pending table's ticket occupies, if the cache still has one that is not released. */
    private static boolean isHeld(JsPendingTable<?> pending) {
        TableTicket ticket = new TableTicket(pending.typedTicket().getTicket());
        return pending.getConnection().getCache().get(ticket)
                .map(state -> state.getResolution() != ClientTableState.ResolutionState.RELEASED)
                .orElse(false);
    }

    public void testFetchedTableOnlyChainedOnIsReleased() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_019);
                    // The fetch is never awaited; only an operation on it is. Once that operation is done with it,
                    // nothing should hold the fetched table.
                    JsPendingTable<JsTable> source = session.getTable("source", null);
                    JsPendingTable<JsResolvedTable> head = source.head(Js.uncheckedCast(1.0));
                    return head.resolve()
                            .then(table -> {
                                assertEquals(1., table.getSize(), 0);
                                table.close();
                                return delay(200);
                            })
                            .then(ignore -> {
                                assertFalse("fetched table still held", isHeld(source));
                                return Promise.resolve(session);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testFetchedTableNeverUsedIsReleased() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_020);
                    // Neither awaited nor chained on: released as soon as the fetch completes
                    JsPendingTable<JsTable> source = session.getTable("source", null);
                    return delay(500)
                            .then(ignore -> {
                                assertFalse("fetched table still held", isHeld(source));
                                return Promise.resolve(session);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testNewTableOnlyChainedOnIsReleased() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_021);
                    // A client-built table that is only chained on holds nothing once its dependent is done
                    JsPendingTable<JsTable> created = session.newTable(new String[] {"X"}, new String[] {"int"},
                            new String[][] {{"1", "2", "3"}}, "UTC");
                    JsPendingTable<JsResolvedTable> head = created.head(Js.uncheckedCast(1.0));
                    return head.resolve()
                            .then(table -> {
                                assertEquals(1., table.getSize(), 0);
                                table.close();
                                return delay(200);
                            })
                            .then(ignore -> {
                                assertFalse("created table still held", isHeld(created));
                                return Promise.resolve(session);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testMergedTableOnlyChainedOnIsReleased() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_022);
                    return session.getTable("source", null).resolve().then(source -> {
                        // A merge that is only chained on holds nothing once its dependent is done. Through the
                        // session, as a caller would, not the connection: the session layer is what is under test.
                        JsPendingTable<JsTable> merged = session.mergeTables(
                                new JsTableReference[] {JsTableReference.of(source), JsTableReference.of(source)});
                        JsPendingTable<JsResolvedTable> head = merged.head(Js.uncheckedCast(7.0));
                        return head.resolve()
                                .then(table -> {
                                    assertEquals(7., table.getSize(), 0);
                                    table.close();
                                    return delay(200);
                                })
                                .then(ignore -> {
                                    assertFalse("merged table still held", isHeld(merged));
                                    source.close();
                                    return Promise.resolve(session);
                                });
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    private static JsTableOperations.DownsampleTableOptions downsampleOptions(LongWrapper... xRange) {
        JsTableOperations.DownsampleTableOptions options = new JsTableOperations.DownsampleTableOptions();
        options.xColumn = Js.uncheckedCast("Timestamp");
        options.yColumns = Js.uncheckedCast(JsArray.of("Y"));
        options.width = 10;
        options.xRange = xRange.length == 0 ? null : Js.uncheckedCast(JsArray.of(xRange));
        return options;
    }

    public void testDownsample() {
        connect(seriesTables)
                .then(session -> {
                    delayTestFinish(10_023);
                    return session.getTable("series", null).resolve();
                })
                .then(series -> {
                    // The whole series, and its first tenth in full resolution, in one batch
                    long end = SERIES_START + 1_000 * SECOND_NANOS;
                    JsPendingTable<JsResolvedTable> whole = series.operations().downsample(downsampleOptions());
                    JsPendingTable<JsResolvedTable> zoomed = series.operations()
                            .downsample(downsampleOptions(LongWrapper.of(SERIES_START), LongWrapper.of(end)));
                    // Both awaited in this task; a pending used later would be rejected
                    Promise<JsResolvedTable> wholeResult = whole.resolve();
                    Promise<JsResolvedTable> zoomedResult = zoomed.resolve();
                    return wholeResult.then(all -> zoomedResult.then(part -> {
                        JsResolvedTable[] results = {all, part};
                        for (JsResolvedTable result : results) {
                            assertTrue("downsampled to " + result.getSize() + " of " + series.getSize() + " rows",
                                    result.getSize() > 0 && result.getSize() < series.getSize());
                            assertNotNull(result.findColumn("Timestamp"));
                            assertNotNull(result.findColumn("Y"));
                        }
                        Column timestamp = part.findColumn("Timestamp");
                        DataOptions.SnapshotOptions opts = new DataOptions.SnapshotOptions();
                        opts.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, part.getSize() - 1));
                        opts.columns = Js.cast(JsArray.of(timestamp));
                        return part.createSnapshot(opts).then(data -> {
                            // Full resolution inside the range, a few bounding rows outside it
                            int outside = 0;
                            for (int i = 0; i < data.getRows().length; i++) {
                                long nanos = Js.<LongWrapper>uncheckedCast(data.getData(i, timestamp)).getWrapped();
                                if (nanos < SERIES_START || nanos > end) {
                                    outside++;
                                }
                            }
                            assertTrue(outside + " of " + data.getRows().length + " rows outside the range",
                                    outside * 10 <= data.getRows().length);
                            for (JsResolvedTable result : results) {
                                result.close();
                            }
                            series.close();
                            return Promise.resolve(series);
                        });
                    }));
                })
                .then(this::finish).catch_(this::report);
    }

    /** Asserts that {@code seek} finds the row at {@code expected}, or none for -1. */
    private static Promise<Object> expectRow(String what, Promise<Double> seek, int expected) {
        return seek.then(row -> {
            assertEquals(what, expected, row.intValue());
            return Promise.resolve((Object) null);
        });
    }

    public void testSeekRow() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_025);
                    return session.getTable("source", null).resolve();
                })
                .then(table -> {
                    Column.ColumnOrName s = Column.ColumnOrName.of("S");
                    // A table: X is 0 to 4
                    return expectRow("Table, X = 3", table.seekRow(0, Column.ColumnOrName.of("X"), ValueType.NUMBER,
                            Js.asAny(3.0), null, null, null), 3)
                            .then(ignore -> expectRow("Table, no X = 9", table.seekRow(0,
                                    Column.ColumnOrName.of("X"), ValueType.NUMBER, Js.asAny(9.0), null, null, null),
                                    -1))
                            // A resolved table: S is "v0" to "v4"
                            .then(ignore -> table.operations().update(cols("S = `v` + X")).resolve())
                            .then(resolved -> expectRow("resolved, S = v3",
                                    resolved.seekRow(0, s, ValueType.STRING, Js.asAny("v3"), null, null, null), 3)
                                    .then(ignore -> expectRow("resolved, S = V2 ignoring case", resolved.seekRow(0, s,
                                            ValueType.STRING, Js.asAny("V2"), true, null, null), 2))
                                    .then(ignore -> expectRow("resolved, next S containing v after row 3",
                                            resolved.seekRow(3, s, ValueType.STRING, Js.asAny("v"), null, true, null),
                                            4))
                                    .then(ignore -> expectRow("resolved, S = v1 searching back from row 3",
                                            resolved.seekRow(3, s, ValueType.STRING, Js.asAny("v1"), null, null,
                                                    true),
                                            1))
                                    .then(ignore -> expectRow("resolved, no S = nope", resolved.seekRow(0, s,
                                            ValueType.STRING, Js.asAny("nope"), null, null, null), -1))
                                    .then(ignore -> {
                                        resolved.close();
                                        table.close();
                                        return Promise.resolve((Object) null);
                                    }));
                })
                .then(this::finish).catch_(this::report);
    }

    public void testDownsampleRejectsBadArguments() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_024);
                    return session.getTable("source", null).resolve();
                })
                .then(table -> {
                    // Invalid arguments throw when the method is called, before anything reaches the server, as the
                    // TableOperations docs say
                    JsTableOperations.DownsampleTableOptions noWidth = downsampleOptions();
                    noWidth.width = 0;
                    try {
                        table.operations().downsample(noWidth);
                        fail("Expected a zero width to throw");
                    } catch (IllegalArgumentException expected) {
                        assertTrue(expected.getMessage(), expected.getMessage().contains("width"));
                    }
                    try {
                        table.operations().downsample(downsampleOptions(LongWrapper.of(SERIES_START)));
                        fail("Expected a one-element range to throw");
                    } catch (IllegalArgumentException expected) {
                        assertTrue(expected.getMessage(), expected.getMessage().contains("xRange"));
                    }
                    try {
                        table.operations().downsample(downsampleOptions(LongWrapper.of(SERIES_START), null));
                        fail("Expected a range with no end to throw");
                    } catch (IllegalArgumentException expected) {
                        assertTrue(expected.getMessage(), expected.getMessage().contains("xRange"));
                    }
                    table.close();
                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
