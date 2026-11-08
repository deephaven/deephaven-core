//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.dom.AbortController;
import elemental2.dom.DomGlobal;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.filter.FilterValue;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import io.deephaven.web.client.ide.IdeConnection;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.List;


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
        return promise.then(ignore -> Promise.reject("Expected the promise to reject with \"" + fragment + "\""),
                err -> {
                    String message = String.valueOf(err);
                    assertTrue(message, message.contains(fragment));
                    return Promise.resolve((Object) null);
                });
    }

    private static Object withSignal(AbortController controller) {
        return JsPropertyMap.of("signal", controller.signal);
    }

    public void testChainBeforeAwait() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_001);
                    JsPendingTable<JsTable> source = session.getTable("source", null);
                    // Two operations chained before anything has been awaited, one await for the lot
                    return source.update(arr("Z = X + Y")).sort(arr("Z")).resolve();
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

    public void testViewResolveIsAResolvedTable() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_026);
                    return Promise.resolve(Js.<IThenable<JsTable>>uncheckedCast(session.getTable("source", null)));
                })
                .then(table -> table.operations().resolve().then(resolved -> {
                    assertTrue("operations().resolve() gives a ResolvedTable, got " + resolved,
                            resolved instanceof JsResolvedTable);
                    // Its own owner: closing it leaves the table open
                    resolved.close();
                    assertFalse(table.isClosed());
                    table.close();
                    return Promise.resolve(table);
                }))
                .then(this::finish).catch_(this::report);
    }

    public void testFetchedResolveIsAResolvedTable() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_027);
                    JsPendingTable<JsTable> pending = session.getTable("source", null);
                    return pending.resolve().then(resolved -> {
                        assertTrue("resolve() gives a ResolvedTable, got " + resolved,
                                resolved instanceof JsResolvedTable);
                        assertEquals(5., resolved.getSize(), 0);
                        // cancel() after delivery closes what the pending produced
                        pending.cancel();
                        try {
                            ((JsResolvedTable) resolved).head(Js.uncheckedCast(1.0));
                            fail("Expected the resolved table to be closed by cancel()");
                        } catch (IllegalStateException expected) {
                            assertTrue(expected.getMessage(), expected.getMessage().contains("closed"));
                        }
                        return Promise.resolve(session);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testFetchedPendingHasOneOwner() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_028);
                    // Awaited first: it produces a Table, and resolve() is then refused
                    JsPendingTable<JsTable> awaitedFirst = session.getTable("source", null);
                    Promise<JsTable> table = Promise.resolve(Js.<IThenable<JsTable>>uncheckedCast(
                            awaitedFirst.then(t -> Promise.resolve(t))));
                    // Awaiting again is fine, and gives the same table
                    Promise<JsTable> again = Promise.resolve(Js.<IThenable<JsTable>>uncheckedCast(
                            awaitedFirst.then(t -> Promise.resolve(t))));
                    Promise<Object> resolveRefused = expectRejection(awaitedFirst.resolve(), "already awaited");

                    // Resolved first: it produces a ResolvedTable, and awaiting is then refused
                    JsPendingTable<JsTable> resolvedFirst = session.getTable("source", null);
                    Promise<?> resolved = resolvedFirst.resolve();
                    Promise<Object> awaitRefused = expectRejection(
                            Promise.resolve(Js.<IThenable<Object>>uncheckedCast(resolvedFirst)), "already resolved");

                    return resolveRefused
                            .then(ignore -> awaitRefused)
                            .then(ignore -> table)
                            .then(t -> again.then(t2 -> {
                                // Generics are unchecked at runtime, so check what arrived
                                assertTrue("awaiting gives a Table, got " + t, (Object) t instanceof JsTable);
                                assertSame(t, t2);
                                t.close();
                                return Js.<Promise<Object>>uncheckedCast(resolved);
                            }))
                            .then(r -> {
                                assertTrue("resolve() gives a ResolvedTable, got " + r, r instanceof JsResolvedTable);
                                ((JsResolvedTable) r).close();
                                return Promise.resolve(session);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testOperandFromAnotherConnectionThrows() {
        connect(tables).then(first -> connect(tables).then(second -> {
            delayTestFinish(10_029);
            JsPendingTable<JsTable> left = first.getTable("source", null);
            JsPendingTable<JsTable> right = second.getTable("source", null);
            try {
                left.naturalJoin(JsTableReference.of(right), arr("X"), null, null);
                fail("Expected an operand from another connection to throw");
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("connection"));
            }
            return Promise.resolve(first);
        })).then(this::finish).catch_(this::report);
    }

    public void testOperandClosedAfterChainingIsKept() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_030);
                    return session.getTable("source", null).resolve();
                })
                // The operand needs a state of its own, which only it retains; a copy would share the source's
                .then(source -> source.update(arr("Y2 = Y * 3")).resolve().then(right -> {
                    JsPendingTable<JsResolvedTable> joined = source.naturalJoin(JsTableReference.of(right),
                            arr("X"), arr("Y2"), null);
                    // Closed in the same task, before the batch that reads it has been sent
                    right.close();
                    Promise<JsResolvedTable> result = joined.resolve().then(Promise::resolve,
                            err -> Promise.reject("Join failed after its operand was closed: " + err));
                    Promise<JsResolvedTable> timeout = delay(5_000).then(ignore -> Promise.reject(
                            "Join did not settle within 5s after its operand was closed"));
                    return Promise.race(result, timeout).then(t -> {
                        assertEquals(5., t.getSize(), 0);
                        t.close();
                        source.close();
                        return Promise.resolve(t);
                    });
                }))
                .then(this::finish).catch_(this::report);
    }

    public void testCancelFetchedFailsWhatWasChained() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_031);
                    JsPendingTable<JsTable> source = session.getTable("source", null);
                    Promise<JsResolvedTable> chained = source.update(arr("Z = X + 1")).resolve();
                    // Chained before the cancel, so it fails with the source, as on a canceled operation
                    source.cancel();
                    return expectRejection(chained, "Source table failed: Operation canceled");
                })
                .then(this::finish).catch_(this::report);
    }

    public void testOperationsOnClosedTableThrows() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_034);
                    return session.getTable("source", null).asPromise().then(table -> {
                        table.close();
                        try {
                            table.operations();
                        } catch (IllegalStateException expected) {
                            return Promise.resolve(session);
                        }
                        return Promise.reject("operations() on a closed table returned instead of throwing");
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testOperationsArePinnedWhenTaken() {
        // A filter applied to the table afterwards does not move a view taken before it
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_035);
                    return session.getTable("source", null).asPromise().then(table -> {
                        JsTableOperations before = table.operations();
                        table.applyFilter(new FilterCondition[] {
                                table.findColumn("X").filter().greaterThan(FilterValue.ofNumber(2.0))});
                        // Both resolved in this task, the only one the views may be used in
                        Promise<JsResolvedTable> unfiltered = before.resolve();
                        Promise<JsResolvedTable> filtered = table.operations().resolve();
                        return unfiltered.then(u -> filtered.then(f -> {
                            double unfilteredSize = u.getSize();
                            double filteredSize = f.getSize();
                            u.close();
                            f.close();
                            table.close();
                            assertEquals("the filter was not applied", 2., filteredSize, 0);
                            assertEquals("a view taken before the filter followed it", 5., unfilteredSize, 0);
                            return Promise.resolve(session);
                        }));
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testOperationsHoldTheTableForTheirTask() {
        // Closing the table at once does not take its state from under the view; once the task is over, the view
        // holds nothing
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_037);
                    return session.getTable("source", null).asPromise().then(table -> {
                        ClientTableState state = table.state();
                        JsTableOperations view = table.operations();
                        table.close();
                        // A few microtasks on, still in this task: the close has been flushed by then
                        return Promise.resolve((Object) null)
                                .then(ignore -> Promise.resolve((Object) null))
                                .then(ignore -> Promise.resolve((Object) null))
                                .then(ignore -> view.resolve().then(Promise::resolve, err -> Promise.reject(
                                        "the view lost the table's state within its task: " + err)))
                                .then(resolved -> {
                                    assertEquals(5., resolved.getSize(), 0);
                                    resolved.close();
                                    return delay(200);
                                }).then(ignore -> {
                                    assertEquals("the view still held the table's state after its task",
                                            ClientTableState.ResolutionState.RELEASED, state.getResolution());
                                    return Promise.resolve(session);
                                });
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testAwaitingOperationsGivesATable() {
        // A table's operations are a pending table: awaiting them gives a table the caller owns
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_038);
                    return session.getTable("source", null).asPromise().then(table -> {
                        JsTableOperations view = table.operations();
                        return Promise.resolve(Js.<IThenable<Object>>uncheckedCast(view)).then(result -> {
                            if (result == view) {
                                return Promise.reject("awaiting operations() gave the view back instead of a table");
                            }
                            JsResolvedTable resolved = Js.uncheckedCast(result);
                            assertEquals(5., resolved.getSize(), 0);
                            resolved.close();
                            table.close();
                            return Promise.resolve(session);
                        });
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testSeveralUsesOfOnePendingBeforeItResolves() {
        // Each use registers on the pending's state, which is not running yet
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_044);
                    List<Promise<?>> uses = new ArrayList<>();
                    try {
                        JsPendingTable<JsTable> fetch = session.getTable("source", null);
                        uses.add(fetch.head(Js.uncheckedCast(1.0)).resolve());
                        uses.add(fetch.tail(Js.uncheckedCast(1.0)).resolve());
                        JsPendingTable<JsResolvedTable> operation = fetch.update(arr("Z = X + Y"));
                        uses.add(operation.head(Js.uncheckedCast(1.0)).resolve());
                        uses.add(operation.tail(Js.uncheckedCast(1.0)).resolve());
                        uses.add(session.mergeTables(new JsTableReference[] {Js.uncheckedCast(operation),
                                Js.uncheckedCast(operation)}).resolve());
                        String id = "several-uses-" + DomGlobal.performance.now();
                        uses.add(session.shareObject(Js.uncheckedCast(operation), Js.uncheckedCast(id + "-a")));
                        uses.add(session.shareObject(Js.uncheckedCast(operation), Js.uncheckedCast(id + "-b")));
                    } catch (Throwable e) {
                        return Promise.reject("using one pending several times before it resolved threw: " + e);
                    }
                    return settleAll(uses, 0).then(ignore -> Promise.resolve(session));
                })
                .then(this::finish).catch_(this::report);
    }

    /** Waits for each use in turn, closing the tables they produce. */
    private static Promise<Object> settleAll(List<Promise<?>> uses, int index) {
        if (index == uses.size()) {
            return Promise.resolve((Object) null);
        }
        return uses.get(index).then(result -> {
            if (result instanceof JsResolvedTable) {
                ((JsResolvedTable) result).close();
            }
            return settleAll(uses, index + 1);
        });
    }

    public void testCreateMutableTableReadsTheSameData() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_041);
                    return session.getTable("source", null).update(arr("Z = X + Y")).resolve();
                })
                .then(resolved -> {
                    JsTable table = resolved.createMutableTable();
                    assertEquals(5., table.getSize(), 0);
                    Column z = table.findColumn("Z");
                    DataOptions.SnapshotOptions opts = new DataOptions.SnapshotOptions();
                    opts.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 4));
                    opts.columns = Js.cast(table.getColumns());
                    return table.createSnapshot(opts).then(data -> {
                        // Z = X + Y = i + 2i
                        assertEquals(6, data.getData(2, z).asInt());
                        table.close();
                        resolved.close();
                        return Promise.resolve(resolved);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testMutableTableFiltersOnItsOwn() {
        // The Table is mutable; the ResolvedTable it came from is not, and does not see the filter
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_042);
                    return session.getTable("source", null).resolve();
                })
                .then(resolved -> {
                    JsTable table = resolved.createMutableTable();
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("X").filter().greaterThan(FilterValue.ofNumber(2.0))});
                    return waitForEvent(table, JsTable.EVENT_SIZECHANGED,
                            e -> assertEquals(2., table.getSize(), 0), 5_042)
                            .then(filtered -> {
                                assertEquals("filtering the mutable table changed the resolved table", 5.,
                                        resolved.getSize(), 0);
                                return resolved.head(Js.uncheckedCast(10.0)).resolve();
                            })
                            .then(head -> {
                                assertEquals(5., head.getSize(), 0);
                                head.close();
                                table.close();
                                resolved.close();
                                return Promise.resolve(head);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testClosingEitherLeavesTheOther() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_043);
                    return session.getTable("source", null).resolve();
                })
                .then(resolved -> {
                    // The ResolvedTable closed first: once its release has been flushed, the Table it made still holds
                    // the state and reads data through it
                    JsTable table = resolved.createMutableTable();
                    ClientTableState state = table.state();
                    resolved.close();
                    return afterReleaseFlush().then(ignore -> {
                        assertFalse("the table did not hold the state once the resolved table was closed",
                                state.getResolution() == ClientTableState.ResolutionState.RELEASED);
                        DataOptions.SnapshotOptions opts = new DataOptions.SnapshotOptions();
                        opts.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 4));
                        opts.columns = Js.cast(table.getColumns());
                        return table.createSnapshot(opts);
                    }).then(data -> {
                        assertEquals(5, data.getRows().length);
                        return table.operations().resolve();
                    }).then(fromTable -> {
                        // The Table closed first: the ResolvedTable it came from still works
                        JsTable other = fromTable.createMutableTable();
                        other.close();
                        return fromTable.head(Js.uncheckedCast(1.0)).resolve().then(head -> {
                            assertEquals("the resolved table did not outlive a table made from it", 1.,
                                    head.getSize(), 0);
                            head.close();
                            fromTable.close();
                            table.close();
                            return Promise.resolve(head);
                        });
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCopyIsOfTheTableWhenCalled() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_032);
                    return session.getTable("source", null).asPromise();
                })
                .then(table -> {
                    Promise<JsTable> pending = table.copy();
                    // Filtered after copy() but before the copy is awaited: the copy must not see it
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("X").filter().greaterThan(FilterValue.ofNumber(2.0))});
                    return pending.then(copy -> {
                        assertEquals("filters on a copy of the unfiltered table", 0, copy.getFilter().length);
                        copy.close();
                        table.close();
                        return Promise.resolve(copy);
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testIntermediatesAreReleased() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_003);
                    return session.getTable("source", null).asPromise();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    // The update is never awaited; only the sort is
                    JsPendingTable<JsResolvedTable> last =
                            table.operations().update(arr("Z = X + Y")).sort(arr("Z"));
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
                    return session.getTable("source", null).sort(arr("X")).resolve();
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
                    return session.getTable("ticking", null).update(arr("Z = X * 2")).resolve();
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
                    return session.getTable("ticking", null).update(arr("Z = X * 2")).resolve();
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
                    return session.getTable("source", null).asPromise();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    JsPendingTable<JsResolvedTable> pending = table.operations().update(arr("Z = X + Y"));
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
                    return session.getTable("slowsource", null).asPromise();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    JsPendingTable<JsResolvedTable> pending = table.operations().update(arr(SLOW));
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
                    return session.getTable("slowsource", null).asPromise();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    JsPendingTable<JsResolvedTable> pending = table.operations().update(arr(SLOW));
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
                    return session.getTable("slowsource", null).asPromise();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    // Both go out in one batch, on one stream
                    JsPendingTable<JsResolvedTable> slow = table.operations().update(arr(SLOW));
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
                    return session.getTable("source", null).asPromise();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    AbortController controller = new AbortController();
                    JsPendingTable<JsResolvedTable> pending = table.operations().update(arr("Z = X + Y"));
                    Promise<JsResolvedTable> resolved = pending.resolve(withSignal(controller));
                    controller.abort();
                    // As for cancel(), chaining on it fails through the result
                    JsPendingTable<JsResolvedTable> chained = pending.sort(arr("Z"));
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
                    return session.getTable("slowsource", null).asPromise();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    AbortController controller = new AbortController();
                    Promise<JsResolvedTable> resolved =
                            table.operations().update(arr(SLOW)).resolve(withSignal(controller));
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
                    return session.getTable("source", null).asPromise();
                })
                .then(table -> {
                    WorkerConnection connection = table.getConnection();
                    AbortController controller = new AbortController();
                    controller.abort();
                    Promise<JsResolvedTable> resolved =
                            table.operations().update(arr("Z = X + Y")).resolve(withSignal(controller));
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

    public void testOperationsResolveWithAbortedSignal() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_036);
                    return session.getTable("source", null).asPromise();
                })
                .then(table -> {
                    AbortController controller = new AbortController();
                    controller.abort();
                    ClientTableState state = table.state();
                    return expectRejection(table.operations().resolve(withSignal(controller)), "Abort")
                            .then(ignore -> {
                                // Nothing was handed out, so closing the table releases its state
                                table.close();
                                return delay(200);
                            })
                            .then(ignore -> {
                                assertEquals("state still held after an aborted resolve",
                                        ClientTableState.ResolutionState.RELEASED, state.getResolution());
                                return Promise.resolve(table);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testAbortAfterDelivery() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_014);
                    return session.getTable("source", null).asPromise();
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

    /** Rejects naming {@code what} unless {@code dependent} rejects because its source was canceled. */
    private static Promise<Object> expectSourceCanceled(String what, Promise<?> dependent) {
        return dependent.then(
                ignore -> Promise.reject(what + " resolved; expected it to fail with its canceled source"),
                err -> String.valueOf(err).contains("Source table failed: Operation canceled")
                        ? Promise.resolve((Object) null)
                        : Promise.reject(what + " rejected for another reason: " + err));
    }

    public void testCancelAfterDeliveryFailsUnsettledDependents() {
        // A pending that has produced its table is canceled while a slow operation chained on it is still running:
        // the table is gone, for what was chained on it too
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_039);
                    return session.getTable("slowsource", null).asPromise().then(slow -> {
                        JsPendingTable<JsResolvedTable> operation = slow.operations().head(Js.uncheckedCast(1.0));
                        Promise<JsResolvedTable> dependentOfOperation = operation.update(arr(SLOW)).resolve();
                        return operation.resolve().then(delivered -> {
                            operation.cancel();
                            return expectSourceCanceled("a dependent of a delivered operation",
                                    dependentOfOperation);
                        }).then(ignore -> {
                            JsPendingTable<JsTable> fetch = session.getTable("slowsource", null);
                            Promise<JsResolvedTable> dependentOfFetch = fetch.update(arr(SLOW)).resolve();
                            return fetch.asPromise().then(delivered -> {
                                fetch.cancel();
                                return expectSourceCanceled("a dependent of a delivered fetch", dependentOfFetch);
                            });
                        }).then(ignore -> {
                            slow.close();
                            return abandonedWorkDone(session);
                        });
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCancelingAViewLeavesItsTable() {
        // Canceling a table's operations ends what came from them, but the table itself stays open and usable
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_040);
                    return session.getTable("slowsource", null).asPromise().then(table -> {
                        ClientTableState state = table.state();
                        JsPendingTable<JsResolvedTable> view = table.operations();
                        // Still running on the server when the view is canceled
                        Promise<JsResolvedTable> chained = view.update(arr(SLOW)).resolve();
                        return view.resolve().then(owned -> {
                            view.cancel();
                            try {
                                owned.head(Js.uncheckedCast(1.0));
                                return Promise.reject("the table the view handed out was still open after cancel");
                            } catch (IllegalStateException expected) {
                                // closed, as for a canceled operation
                            }
                            return expectSourceCanceled("an operation chained on the canceled view", chained);
                        }).then(ignore -> {
                            assertEquals("canceling the view failed its table's state",
                                    ClientTableState.ResolutionState.RUNNING, state.getResolution());
                            return table.operations().resolve();
                        }).then(again -> {
                            assertEquals(1., again.getSize(), 0);
                            again.close();
                            table.close();
                            return abandonedWorkDone(session);
                        });
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCancelAfterDeliveryClosesTable() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_015);
                    return session.getTable("source", null).asPromise();
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
                    Promise<JsResolvedTable> fetched = pending.resolve(withSignal(controller));
                    controller.abort();
                    // The fetch itself is not withdrawn, but the state it fetches is released
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
                    return session.getTable("source", null).asPromise();
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
                    return session.getTable("source", null).asPromise().then(source -> {
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
        options.yColumns = arr("Y");
        options.width = 10;
        options.xRange = xRange.length == 0 ? null : arr((Object[]) xRange);
        return options;
    }

    public void testDownsample() {
        connect(seriesTables)
                .then(session -> {
                    delayTestFinish(10_023);
                    return session.getTable("series", null).asPromise();
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
                    return session.getTable("source", null).asPromise();
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
                            .then(ignore -> table.operations().update(arr("S = `v` + X")).resolve())
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
                    return session.getTable("source", null).asPromise();
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
