//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsError;
import elemental2.dom.DomGlobal;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.input.JsInputTable;
import io.deephaven.web.client.ide.IdeSession;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.annotations.JsFunction;
import jsinterop.base.Js;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static elemental2.dom.DomGlobal.console;

/**
 * How pending tables fail when they are used wrongly: awaited, chained on, or passed as an operand after the task that
 * created them, or after being canceled or closed. Each scenario runs against every kind of pending table, since they
 * share one implementation of these rules.
 */
public class PendingTableMisuseTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table, input_table")
            .script("source", "empty_table(5).update([\"X = i\", \"Y = i * 2\"])")
            .script("input", "input_table(init_table=source, key_cols=[\"X\"])");

    /** What a pending used after the task that created it must say, in its rejection and its warning. */
    private static final String LATE = "after the task that created it";

    /** A scenario run once per kind of pending table; {@code kind} names it in failures. */
    private interface PendingScenario {
        Promise<Object> run(String kind, Supplier<JsPendingTable<?>> make);
    }

    /**
     * Runs {@code scenario} against each kind of pending table: an operation, sent in a batch, and an entry-point
     * fetch.
     */
    private static Promise<Object> forEachKind(IdeSession session, JsTable table, PendingScenario scenario) {
        return scenario.run("operation", () -> table.operations().update(arr("Z = X + Y")))
                .then(ignore -> scenario.run("fetch", () -> session.getTable("source", null)));
    }

    /** The resolved source table of the running test, for scenarios that need an operand or a connection. */
    private JsTable source;

    /** Connects, resolves the source table, runs {@code scenario} for each kind, and closes the table. */
    private void runForEachKind(int timeout, PendingScenario scenario) {
        connect(tables)
                .then(session -> {
                    delayTestFinish(timeout);
                    return session.getTable("source", null).asPromise()
                            .then(table -> {
                                source = table;
                                return forEachKind(session, table, scenario);
                            }).then(ignore -> {
                                source.close();
                                return Promise.resolve(session);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    /** Resolves once {@code promise} has rejected with {@code fragment} in its message; fails naming {@code what}. */
    private static Promise<Object> expectRejection(String what, Promise<?> promise, String fragment) {
        return promise.then(
                ignore -> Promise.reject(what + " resolved; expected it to reject with \"" + fragment + "\""),
                err -> {
                    String message = String.valueOf(err);
                    assertTrue(what + " rejected for another reason: " + message, message.contains(fragment));
                    if (!fragment.equals(LATE)) {
                        // A real failure must not be reported as a late use
                        assertFalse(what + " was reported as a late use: " + message, message.contains(LATE));
                    }
                    return Promise.resolve((Object) null);
                });
    }

    /** console.warn as the late-use check calls it: a message and the error marking where the pending was made. */
    @JsFunction
    private interface Warn {
        void write(Object message, Object detail);
    }

    /** One captured warning; {@code stack} is the detail's stack if the detail was an Error, else null. */
    private static final class Warning {
        final String message;
        final String stack;

        Warning(Object message, Object detail) {
            this.message = String.valueOf(message);
            this.stack = detail instanceof JsError ? ((JsError) detail).stack : null;
        }

        @Override
        public String toString() {
            return message;
        }
    }

    /** Runs {@code body} with console.warn captured, then hands the warnings to {@code check}. */
    private static Promise<Object> withWarnings(Supplier<Promise<?>> body, Consumer<List<Warning>> check) {
        Warn original = Js.uncheckedCast(Js.asPropertyMap(console).get("warn"));
        List<Warning> seen = new ArrayList<>();
        Warn capture = (message, detail) -> {
            original.write(message, detail);
            seen.add(new Warning(message, detail));
        };
        Js.asPropertyMap(console).set("warn", capture);
        Promise<?> running;
        try {
            running = body.get();
        } catch (RuntimeException e) {
            Js.asPropertyMap(console).set("warn", original);
            throw e;
        }
        return running.then(ignore -> {
            Js.asPropertyMap(console).set("warn", original);
            check.accept(seen);
            return Promise.resolve((Object) null);
        }, err -> {
            Js.asPropertyMap(console).set("warn", original);
            return Promise.reject(err);
        });
    }

    /** A late-use warning was logged, carrying an Error whose stack shows where the pending was made. */
    private static Consumer<List<Warning>> warnedLate(String what) {
        return seen -> {
            Warning late = seen.stream().filter(w -> w.message.contains(LATE)).findFirst().orElse(null);
            assertNotNull(what + " logged no warning: " + seen, late);
            assertTrue(what + " logged no creation stack", late.stack != null && !late.stack.isEmpty());
            // The frame that made the pending must survive the API's own frames above it
            assertTrue(what + " logged a creation stack without the caller's frame " + makerFrame + ": " + late.stack,
                    late.stack.contains(makerFrame));
        };
    }

    /** The name of the frame {@link #made} runs in, as this browser and compile style print it. */
    private static String makerFrame;

    /** Makes a pending table, first noting the name of this frame so a creation stack can be checked for it. */
    private static JsPendingTable<?> made(Supplier<JsPendingTable<?>> make) {
        makerFrame = firstFrameName(new JsError().stack);
        return make.get();
    }

    /** The function name of the first frame of a stack, in either the "at name (...)" or "name@..." form. */
    private static String firstFrameName(String stack) {
        for (String line : stack.split("\n")) {
            String frame = line.trim();
            String name;
            if (frame.startsWith("at ")) {
                int paren = frame.indexOf(" (");
                name = paren < 0 ? frame.substring(3) : frame.substring(3, paren);
            } else if (frame.indexOf('@') > 0) {
                name = frame.substring(0, frame.indexOf('@'));
            } else {
                continue;
            }
            // Drop a receiver prefix such as "Object.", which differs between the two stacks
            return name.substring(name.lastIndexOf('.') + 1);
        }
        return stack;
    }

    private static Consumer<List<Warning>> quiet(String what) {
        return seen -> assertFalse(what + " warned: " + seen, seen.stream().anyMatch(w -> w.message.contains(LATE)));
    }

    /** As {@code expectRejection}, but fails instead of waiting forever if {@code promise} never settles. */
    private static Promise<Object> expectRejectionWithin(String what, Promise<?> promise, String fragment) {
        Promise<Object> timeout = delay(3_000)
                .then(ignore -> Promise.reject(what + " did not settle within 3s; expected it to reject"));
        return Promise.race(expectRejection(what, promise, fragment), timeout);
    }

    private static JsTableReference operand(JsPendingTable<?> pending) {
        return Js.uncheckedCast(pending);
    }

    /**
     * Chaining on a pending nobody awaited, in a later task, fails through the returned pending rather than by
     * throwing, and the failure reaches what is chained on that in turn. Whether or not the server has answered yet, so
     * the outcome does not depend on timing.
     */
    private static PendingScenario lateChainRejects(boolean afterAnswer) {
        return (kind, make) -> {
            JsPendingTable<?> pending = made(make);
            String what =
                    "chaining on an unawaited " + kind + (afterAnswer ? " after its answer" : " in the next task");
            return withWarnings(() -> later(what, afterAnswer, pending).then(ignore -> {
                JsPendingTable<JsResolvedTable> late = pending.head(Js.uncheckedCast(1.0));
                JsPendingTable<JsResolvedTable> next = late.head(Js.uncheckedCast(1.0));
                return expectLateUse(what, late.resolve())
                        // Passed on unchanged, not prefixed again at each step of the chain
                        .then(ignore2 -> expectLateUse("an operation chained on " + what, next.resolve()));
            }), warnedLate(what));
        };
    }

    /** Rejected with one "Cannot use source table:" prefix ahead of the late-use message. */
    private static Promise<Object> expectLateUse(String what, Promise<?> promise) {
        String cannotUse = "Cannot use source table: A pending table";
        return promise.then(
                ignore -> Promise.reject(what + " resolved; expected it to reject for being used " + LATE),
                err -> {
                    String message = String.valueOf(err);
                    assertTrue(what + " rejected for another reason: " + message,
                            message.startsWith(cannotUse) && message.contains(LATE));
                    return Promise.resolve((Object) null);
                });
    }

    /** Awaiting a pending nobody awaited, in a later task, rejects whether or not the server has answered yet. */
    private static PendingScenario lateAwaitRejects(boolean afterAnswer) {
        return (kind, make) -> {
            JsPendingTable<?> pending = made(make);
            String what = "awaiting an unawaited " + kind + (afterAnswer ? " after its answer" : " in the next task");
            return withWarnings(() -> later(what, afterAnswer, pending)
                    .then(ignore -> expectRejection(what, Js.uncheckedCast(pending.resolve()), LATE)),
                    warnedLate(what));
        };
    }

    /**
     * Moves to a later task: the next one, or the first after the server answered, which is when an unawaited pending
     * is released. Fails if that does not happen within 5s.
     */
    private static Promise<Object> later(String what, boolean afterAnswer, JsPendingTable<?> pending) {
        if (!afterAnswer) {
            return delay(0);
        }
        return untilReleased(what, pending, 100);
    }

    private static Promise<Object> untilReleased(String what, JsPendingTable<?> pending, int triesLeft) {
        return delay(50).then(ignore -> {
            if (!isHeld(pending)) {
                return Promise.resolve((Object) null);
            }
            if (triesLeft == 0) {
                return Promise.reject(what + ": the pending was still held after 5s, so the server never answered");
            }
            return untilReleased(what, pending, triesLeft - 1);
        });
    }

    /** Whether the state a pending table's ticket occupies is still in the cache and not released. */
    private static boolean isHeld(JsPendingTable<?> pending) {
        TableTicket ticket = new TableTicket(pending.typedTicket().getTicket());
        return pending.getConnection().getCache().get(ticket)
                .map(state -> state.getResolution() != ClientTableState.ResolutionState.RELEASED)
                .orElse(false);
    }

    public void testLateChainRejects() {
        runForEachKind(25_001, (kind, make) -> lateChainRejects(false).run(kind, make)
                .then(ignore -> lateChainRejects(true).run(kind, make)));
    }

    public void testLateAwaitRejects() {
        runForEachKind(25_002, (kind, make) -> lateAwaitRejects(false).run(kind, make)
                .then(ignore -> lateAwaitRejects(true).run(kind, make)));
    }

    public void testWrappedInItsTaskStaysUsable() {
        // Promise.resolve calls then() within the creating task, so the result can be awaited any time later
        runForEachKind(10_003, (kind, make) -> withWarnings(() -> {
            Promise<JsITable> kept = Promise.resolve(Js.<IThenable<JsITable>>uncheckedCast(make.get()));
            return delay(500).then(ignore -> kept).then(table -> {
                assertEquals(5., table.getSize(), 0);
                table.close();
                return Promise.resolve(table);
            });
        }, quiet("a " + kind + " wrapped in its task")));
    }

    public void testAwaitedThenUsedLaterRejects() {
        // Awaiting in its task does not extend the pending's life; the table the await produced is what lasts
        runForEachKind(15_004, (kind, make) -> {
            JsPendingTable<?> pending = made(make);
            Promise<JsITable> owned = Js.uncheckedCast(pending.resolve());
            String what = "a " + kind + " awaited in its task, then used later";
            return withWarnings(() -> owned.then(table -> {
                assertEquals(5., table.getSize(), 0);
                return expectLateUse(what + " (chained on)", pending.head(Js.uncheckedCast(1.0)).resolve())
                        .then(ignore -> expectRejection(what + " (awaited again)",
                                Js.uncheckedCast(pending.resolve()), LATE))
                        .then(ignore -> {
                            // The table itself is unaffected and still the caller's to close
                            assertEquals(5., table.getSize(), 0);
                            table.close();
                            return Promise.resolve((Object) null);
                        });
            }), warnedLate(what));
        });
    }

    public void testChainOnCanceledRejects() {
        runForEachKind(10_005, (kind, make) -> {
            JsPendingTable<?> pending = make.get();
            WorkerConnection connection = pending.getConnection();
            pending.cancel();
            JsPendingTable<JsResolvedTable> late;
            try {
                late = pending.head(Js.uncheckedCast(1.0));
            } catch (RuntimeException e) {
                return Promise.reject("chaining on a canceled " + kind + " threw instead of rejecting: " + e);
            }
            return expectRejection("chaining on a canceled " + kind, late.resolve(), "Operation canceled")
                    .then(ignore -> delay(200))
                    .then(ignore -> {
                        // Neither the canceled operation nor the one chained on it holds anything, here or there
                        assertEquals("batch states left after chaining on a canceled " + kind, 0,
                                batchStates(connection));
                        return Promise.resolve((Object) null);
                    });
        });
    }

    public void testChainedInItsTaskThenUsedLaterRejects() {
        // Chaining in the creating task holds the source only until that operation is done with it
        runForEachKind(15_006, (kind, make) -> {
            JsPendingTable<?> pending = made(make);
            pending.head(Js.uncheckedCast(1.0));
            String what = "chaining later on a " + kind + " that was only chained on in its task";
            return withWarnings(() -> delay(500).then(ignore -> expectRejection(what,
                    pending.head(Js.uncheckedCast(1.0)).resolve(), LATE)), warnedLate(what));
        });
    }

    public void testSequentialAwaitsRejectTheSecond() {
        // await a; await b; - b was not awaited in the task that created it
        runForEachKind(15_007, (kind, make) -> {
            JsPendingTable<?> first = made(make);
            JsPendingTable<?> second = made(make);
            String what = "awaiting the second of two " + kind + "s after the first";
            return withWarnings(() -> Js.<Promise<JsITable>>uncheckedCast(first.resolve()).then(table -> {
                table.close();
                return expectRejection(what, Js.uncheckedCast(second.resolve()), LATE);
            }), warnedLate(what));
        });
    }

    public void testLateOperandRejects() {
        // An operand is only read for its ticket, so a late operand must be caught where it is passed
        runForEachKind(15_009, (kind, make) -> {
            JsPendingTable<?> pending = made(make);
            String what = "an operation taking an unawaited " + kind + " as its operand 500ms later";
            return withWarnings(() -> delay(500).then(ignore -> expectRejectionWithin(what,
                    source.operations().whereIn(operand(pending), arr("X")).resolve(), LATE)), warnedLate(what));
        });
    }

    public void testFailedOperandRejects() {
        // A chain that failed for late use was never sent, so nothing on the server will ever define its ticket
        runForEachKind(15_010, (kind, make) -> {
            JsPendingTable<?> pending = make.get();
            return delay(0).then(ignore -> {
                JsPendingTable<JsResolvedTable> failed = pending.head(Js.uncheckedCast(1.0));
                return expectRejectionWithin("an operation taking a failed chain on a " + kind + " as its operand",
                        source.operations().whereIn(operand(failed), arr("X")).resolve(), LATE);
            });
        });
    }

    public void testCanceledOperandRejects() {
        runForEachKind(15_011, (kind, make) -> {
            JsPendingTable<?> pending = make.get();
            pending.cancel();
            return expectRejectionWithin("an operation taking a canceled " + kind + " as its operand",
                    source.operations().whereIn(operand(pending), arr("X")).resolve(), "Operation canceled");
        });
    }

    /**
     * Every method outside the batch that takes another table: the legacy joins and snapshot, merges, and input tables.
     * Each reads only the operand's ticket.
     */
    private Map<String, Function<JsTableReference, Promise<?>>> unbatched(IdeSession session, JsInputTable input) {
        Map<String, Function<JsTableReference, Promise<?>>> consumers = new LinkedHashMap<>();
        consumers.put("Table.naturalJoin", op -> source.naturalJoin(op, arr("X"), arr("Y2=Y")).resolve());
        consumers.put("Table.exactJoin", op -> source.exactJoin(op, arr("X"), arr("Y2=Y")).resolve());
        consumers.put("Table.crossJoin", op -> source.crossJoin(op, arr("X"), arr("Y2=Y"), null).resolve());
        consumers.put("Table.asOfJoin", op -> source.asOfJoin(op, arr("X"), arr("Y2=Y"), null).resolve());
        consumers.put("Table.snapshot", op -> source.snapshot(op, true, null).resolve());
        consumers.put("mergeTables",
                op -> session.mergeTables(new JsTableReference[] {JsTableReference.of(source), op}).resolve());
        consumers.put("InputTable.addTable", input::addTable);
        consumers.put("InputTable.deleteTable", input::deleteTable);
        return consumers;
    }

    /** Waits for every check, then fails listing each one that failed, so one hang does not hide another. */
    private static Promise<Object> allOf(List<Promise<Object>> checks) {
        Promise<?>[] outcomes = checks.stream()
                .map(check -> check.then(ignore -> Promise.resolve((Object) null),
                        err -> Promise.resolve((Object) String.valueOf(err))))
                .toArray(Promise[]::new);
        return Promise.all(outcomes).then(results -> {
            List<String> failures = new ArrayList<>();
            for (Object result : results) {
                if (result != null) {
                    failures.add((String) result);
                }
            }
            return failures.isEmpty() ? Promise.resolve((Object) null)
                    : Promise.reject(failures.size() + " failed:\n" + String.join("\n", failures));
        });
    }

    /**
     * For each kind of pending, spoils a fresh one per method outside the batch, passes it as that method's operand,
     * and expects a rejection with {@code fragment} within 3s instead of a wait on a ticket nothing will define. The
     * operand comes in a {@code Supplier}, since a promise would adopt it.
     */
    private void runUnbatched(int timeout, String spoiled,
            Function<JsPendingTable<?>, Promise<Supplier<JsTableReference>>> spoil, String fragment) {
        connect(tables)
                .then(session -> {
                    delayTestFinish(timeout);
                    return session.getTable("source", null).asPromise()
                            .then(table -> {
                                source = table;
                                return session.getTable("input", null).asPromise();
                            })
                            .then(inputTable -> inputTable.inputTable().then(input -> {
                                Map<String, Function<JsTableReference, Promise<?>>> consumers =
                                        unbatched(session, input);
                                return forEachKind(session, source, (kind, make) -> {
                                    List<Promise<Object>> checks = new ArrayList<>();
                                    consumers.forEach((name, consumer) -> {
                                        String what = name + " taking a " + spoiled + " " + kind + " as its operand";
                                        checks.add(spoil.apply(make.get()).then(op -> {
                                            Promise<?> result;
                                            try {
                                                result = consumer.apply(op.get());
                                            } catch (RuntimeException e) {
                                                return Promise.reject(what + " threw instead of rejecting: " + e);
                                            }
                                            return expectRejectionWithin(what, result, fragment);
                                        }));
                                    });
                                    return allOf(checks);
                                }).then(ignore -> {
                                    inputTable.close();
                                    return Promise.resolve((Object) null);
                                });
                            }))
                            .then(ignore -> {
                                source.close();
                                return Promise.resolve(session);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testCanceledOperandOutsideTheBatchRejects() {
        runUnbatched(15_013, "canceled", pending -> {
            pending.cancel();
            return Promise.resolve((Supplier<JsTableReference>) () -> operand(pending));
        }, "Operation canceled");
    }

    public void testFailedOperandOutsideTheBatchRejects() {
        // A chain that failed for late use was never sent, so nothing on the server will ever define its ticket
        runUnbatched(15_014, "failed chain on a", pending -> delay(0).then(ignore -> {
            JsPendingTable<JsResolvedTable> failed = pending.head(Js.uncheckedCast(1.0));
            return Promise.resolve((Supplier<JsTableReference>) () -> operand(failed));
        }), LATE);
    }

    /**
     * A pending that failed before anyone used it reports that failure when used late, not the late use: lateness lost
     * nothing, and the real error is the one the caller needs.
     */
    public void testLateUseReportsTheEarlierFailure() {
        runForEachKind(15_012, (kind, make) -> {
            JsPendingTable<?> failing;
            String fragment;
            if (kind.equals("operation")) {
                failing = source.operations().update(arr("Z = NoSuchColumn + 1"));
                // The server logs the details and sends only the status
                fragment = "INVALID_ARGUMENT";
            } else {
                // A definition the server cannot resolve: the fetch fails as soon as it is sent
                failing = source.getConnection().getObject(new JsVariableDefinition(JsVariableType.TABLE, "missing",
                        DomGlobal.btoa("s/missing"), null), null).asPendingTable();
                fragment = "missing";
            }
            String what = "a " + kind + " that failed, used 500ms later";
            return delay(500)
                    .then(ignore -> expectRejection(what + " (awaited)", Js.uncheckedCast(failing.resolve()),
                            fragment))
                    .then(ignore -> expectRejection(what + " (chained on)",
                            failing.head(Js.uncheckedCast(1.0)).resolve(), fragment));
        });
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
