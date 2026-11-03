//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.ReadonlyArray;
import elemental2.dom.DomGlobal;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.web.client.ide.IdeSession;
import jsinterop.base.Js;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static elemental2.dom.DomGlobal.console;

/**
 * How pending tables fail when they are used wrongly: awaited or chained on after the task that created them, or
 * chained on after being canceled. Each scenario runs against every kind of pending table, since they share one
 * implementation of these rules.
 */
public class PendingTableMisuseTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("source", "empty_table(5).update([\"X = i\", \"Y = i * 2\"])");

    /** What a pending used after the task that created it must say, in its rejection and its warning. */
    private static final String LATE = "after the task that created it";

    /** A scenario run once per kind of pending table; {@code kind} names it in failures. */
    private interface PendingScenario {
        Promise<Object> run(String kind, Supplier<JsPendingTable<?>> make);
    }

    private static ReadonlyArray<JsTable.CustomColumnArgUnionType> cols(String... exprs) {
        return Js.uncheckedCast(JsArray.of(exprs));
    }

    private static Promise<Object> delay(int millis) {
        return new Promise<>((resolve, reject) -> DomGlobal.setTimeout(p -> resolve.onInvoke((Object) null), millis));
    }

    /**
     * Runs {@code scenario} against each kind of pending table: an operation, sent in a batch, and an entry-point
     * fetch.
     */
    private static Promise<Object> forEachKind(IdeSession session, JsTable table, PendingScenario scenario) {
        return scenario.run("operation", () -> table.operations().update(cols("Z = X + Y")))
                .then(ignore -> scenario.run("fetch", () -> session.getTable("source", null)));
    }

    /** Connects, resolves the source table, runs {@code scenario} for each kind, and closes the table. */
    private void runForEachKind(int timeout, PendingScenario scenario) {
        connect(tables)
                .then(session -> {
                    delayTestFinish(timeout);
                    return session.getTable("source", null).resolve()
                            .then(table -> forEachKind(session, table, scenario).then(ignore -> {
                                table.close();
                                return Promise.resolve(session);
                            }));
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
                    return Promise.resolve((Object) null);
                });
    }

    /** Runs {@code body} with console.warn captured, then hands the warnings to {@code check}. */
    private static Promise<Object> withWarnings(Supplier<Promise<?>> body, Consumer<List<String>> check) {
        LogMethod original = Js.uncheckedCast(Js.asPropertyMap(console).get("warn"));
        List<String> seen = new ArrayList<>();
        LogMethod capture = args -> {
            original.write(args);
            seen.add(Arrays.toString(args));
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

    private static Consumer<List<String>> warnedLate(String what) {
        return seen -> assertTrue(what + " logged no warning: " + seen,
                seen.stream().anyMatch(w -> w.contains(LATE)));
    }

    private static Consumer<List<String>> quiet(String what) {
        return seen -> assertFalse(what + " warned: " + seen, seen.stream().anyMatch(w -> w.contains(LATE)));
    }

    /**
     * Chaining on a pending nobody awaited, in a later task, fails through the returned pending rather than by
     * throwing, and the failure reaches what is chained on that in turn. Whether or not the server has answered yet, so
     * the outcome does not depend on timing.
     */
    private static PendingScenario lateChainRejects(int wait) {
        return (kind, make) -> {
            JsPendingTable<?> pending = make.get();
            String what = "chaining on an unawaited " + kind + " " + wait + "ms later";
            return withWarnings(() -> delay(wait).then(ignore -> {
                JsPendingTable<JsResolvedTable> late = pending.head(Js.uncheckedCast(1.0));
                JsPendingTable<JsResolvedTable> next = late.head(Js.uncheckedCast(1.0));
                return expectRejection(what, late.resolve(), LATE)
                        .then(ignore2 -> expectRejection("an operation chained on " + what, next.resolve(), LATE));
            }), warnedLate(what));
        };
    }

    /** Awaiting a pending nobody awaited, in a later task, rejects whether or not the server has answered yet. */
    private static PendingScenario lateAwaitRejects(int wait) {
        return (kind, make) -> {
            JsPendingTable<?> pending = make.get();
            String what = "awaiting an unawaited " + kind + " " + wait + "ms later";
            return withWarnings(
                    () -> delay(wait).then(ignore -> expectRejection(what, Js.uncheckedCast(pending.resolve()), LATE)),
                    warnedLate(what));
        };
    }

    public void testLateChainRejects() {
        runForEachKind(15_001, (kind, make) -> lateChainRejects(0).run(kind, make)
                .then(ignore -> lateChainRejects(500).run(kind, make)));
    }

    public void testLateAwaitRejects() {
        runForEachKind(15_002, (kind, make) -> lateAwaitRejects(0).run(kind, make)
                .then(ignore -> lateAwaitRejects(500).run(kind, make)));
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

    public void testAwaitedThenChainedLaterStaysUsable() {
        runForEachKind(10_004, (kind, make) -> withWarnings(() -> {
            JsPendingTable<?> pending = make.get();
            Promise<JsITable> owned = Js.uncheckedCast(pending.resolve());
            return delay(500).then(ignore -> pending.head(Js.uncheckedCast(1.0)).resolve()).then(head -> {
                assertEquals(1., head.getSize(), 0);
                head.close();
                return owned;
            }).then(table -> {
                table.close();
                return Promise.resolve(table);
            });
        }, quiet("a " + kind + " awaited, then chained on later")));
    }

    public void testChainOnCanceledRejects() {
        runForEachKind(10_005, (kind, make) -> {
            JsPendingTable<?> pending = make.get();
            pending.cancel();
            JsPendingTable<JsResolvedTable> late;
            try {
                late = pending.head(Js.uncheckedCast(1.0));
            } catch (RuntimeException e) {
                return Promise.reject("chaining on a canceled " + kind + " threw instead of rejecting: " + e);
            }
            return expectRejection("chaining on a canceled " + kind, late.resolve(), "Operation canceled");
        });
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
