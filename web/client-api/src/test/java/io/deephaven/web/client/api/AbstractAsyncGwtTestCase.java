//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.gwt.core.client.JavaScriptException;
import com.google.gwt.junit.client.GWTTestCase;
import elemental2.core.JsArray;
import elemental2.core.JsError;
import elemental2.core.JsString;
import elemental2.dom.DomGlobal;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.event.Event;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.deephaven.web.client.api.subscription.ViewportData;
import io.deephaven.web.client.api.tree.JsTreeTable;
import io.deephaven.web.client.fu.CancellablePromise;
import io.deephaven.web.client.ide.IdeSession;
import io.deephaven.web.shared.fu.JsRunnable;
import io.deephaven.web.shared.fu.RemoverFn;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static elemental2.dom.DomGlobal.console;

public abstract class AbstractAsyncGwtTestCase extends GWTTestCase {
    @JsMethod(namespace = JsPackage.GLOBAL)
    private static native Object eval(String code);

    private static Promise<JsPropertyMap<Object>> importScript(String moduleName) {
        return (Promise<JsPropertyMap<Object>>) eval("import('" + moduleName + "')");
    }

    private static Promise<Void> importDhInternal() {
        return importScript(localServer + "/jsapi/dh-internal.js")
                .then(module -> {
                    Js.asPropertyMap(DomGlobal.window).set("dhinternal", module.get("dhinternal"));
                    return Promise.resolve((Void) null);
                });
    }

    public static final String localServer = System.getProperty("dh.server", "http://localhost:10000");

    public static class TableSourceBuilder {
        private final List<String> pythonScripts = new ArrayList<>();

        public TableSourceBuilder script(String script) {
            pythonScripts.add(script);
            return this;
        }

        public TableSourceBuilder script(String tableName, String python) {
            pythonScripts.add(tableName + "=" + python);

            return this;
        }
    }

    /**
     * Set this to a value higher than 1 to get more time to run debugger without timeouts failing.
     */
    protected static final int TIMEOUT_SCALE = Integer.parseInt(System.getProperty("test.timeout.scale", "1"));
    public static final double DELTA = 0.0001;

    public JsString toJsString(String k) {
        return Js.cast(k);
    }

    public JsArray<JsString> toJsString(String... k) {
        return Js.cast(k);
    }

    @JsProperty(name = "log", namespace = "console")
    private static native elemental2.core.Function getLog();

    static <T> IThenable.ThenOnFulfilledCallbackFn<T, T> logOnSuccess(Object... rest) {
        return value -> {
            // GWT will puke if we have varargs being sent to varargs;
            // so we go JS on it and just grab the function to apply
            getLog().apply(null, rest);
            return Promise.resolve(value);
        };
    }

    static Promise<Object> log(Object... rest) {
        getLog().apply(null, rest);
        return Promise.resolve(rest);
    }

    JsRunnable assertEventNotCalled(
            HasEventHandling handling,
            String... events) {
        RemoverFn[] undos = new RemoverFn[events.length];
        for (int i = 0; i < events.length; i++) {
            final String ev = events[i];
            undos[i] = handling.addEventListener(ev, e -> {
                log("Did not expect", ev, "but fired event", e);
                report("Expected " + ev + " to not be called; detail: " + (e.getDetail()));
            });
        }
        return () -> {
            for (RemoverFn undo : undos) {
                undo.remove();
            }

        };
    }

    static <T> IThenable.ThenOnFulfilledCallbackFn<T, T> run(JsRunnable allow) {
        return t -> {
            allow.run();
            return Promise.resolve(t);
        };
    }

    static <T> Promise<T> expectFailure(Promise<?> state, T value) {
        return state.then(val -> Promise.reject("Failed"),
                error -> Promise.resolve(value));
    }

    /**
     * Imports the webpack content, including protobuf types. Does not connect to the server.
     */
    protected Promise<Void> setupDhInternal() {
        delayTestFinish(504);
        return importDhInternal();
    }

    /**
     * Connects and authenticates to the configured server and runs the specified scripts.
     */
    protected Promise<IdeSession> connect(TableSourceBuilder tables) {
        // start by delaying test finish by 1.0s so we fail fast in cases where we aren't set up right
        delayTestFinish(1007);
        return importDhInternal().then(module -> {
            CoreClient coreClient = new CoreClient(localServer, null);
            return coreClient.login(JsPropertyMap.of("type", CoreClient.LOGIN_TYPE_ANONYMOUS))
                    .then(ignore -> coreClient.getAsIdeConnection())
                    .then(ide -> {
                        delayTestFinish(501);
                        return ide.getConsoleTypes().then(consoleTypes -> {
                            delayTestFinish(502);
                            CancellablePromise<IdeSession> ideSession = ide.startSession(consoleTypes.getAt(0));
                            return ideSession.then(session -> {

                                if (consoleTypes.includes("python")) {
                                    return runAllScriptsInOrder(ideSession, session, tables.pythonScripts);
                                }
                                throw new IllegalStateException("Unsupported script type " + consoleTypes);
                            });
                        });
                    });
        });
    }

    private Promise<IdeSession> runAllScriptsInOrder(CancellablePromise<IdeSession> ideSession, IdeSession session,
            List<String> code) {
        Promise<IdeSession> result = ideSession;
        for (int i = 0; i < code.size(); i++) {
            final int index = i;
            result = result.then(ignore -> {
                delayTestFinish(4000 + index);

                return session.runCode(code.get(index));
            }).then(r -> {
                if (r.getError() != null) {
                    return Promise.reject(r.getError());
                }
                return ideSession;
            });
        }
        return result;
    }

    public IThenable.ThenOnFulfilledCallbackFn<IdeSession, JsTable> table(String tableName) {
        return session -> session.getTable(tableName, null);
    }

    public IThenable.ThenOnFulfilledCallbackFn<IdeSession, JsTreeTable> treeTable(String tableName) {
        return session -> session.getTreeTable(tableName);
    }

    public IThenable.ThenOnFulfilledCallbackFn<IdeSession, JsPartitionedTable> partitionedTable(String tableName) {
        return session -> session.getPartitionedTable(tableName);
    }

    /**
     * Utility method to report Promise errors to the unit test framework
     */
    protected <T> Promise<T> report(Object error) {
        if (error instanceof String) {
            reportUncaughtException(new RuntimeException((String) error));
        } else if (error instanceof Throwable) {
            reportUncaughtException((Throwable) error);
        }
        if (error instanceof JsError) {
            reportUncaughtException(new JavaScriptException(error));
        } else {
            reportUncaughtException(new RuntimeException(error.toString()));
        }
        // keep failing down the chain in case someone else cares
        return Promise.reject(error);
    }

    protected Promise<?> finish(Object input) {
        finishTest();
        return Promise.resolve(input);
    }

    /**
     * Helper method to add a listener to the promise of a table, and ensure that an update is received with the
     * expected number of items, within the specified timeout.
     *
     * Prereq: have already requested a viewport on that table
     */
    protected Promise<JsTable> assertUpdateReceived(Promise<JsTable> tablePromise, int count, int timeoutInMillis) {
        return tablePromise.then(table -> assertUpdateReceived(table, count, timeoutInMillis));
    }

    /**
     * Helper method to add a listener to a table, and ensure that an update is received with the expected number of
     * items, within the specified timeout.
     *
     * Prereq: have already requested a viewport on that table. Remember to request that within the same event loop, so
     * that there isn't a data race and the update gets missed.
     */
    protected Promise<JsTable> assertUpdateReceived(JsTable table, int count, int timeoutInMillis) {
        return assertUpdateReceived(table, viewportData -> assertEquals(count, viewportData.getRows().length),
                timeoutInMillis);
    }

    protected Promise<JsTable> assertUpdateReceived(JsTable table, Consumer<ViewportData> check, int timeoutInMillis) {
        return Promise.race(this.<JsTable, ViewportData>waitForEvent(table, JsTable.EVENT_UPDATED, e -> {
            ViewportData viewportData = e.getDetail();
            check.accept(viewportData);
        }, timeoutInMillis),
                table.nextEvent(JsTable.EVENT_REQUEST_FAILED, (double) timeoutInMillis).then(Promise::reject))
                .then(ignore -> Promise.resolve(table));
    }

    protected <T> IThenable.ThenOnFulfilledCallbackFn<T, T> delayFinish(int timeout) {
        return object -> {
            delayTestFinish(timeout);
            return Promise.resolve(object);
        };
    }

    protected IThenable.ThenOnFulfilledCallbackFn<JsTable, JsTable> waitForTick(int timeout) {
        return table -> waitForEvent(table, JsTable.EVENT_SIZECHANGED, ignored -> {
        }, timeout);
    }

    protected IThenable.ThenOnFulfilledCallbackFn<JsTable, JsTable> waitForTickTwice(int timeout) {
        return table -> {
            // wait for two ticks... one from setting the viewport, and then another for whenever the table actually
            // ticks.
            // (if these happen out of order, they will still be very close)
            return waitForEvent(table, JsTable.EVENT_SIZECHANGED, ignored -> {
            }, timeout)
                    .then(t -> waitForEvent(table, JsTable.EVENT_SIZECHANGED, ignored -> {
                    }, timeout));
        };
    }

    protected <V extends HasEventHandling, T> Promise<V> waitForEventWhere(V evented, String eventName,
            Predicate<Event<T>> check, int timeout) {
        // note that this roughly reimplements the 'kill timer' so this can be run in parallel with itself or other
        // similar steps
        return new Promise<>((resolve, reject) -> {
            boolean[] complete = {false};
            console.log("adding " + eventName + " listener ", evented);
            // apparent compiler bug, review in gwt 2.9
            RemoverFn unsub = Js.<HasEventHandling>uncheckedCast(evented)
                    .<T>addEventListener(eventName, e -> {
                        if (complete[0]) {
                            return;// already done, but timeout hasn't cleared us yet
                        }
                        console.log("event ", e, " observed ", eventName, " for ", evented);
                        try {
                            if (check.test(e)) {
                                complete[0] = true;
                                resolve.onInvoke(evented);
                            }
                        } catch (Throwable ex) {
                            reject.onInvoke(ex);
                        }
                    });
            DomGlobal.setTimeout(p0 -> {
                unsub.remove();
                if (!complete[0]) {
                    reject.onInvoke("Failed to complete in " + timeout + "ms " + evented);
                }
                complete[0] = true;
                // complete already handled
            }, timeout * TIMEOUT_SCALE + 13);

        });
    }

    protected <V extends HasEventHandling, T> Promise<V> waitForEvent(V evented, String eventName,
            Consumer<Event<T>> check, int timeout) {
        return this.<V, T>waitForEventWhere(evented, eventName, e -> {
            check.accept(e);
            return true;
        }, timeout);
    }


    protected static <T> Promise<T> promiseAllThen(T then, IThenable<?>... promises) {
        return Promise.all(promises).then(items -> Promise.resolve(then));
    }

    protected <T> IThenable<T> waitFor(BooleanSupplier predicate, int checkInterval, int timeout, T result) {
        return new Promise<>((resolve, reject) -> {
            schedule(predicate, checkInterval, () -> resolve.onInvoke(result),
                    () -> reject.onInvoke("timeout of " + timeout + " exceeded"), timeout);
        });
    }

    protected <T> IThenable.ThenOnFulfilledCallbackFn<T, T> waitFor(int millis) {
        return result -> new Promise<>((resolve, reject) -> {
            DomGlobal.setTimeout(p -> resolve.onInvoke(result), millis);
        });
    }

    protected <T extends HasEventHandling> IThenable.ThenOnFulfilledCallbackFn<T, T> waitForEvent(T table,
            String eventName, int millis) {
        return result -> new Promise<>((resolve, reject) -> {
            boolean[] success = {false};
            table.addEventListenerOneShot(eventName, e -> {
                success[0] = true;
                resolve.onInvoke(table);
            });
            DomGlobal.setTimeout(p -> {
                if (!success[0]) {
                    reject.onInvoke("Waited " + millis + "ms");
                }
            }, millis);
        });
    }

    private void schedule(BooleanSupplier predicate, int checkInterval, Runnable complete, Runnable fail, int timeout) {
        if (timeout <= 0) {
            fail.run();
        }
        if (predicate.getAsBoolean()) {
            complete.run();
        }
        DomGlobal.setTimeout(ignore -> {
            if (predicate.getAsBoolean()) {
                complete.run();
            } else {
                schedule(predicate, checkInterval, complete, fail, (timeout * TIMEOUT_SCALE) - checkInterval);
            }
        }, checkInterval);
    }

    protected Promise<JsTable> assertNextViewportIs(JsTable table, Function<JsTable, Column> column,
            String[] expected) {
        return assertUpdateReceived(table, viewportData -> {
            String[] actual = Js.uncheckedCast(getColumnData(viewportData, column.apply(table)));
            assertTrue("Expected " + Arrays.toString(expected) + ", found " + Arrays.toString(actual) + " in table "
                    + table + " at state " + table.state(), Arrays.equals(expected, actual));
        }, 2000);
    }

    protected Object getColumnData(ViewportData viewportData, Column a) {
        return viewportData.getRows().map((r, index) -> r.get(a));
    }

    protected Promise<JsTable> assertNextViewportIs(JsTable table, double... expected) {
        return assertUpdateReceived(table, viewportData -> {
            double[] actual = Js.uncheckedCast(getColumnData(viewportData, table.findColumn("I")));
            assertTrue("Expected " + Arrays.toString(expected) + ", found " + Arrays.toString(actual) + " in table "
                    + table, Arrays.equals(expected, actual));
        }, 2000);
    }

    public static List<Column> filterColumns(JsTable table, JsPredicate<Column> filter) {
        List<Column> matches = new ArrayList<>();
        table.getColumns().forEach((c, i) -> {
            if (filter.test(c)) {
                matches.add(c);
            }
            return null;
        });
        return matches;
    }
}
